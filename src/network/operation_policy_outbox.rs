//! Durable delivery obligations for complete creation-policy witnesses.
//!
//! A caller authenticates the packet before enrollment and authenticates the
//! adjacent peer before passing its identity to `record_authenticated_receipt`.
//! This store pins routing/link configuration, preserves the exact signed
//! packet across retries, and never treats a bare receipt as a certificate.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::operation::CrdtSubject;
use crate::store::backend::{create_dir_all_durable, fsync_dir};
use crate::types::NodeId;

use super::operation_transport::{OperationPacket, OperationReceipt, PolicyWitnessIdentity};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum PolicyOutboxError {
    #[error("policy witness outbox I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("policy witness outbox JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("policy witness identity is already bound to another packet")]
    Conflict,
    #[error("policy witness send is unknown")]
    Unknown,
    #[error("policy witness receipt has the wrong peer or commitment")]
    WrongReceipt,
    #[error("policy witness outbox needs reopen after an uncertain write")]
    Poisoned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pin {
    version: u32,
    sender: NodeId,
    route_digest: [u8; 32],
    link_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyWitnessId {
    pub destination: NodeId,
    pub subject: CrdtSubject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SendRecord {
    version: u32,
    sender: NodeId,
    id: PolicyWitnessId,
    next_hop: NodeId,
    peer_addr: String,
    packet_commitment: [u8; 32],
    packet: OperationPacket,
}

/// An acknowledged send no longer needs a retry body. Keep its authenticated
/// identity so later enrollment and recovery still reject conflicting Create
/// packets with the same destination and subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactSendRecord {
    version: u32,
    sender: NodeId,
    id: PolicyWitnessId,
    next_hop: NodeId,
    peer_addr: String,
    identity: PolicyWitnessIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AckRecord {
    version: u32,
    id: PolicyWitnessId,
    commitment: [u8; 32],
}

type PolicyOutboxRecords = (
    BTreeMap<PolicyWitnessId, SendRecord>,
    BTreeMap<PolicyWitnessId, CompactSendRecord>,
    BTreeMap<PolicyWitnessId, AckRecord>,
);

#[derive(Debug, Clone)]
pub struct PendingPolicyWitness {
    pub id: PolicyWitnessId,
    pub destination: NodeId,
    pub next_hop: NodeId,
    pub peer_addr: String,
    pub packet: Option<OperationPacket>,
    pub identity: PolicyWitnessIdentity,
    pub commitment: [u8; 32],
    pub acknowledged: bool,
}

pub struct OperationPolicyOutbox {
    dir: PathBuf,
    _lock: File,
    pin: Pin,
    sends: BTreeMap<PolicyWitnessId, SendRecord>,
    compact: BTreeMap<PolicyWitnessId, CompactSendRecord>,
    acknowledgments: BTreeMap<PolicyWitnessId, AckRecord>,
    poisoned: bool,
}

impl OperationPolicyOutbox {
    /// Read-only startup check. Run this with the other operation manifest
    /// checks before any missing pin is created.
    pub fn verify_pin(
        dir: impl AsRef<Path>,
        sender: &NodeId,
        route_digest: [u8; 32],
        link_digest: [u8; 32],
    ) -> Result<(), PolicyOutboxError> {
        let dir = dir.as_ref();
        if !dir.exists() {
            return Ok(());
        }
        let pin_path = dir.join("manifest.json");
        if pin_path.exists() {
            let pin: Pin = read_canonical(&pin_path)?;
            if pin
                != (Pin {
                    version: VERSION,
                    sender: sender.clone(),
                    route_digest,
                    link_digest,
                })
            {
                return Err(invalid("policy witness manifests or sender changed").into());
            }
        } else {
            for entry in fs::read_dir(dir)? {
                let name = entry?.file_name();
                if name.to_string_lossy().ends_with(".send")
                    || name.to_string_lossy().ends_with(".ack")
                {
                    return Err(invalid("policy witness records lack manifest pin").into());
                }
            }
        }
        Ok(())
    }

    /// The two digests identify immutable route and authenticated-link
    /// manifests. Reopening with different values fails while this directory
    /// retains obligations. Use a separate directory for a new deployment.
    pub fn open(
        dir: impl AsRef<Path>,
        sender: NodeId,
        route_digest: [u8; 32],
        link_digest: [u8; 32],
    ) -> Result<Self, PolicyOutboxError> {
        let dir = dir.as_ref();
        create_dir_all_durable(dir)?;
        let lock_path = dir.join(".lock");
        let fresh_lock = !lock_path.exists();
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        lock.try_lock()
            .map_err(|error| io::Error::new(io::ErrorKind::WouldBlock, error.to_string()))?;
        if fresh_lock {
            lock.sync_all()?;
            fsync_dir(dir)?;
        }
        // A crash before publishing the pin may leave its temporary file.
        // Remove it under the directory lease before write_new chooses a
        // temporary name; a reused process ID can otherwise collide with it.
        let mut removed_temp = false;
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if name
                .to_str()
                .is_some_and(|name| name.starts_with(".tmp-") && name.ends_with(".tmp"))
            {
                fs::remove_file(entry.path())?;
                removed_temp = true;
            }
        }
        if removed_temp {
            fsync_dir(dir)?;
        }

        let expected = Pin {
            version: VERSION,
            sender,
            route_digest,
            link_digest,
        };
        let pin_path = dir.join("manifest.json");
        let pin = if pin_path.exists() {
            let pin: Pin = read_canonical(&pin_path)?;
            if pin != expected {
                return Err(invalid("policy witness manifests or sender changed").into());
            }
            pin
        } else {
            // A record without its pin could have been admitted under a
            // different route or peer identity. Never adopt it silently.
            for entry in fs::read_dir(dir)? {
                let name = entry?.file_name();
                if name.to_string_lossy().ends_with(".send")
                    || name.to_string_lossy().ends_with(".ack")
                {
                    return Err(invalid("policy witness records lack manifest pin").into());
                }
            }
            write_new(dir, &pin_path, &serde_json::to_vec(&expected)?)?;
            expected
        };

        let (sends, compact, acknowledgments) = read_records(dir, &pin)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            _lock: lock,
            pin,
            sends,
            compact,
            acknowledgments,
            poisoned: false,
        })
    }

    /// Reconcile a write whose rename or directory fsync returned an
    /// uncertain result. Keep the exclusive directory lease while validating
    /// every committed file. The retry worker calls this on each pass, so a
    /// transient disk error does not require a process restart.
    pub fn recover_if_poisoned(&mut self) -> Result<(), PolicyOutboxError> {
        if !self.poisoned {
            return Ok(());
        }
        let pin: Pin = read_canonical(&self.dir.join("manifest.json"))?;
        if pin != self.pin {
            return Err(invalid("policy witness manifests or sender changed").into());
        }
        let (sends, compact, acknowledgments) = read_records(&self.dir, &pin)?;
        self.sends = sends;
        self.compact = compact;
        self.acknowledgments = acknowledgments;
        self.poisoned = false;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn mark_uncertain_for_test(&mut self) {
        self.poisoned = true;
    }

    /// Returns only after the complete signed packet and its identity are
    /// durable. A retry must reproduce exactly the same packet.
    pub fn enqueue(
        &mut self,
        destination: NodeId,
        next_hop: NodeId,
        peer_addr: String,
        packet: OperationPacket,
    ) -> Result<PolicyWitnessId, PolicyOutboxError> {
        if self.poisoned {
            return Err(PolicyOutboxError::Poisoned);
        }
        let id = PolicyWitnessId {
            destination,
            subject: packet.signed.bound.operation.subject.clone(),
        };
        let identity = PolicyWitnessIdentity::from(&packet);
        if packet.signed.bound.validate_structure().is_err()
            || !matches!(
                packet.signed.bound.operation.action,
                crate::operation::CandidateKvAction::Create { .. }
            )
            || next_hop.0.is_empty()
            || peer_addr.is_empty()
            || id.destination.0.is_empty()
        {
            return Err(invalid("invalid policy witness packet structure").into());
        }
        let record = SendRecord {
            version: VERSION,
            sender: self.pin.sender.clone(),
            id: id.clone(),
            next_hop,
            peer_addr,
            packet_commitment: packet.commitment(),
            packet,
        };
        if let Some(existing) = self.sends.get(&id) {
            return if existing == &record {
                Ok(id)
            } else {
                Err(PolicyOutboxError::Conflict)
            };
        }
        if let Some(existing) = self.compact.get(&id) {
            return if existing.sender == record.sender
                && existing.next_hop == record.next_hop
                && existing.peer_addr == record.peer_addr
                && existing.identity == identity
            {
                Ok(id)
            } else {
                Err(PolicyOutboxError::Conflict)
            };
        }
        self.poisoned = true;
        write_new(
            &self.dir,
            &record_path(&self.dir, &id, "send"),
            &serde_json::to_vec(&record)?,
        )?;
        self.sends.insert(id.clone(), record);
        self.poisoned = false;
        Ok(id)
    }

    /// `authenticated_peer` must come from the authenticated transport, not
    /// from an untrusted receipt body. The receiver receipt commits to the
    /// exact packet and becomes durable before this send leaves `pending()`.
    pub fn record_authenticated_receipt(
        &mut self,
        id: &PolicyWitnessId,
        authenticated_peer: &NodeId,
        receipt: &OperationReceipt,
    ) -> Result<(), PolicyOutboxError> {
        if self.poisoned {
            return Err(PolicyOutboxError::Poisoned);
        }
        let (next_hop, commitment) = if let Some(send) = self.sends.get(id) {
            (&send.next_hop, send.packet_commitment)
        } else if let Some(send) = self.compact.get(id) {
            (&send.next_hop, send.identity.packet_commitment)
        } else {
            return Err(PolicyOutboxError::Unknown);
        };
        if authenticated_peer != next_hop || receipt.commitment != commitment {
            return Err(PolicyOutboxError::WrongReceipt);
        }
        if self.acknowledgments.contains_key(id) {
            return Ok(());
        }
        let ack = AckRecord {
            version: VERSION,
            id: id.clone(),
            commitment: receipt.commitment,
        };
        self.poisoned = true;
        write_new(
            &self.dir,
            &record_path(&self.dir, id, "ack"),
            &serde_json::to_vec(&ack)?,
        )?;
        self.acknowledgments.insert(id.clone(), ack);
        self.poisoned = false;
        Ok(())
    }

    /// True only when the exact routed send has a durable authenticated ACK.
    /// The result remains available after the value-bearing send is compacted.
    pub fn has_durable_receipt(
        &self,
        destination: &NodeId,
        subject: &CrdtSubject,
        commitment: [u8; 32],
    ) -> bool {
        if self.poisoned {
            return false;
        }
        let id = PolicyWitnessId {
            destination: destination.clone(),
            subject: subject.clone(),
        };
        self.acknowledgments
            .get(&id)
            .is_some_and(|ack| ack.commitment == commitment)
            && (self
                .sends
                .get(&id)
                .is_some_and(|send| send.packet_commitment == commitment)
                || self
                    .compact
                    .get(&id)
                    .is_some_and(|send| send.identity.packet_commitment == commitment))
    }

    /// Replace every acknowledged full send with a value-free identity.
    /// The ACK is fsynced before the full packet can be replaced. An uncertain
    /// rename leaves the writer poisoned until directory recovery validates
    /// either the old or new record under its pinned route and link manifests.
    pub fn compact_acked_and_sync(&mut self) -> Result<usize, PolicyOutboxError> {
        self.recover_if_poisoned()?;
        let ids: Vec<_> = self
            .sends
            .keys()
            .filter(|id| self.acknowledgments.contains_key(*id))
            .cloned()
            .collect();
        for id in &ids {
            let send = self.sends.get(id).expect("selected send exists");
            let record = CompactSendRecord {
                version: 2,
                sender: send.sender.clone(),
                id: id.clone(),
                next_hop: send.next_hop.clone(),
                peer_addr: send.peer_addr.clone(),
                identity: PolicyWitnessIdentity::from(&send.packet),
            };
            self.poisoned = true;
            atomic_replace(
                &self.dir,
                &record_path(&self.dir, id, "send"),
                &serde_json::to_vec(&record)?,
            )?;
            self.compact.insert(id.clone(), record);
            self.sends.remove(id);
            self.poisoned = false;
        }
        Ok(ids.len())
    }

    pub fn pending(&self) -> Vec<PendingPolicyWitness> {
        if self.poisoned {
            return Vec::new();
        }
        self.sends
            .iter()
            .filter(|(id, _)| !self.acknowledgments.contains_key(*id))
            .map(|(id, record)| self.snapshot(id, record))
            .collect()
    }

    /// All durable send identities. Acknowledged compact entries have no
    /// packet body; pending entries retain the exact retry packet.
    pub fn entries(&self) -> Vec<PendingPolicyWitness> {
        if self.poisoned {
            return Vec::new();
        }
        let mut entries: Vec<_> = self
            .sends
            .iter()
            .map(|(id, record)| self.snapshot(id, record))
            .collect();
        entries.extend(
            self.compact
                .iter()
                .map(|(id, record)| PendingPolicyWitness {
                    id: id.clone(),
                    destination: id.destination.clone(),
                    next_hop: record.next_hop.clone(),
                    peer_addr: record.peer_addr.clone(),
                    packet: None,
                    identity: record.identity.clone(),
                    commitment: record.identity.packet_commitment,
                    acknowledged: true,
                }),
        );
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        entries
    }

    fn snapshot(&self, id: &PolicyWitnessId, record: &SendRecord) -> PendingPolicyWitness {
        PendingPolicyWitness {
            id: id.clone(),
            destination: id.destination.clone(),
            next_hop: record.next_hop.clone(),
            peer_addr: record.peer_addr.clone(),
            packet: Some(record.packet.clone()),
            identity: PolicyWitnessIdentity::from(&record.packet),
            commitment: record.packet_commitment,
            acknowledged: self.acknowledgments.contains_key(id),
        }
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn read_records(dir: &Path, pin: &Pin) -> Result<PolicyOutboxRecords, PolicyOutboxError> {
    let mut sends = BTreeMap::new();
    let mut compact = BTreeMap::new();
    let mut acknowledgments = BTreeMap::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(".tmp-") && name.ends_with(".tmp") {
            fs::remove_file(path)?;
            continue;
        }
        if name == ".lock" || name == "manifest.json" {
            continue;
        }
        if !path.is_file() {
            return Err(invalid("non-file in policy witness outbox").into());
        }
        if name.ends_with(".send") {
            let value: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
            if value.get("version").and_then(|version| version.as_u64()) == Some(2) {
                let record: CompactSendRecord = read_canonical(&path)?;
                if record.version != 2
                    || record.sender != pin.sender
                    || record.id.subject != record.identity.subject
                    || record.identity.declaration.creation_id != record.identity.subject
                    || record.identity.declaration.key.is_empty()
                    || record.next_hop.0.is_empty()
                    || record.peer_addr.is_empty()
                    || path != record_path(dir, &record.id, "send")
                    || compact.insert(record.id.clone(), record).is_some()
                {
                    return Err(invalid("invalid compact policy witness send record").into());
                }
                continue;
            }
            let record: SendRecord = read_canonical(&path)?;
            if record.version != VERSION
                || record.sender != pin.sender
                || record.id.subject != record.packet.signed.bound.operation.subject
                || record.next_hop.0.is_empty()
                || record.peer_addr.is_empty()
                || !matches!(
                    record.packet.signed.bound.operation.action,
                    crate::operation::CandidateKvAction::Create { .. }
                )
                || record.packet_commitment != record.packet.commitment()
                || path != record_path(dir, &record.id, "send")
                || record.packet.signed.bound.validate_structure().is_err()
                || sends.insert(record.id.clone(), record).is_some()
            {
                return Err(invalid("invalid policy witness send record").into());
            }
        } else if name.ends_with(".ack") {
            let record: AckRecord = read_canonical(&path)?;
            if record.version != VERSION
                || path != record_path(dir, &record.id, "ack")
                || acknowledgments.insert(record.id.clone(), record).is_some()
            {
                return Err(invalid("invalid policy witness receipt record").into());
            }
        } else {
            return Err(invalid("unexpected policy witness outbox file").into());
        }
    }
    for (id, ack) in &acknowledgments {
        if sends
            .get(id)
            .is_none_or(|send| send.packet_commitment != ack.commitment)
            && compact
                .get(id)
                .is_none_or(|send| send.identity.packet_commitment != ack.commitment)
        {
            return Err(invalid("receipt has no matching policy witness send").into());
        }
    }
    if compact
        .keys()
        .any(|id| !acknowledgments.contains_key(id) || sends.contains_key(id))
    {
        return Err(invalid("compact send lacks ACK or conflicts with full send").into());
    }
    fsync_dir(dir)?;
    Ok((sends, compact, acknowledgments))
}

fn read_canonical<T: for<'de> Deserialize<'de> + Serialize>(
    path: &Path,
) -> Result<T, PolicyOutboxError> {
    let bytes = fs::read(path)?;
    let value: T = serde_json::from_slice(&bytes)?;
    if serde_json::to_vec(&value)? != bytes {
        return Err(invalid("noncanonical policy witness JSON").into());
    }
    Ok(value)
}

fn record_path(dir: &Path, id: &PolicyWitnessId, extension: &str) -> PathBuf {
    let mut hash = Sha256::new();
    hash.update(b"asteroidb/policy-witness-outbox-path/v1");
    for field in [
        &id.destination.0,
        &id.subject.namespace_id,
        &id.subject.event_dot.writer.0,
    ] {
        hash.update((field.len() as u64).to_be_bytes());
        hash.update(field.as_bytes());
    }
    hash.update(id.subject.event_dot.counter.to_be_bytes());
    dir.join(format!("{}.{}", hex::encode(hash.finalize()), extension))
}

fn write_new(dir: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    if path.exists() {
        return Err(invalid("policy witness file already exists"));
    }
    let temp = dir.join(format!(
        ".tmp-{}-{}.tmp",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        fsync_dir(dir)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn atomic_replace(dir: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temp = dir.join(format!(
        ".tmp-{}-{}.tmp",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        fsync_dir(dir)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::writer_auth::{SignedBoundOperation, SignedCreation};
    use crate::hlc::HlcTimestamp;
    use crate::operation::{
        BoundOperation, CandidateKvAction, CertificationMode, Declaration, EventDot, OperationLeaf,
    };
    use ed25519_dalek::SigningKey;
    use tempfile::tempdir;

    fn packet() -> OperationPacket {
        let key = SigningKey::from_bytes(&[9; 32]);
        let subject = CrdtSubject {
            namespace_id: "namespace".into(),
            event_dot: EventDot {
                writer: NodeId("creator".into()),
                counter: 7,
            },
        };
        let bound = BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: "key".into(),
                    value_bytes: b"value".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 1,
                        logical: 0,
                        node_id: "creator".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject,
                key: "key".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let creation = SignedCreation::sign(&bound, &key).unwrap();
        OperationPacket {
            signed: SignedBoundOperation::sign(bound, &key, creation).unwrap(),
        }
    }

    fn open(dir: &Path) -> OperationPolicyOutbox {
        OperationPolicyOutbox::open(dir, NodeId("sender".into()), [1; 32], [2; 32]).unwrap()
    }

    #[test]
    fn interrupted_first_pin_clears_temp_before_reusing_its_name() {
        let dir = tempdir().unwrap();
        let next = TEMP_SEQUENCE.load(Ordering::Relaxed);
        let stale = (0..32)
            .map(|offset| {
                dir.path()
                    .join(format!(".tmp-{}-{}.tmp", std::process::id(), next + offset))
            })
            .collect::<Vec<_>>();
        for path in &stale {
            fs::write(path, b"incomplete pin").unwrap();
        }
        fsync_dir(dir.path()).unwrap();
        drop(open(dir.path()));
        assert!(dir.path().join("manifest.json").exists());
        assert!(stale.iter().all(|path| !path.exists()));

        let dir = tempdir().unwrap();
        fs::write(dir.path().join("orphan.send"), b"stored obligation").unwrap();
        assert!(
            OperationPolicyOutbox::open(dir.path(), NodeId("sender".into()), [1; 32], [2; 32],)
                .is_err()
        );
        assert!(!dir.path().join("manifest.json").exists());
    }

    #[test]
    fn preflight_rejects_changed_route_without_rewriting_pin() {
        let dir = tempdir().unwrap();
        drop(open(dir.path()));
        let original = fs::read(dir.path().join("manifest.json")).unwrap();
        assert!(
            OperationPolicyOutbox::verify_pin(
                dir.path(),
                &NodeId("sender".into()),
                [3; 32],
                [2; 32],
            )
            .is_err()
        );
        assert_eq!(
            fs::read(dir.path().join("manifest.json")).unwrap(),
            original
        );
    }

    #[test]
    fn durable_retry_and_authenticated_receipt() {
        let dir = tempdir().unwrap();
        let packet = packet();
        let destination = NodeId("final".into());
        let next_hop = NodeId("adjacent".into());
        let id = {
            let mut outbox = open(dir.path());
            let id = outbox
                .enqueue(
                    destination.clone(),
                    next_hop.clone(),
                    "http://peer".into(),
                    packet.clone(),
                )
                .unwrap();
            assert_eq!(outbox.pending().len(), 1);
            assert_eq!(
                outbox
                    .enqueue(
                        destination.clone(),
                        next_hop.clone(),
                        "http://peer".into(),
                        packet.clone()
                    )
                    .unwrap(),
                id
            );
            assert!(matches!(
                outbox.enqueue(
                    destination.clone(),
                    NodeId("other".into()),
                    "http://peer".into(),
                    packet.clone()
                ),
                Err(PolicyOutboxError::Conflict)
            ));
            id
        };
        let mut outbox = open(dir.path());
        let pending = outbox.pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].packet, Some(packet.clone()));
        assert_eq!(pending[0].next_hop, next_hop);
        assert_eq!(pending[0].peer_addr, "http://peer");
        let receipt = OperationReceipt {
            commitment: packet.commitment(),
            already_present: false,
        };
        assert!(matches!(
            outbox.record_authenticated_receipt(&id, &NodeId("other".into()), &receipt),
            Err(PolicyOutboxError::WrongReceipt)
        ));
        let wrong = OperationReceipt {
            commitment: [8; 32],
            already_present: false,
        };
        assert!(matches!(
            outbox.record_authenticated_receipt(&id, &next_hop, &wrong),
            Err(PolicyOutboxError::WrongReceipt)
        ));
        assert_eq!(outbox.pending().len(), 1);
        outbox
            .record_authenticated_receipt(&id, &next_hop, &receipt)
            .unwrap();
        assert!(outbox.pending().is_empty());
        assert_eq!(outbox.entries().len(), 1);
        assert!(outbox.entries()[0].acknowledged);
        assert_eq!(outbox.entries()[0].packet, Some(packet.clone()));
        assert_eq!(outbox.compact_acked_and_sync().unwrap(), 1);
        assert_eq!(outbox.compact_acked_and_sync().unwrap(), 0);
        assert!(outbox.entries()[0].packet.is_none());
        assert!(outbox.has_durable_receipt(
            &destination,
            &packet.signed.bound.operation.subject,
            packet.commitment(),
        ));
        assert_eq!(
            outbox
                .enqueue(
                    destination.clone(),
                    next_hop.clone(),
                    "http://peer".into(),
                    packet.clone()
                )
                .unwrap(),
            id
        );
        assert!(matches!(
            outbox.enqueue(
                destination.clone(),
                NodeId("other".into()),
                "http://peer".into(),
                packet.clone()
            ),
            Err(PolicyOutboxError::Conflict)
        ));
        let on_disk: serde_json::Value =
            serde_json::from_slice(&fs::read(record_path(dir.path(), &id, "send")).unwrap())
                .unwrap();
        assert!(on_disk.get("packet").is_none());
        drop(outbox);
        let outbox = open(dir.path());
        assert!(outbox.pending().is_empty());
        assert!(outbox.entries()[0].acknowledged);
        assert!(outbox.entries()[0].packet.is_none());
    }

    #[test]
    fn uncertain_ack_is_recovered_under_the_same_directory_lease() {
        let dir = tempdir().unwrap();
        let packet = packet();
        let mut outbox = open(dir.path());
        let id = outbox
            .enqueue(
                NodeId("final".into()),
                NodeId("adjacent".into()),
                "http://peer".into(),
                packet.clone(),
            )
            .unwrap();
        // Model a failure reported after the ACK rename: the committed file
        // exists, while the in-memory map was not updated and sends are
        // stopped until recovery validates the directory.
        let ack = AckRecord {
            version: VERSION,
            id: id.clone(),
            commitment: packet.commitment(),
        };
        write_new(
            dir.path(),
            &record_path(dir.path(), &id, "ack"),
            &serde_json::to_vec(&ack).unwrap(),
        )
        .unwrap();
        outbox.poisoned = true;
        assert!(outbox.pending().is_empty());
        outbox.recover_if_poisoned().unwrap();
        assert!(outbox.pending().is_empty());
        assert!(outbox.entries()[0].acknowledged);
        assert!(matches!(
            OperationPolicyOutbox::open(dir.path(), NodeId("sender".into()), [1; 32], [2; 32]),
            Err(PolicyOutboxError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn published_compaction_recovers_and_rejects_a_rebound_identity() {
        let dir = tempdir().unwrap();
        let packet = packet();
        let mut outbox = open(dir.path());
        let id = outbox
            .enqueue(
                NodeId("final".into()),
                NodeId("adjacent".into()),
                "http://peer".into(),
                packet.clone(),
            )
            .unwrap();
        outbox
            .record_authenticated_receipt(
                &id,
                &NodeId("adjacent".into()),
                &OperationReceipt {
                    commitment: packet.commitment(),
                    already_present: false,
                },
            )
            .unwrap();
        let record = CompactSendRecord {
            version: 2,
            sender: NodeId("sender".into()),
            id: id.clone(),
            next_hop: NodeId("adjacent".into()),
            peer_addr: "http://peer".into(),
            identity: PolicyWitnessIdentity::from(&packet),
        };
        let path = record_path(dir.path(), &id, "send");
        atomic_replace(dir.path(), &path, &serde_json::to_vec(&record).unwrap()).unwrap();
        outbox.mark_uncertain_for_test();
        outbox.recover_if_poisoned().unwrap();
        assert!(outbox.pending().is_empty());
        assert!(outbox.entries()[0].packet.is_none());
        drop(outbox);

        let mut rebound = record;
        rebound.identity.packet_commitment[0] ^= 1;
        atomic_replace(dir.path(), &path, &serde_json::to_vec(&rebound).unwrap()).unwrap();
        assert!(
            OperationPolicyOutbox::open(dir.path(), NodeId("sender".into()), [1; 32], [2; 32])
                .is_err()
        );
    }

    #[test]
    fn recovery_rejects_manifest_or_corrupt_record() {
        let dir = tempdir().unwrap();
        let id = {
            let mut outbox = open(dir.path());
            outbox
                .enqueue(
                    NodeId("final".into()),
                    NodeId("adjacent".into()),
                    "http://peer".into(),
                    packet(),
                )
                .unwrap()
        };
        assert!(
            OperationPolicyOutbox::open(dir.path(), NodeId("sender".into()), [3; 32], [2; 32])
                .is_err()
        );
        let path = record_path(dir.path(), &id, "send");
        let mut record: SendRecord = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        record.packet_commitment[0] ^= 1;
        fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
        assert!(
            OperationPolicyOutbox::open(dir.path(), NodeId("sender".into()), [1; 32], [2; 32])
                .is_err()
        );
    }
}
