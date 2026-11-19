//! Durable custody-send obligations for exact operation packets.
//!
//! Store this directory outside the ordinary CRDT WAL and its checkpoint
//! pruning. Each send has a durable body file. A matching receiver receipt
//! is separately made durable before the send leaves the retry set. Once both
//! that receipt is durable (and terminal completion for routed sends), a
//! closed-index snapshot can replace their details and the signed packet.
//! Its immutable identity metadata remains for reconciliation. This module
//! does not discover required destinations
//! or establish the receiver's identity; the caller must select a trusted
//! adjacent peer and use an authenticated transport to it.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::operation::CrdtSubject;
use crate::store::backend::{create_dir_all_durable, fsync_dir};

use super::operation_link_auth::OperationLinkAuth;
use super::operation_transport::{
    OperationCompletionReport, OperationPacket, OperationRetirementNotice, OperationTransfer,
    OperationTransferReceipt, OperationTransportClient, SendError, TransferId,
};
use crate::types::NodeId;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const OUTBOX_LOCK: &str = ".lock";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SendRecord {
    version: u32,
    id: u64,
    peer_addr: String,
    sender: NodeId,
    receiver: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    destination: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent: Option<TransferId>,
    #[serde(default, skip_serializing_if = "is_zero")]
    depth: u64,
    packet: OperationPacket,
}

/// A completed transfer still needs its identity for enrollment, child
/// completion, and sender-wide retirement notices, but has no retry body.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactSendRecord {
    version: u32,
    id: u64,
    peer_addr: String,
    sender: NodeId,
    receiver: NodeId,
    destination: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent: Option<TransferId>,
    #[serde(default, skip_serializing_if = "is_zero")]
    depth: u64,
    subject: CrdtSubject,
    commitment: [u8; 32],
}

/// Non-routed send identity retained after the neighbor's durable ACK.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactNeighborSendRecord {
    version: u32,
    id: u64,
    peer_addr: String,
    sender: NodeId,
    receiver: NodeId,
    subject: CrdtSubject,
    commitment: [u8; 32],
}

/// Exclusive contiguous closure boundary plus closed IDs above holes.
/// `.send` identity descriptors remain present for every ID represented here.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ClosedIndex {
    version: u32,
    before: u64,
    sparse: BTreeSet<u64>,
}

impl ClosedIndex {
    fn contains(&self, id: u64) -> bool {
        id < self.before || self.sparse.contains(&id)
    }

    fn insert(&mut self, id: u64) -> Result<(), OutboxError> {
        if self.contains(id) {
            return Ok(());
        }
        self.sparse.insert(id);
        while self.sparse.remove(&self.before) {
            self.before = self.before.checked_add(1).ok_or(OutboxError::Exhausted)?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ClosureJournal {
    version: u32,
    id: u64,
    commitment: [u8; 32],
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub id: u64,
    pub peer_addr: String,
    pub sender: NodeId,
    pub receiver: NodeId,
    pub destination: Option<NodeId>,
    pub parent: Option<TransferId>,
    pub depth: u64,
    pub subject: CrdtSubject,
    pub commitment: [u8; 32],
    pub packet: Option<OperationPacket>,
    pub receipt: Option<OperationTransferReceipt>,
    pub completion: Option<OperationCompletionReport>,
    pub closed: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    #[error("outbox I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("outbox serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("outbox send failed: {0}")]
    Send(#[from] SendError),
    #[error("unknown outbox transfer {0}")]
    Unknown(u64),
    #[error("receiver receipt does not match transfer {0}")]
    WrongReceipt(u64),
    #[error(
        "operation ID already has different packet content for peer {peer_addr} (transfer {transfer_id})"
    )]
    ConflictingOperation { peer_addr: String, transfer_id: u64 },
    #[error("outbox transfer sequence exhausted")]
    Exhausted,
    #[error(
        "legacy operation outbox format; move this directory aside while stopped, then restart to replay signed inbox packets into a new outbox"
    )]
    LegacyFormat,
}

/// One writer must own this directory at a time. Call `open` before serving
/// traffic; any corrupt committed file fails recovery rather than silently
/// dropping an outstanding send.
pub struct OperationOutbox {
    dir: PathBuf,
    lock: File,
    sender: NodeId,
    routed: bool,
    next_id: u64,
    entries: BTreeMap<u64, OutboxEntry>,
    closed: ClosedIndex,
    poisoned: bool,
}

impl OperationOutbox {
    /// The link identity and address pin must survive a restart alongside
    /// outstanding `.send` descriptors.
    pub fn verify_link_manifest(&self, digest: [u8; 32]) -> Result<(), OutboxError> {
        let path = self.dir.join(".link-manifest");
        if path.exists() {
            if fs::read(&path)? != digest {
                return Err(
                    invalid_data("operation link identities changed after enrollment").into(),
                );
            }
            return Ok(());
        }
        if !self.entries.is_empty() || self.closed.before != 0 || !self.closed.sparse.is_empty() {
            return Err(invalid_data(
                "operation outbox has unsigned existing custody records; refuse link-auth migration",
            )
            .into());
        }
        for file in fs::read_dir(&self.dir)? {
            let name = file?.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".send")
                || name.ends_with(".ack")
                || name.ends_with(".done")
                || name.ends_with(".closed")
            {
                return Err(invalid_data(
                    "operation outbox has unsigned completion records; refuse link-auth migration",
                )
                .into());
            }
        }
        Ok(())
    }

    pub fn ensure_link_manifest(&self, digest: [u8; 32]) -> Result<(), OutboxError> {
        self.verify_link_manifest(digest)?;
        atomic_create(&self.dir, &self.dir.join(".link-manifest"), &digest)?;
        Ok(())
    }
    pub fn is_routed(&self) -> bool {
        self.routed
    }

    pub fn verify_routed_manifest(&self, digest: [u8; 32]) -> Result<(), OutboxError> {
        if !self.routed {
            return Err(invalid_data("routing manifest used with legacy outbox").into());
        }
        let path = self.dir.join(".routing");
        if path.exists() && fs::read(path)? != hex::encode(digest).as_bytes() {
            return Err(invalid_data("outbox routing manifest changed").into());
        }
        Ok(())
    }

    pub fn ensure_routed_manifest(&self, digest: [u8; 32]) -> Result<(), OutboxError> {
        self.verify_routed_manifest(digest)?;
        atomic_create(
            &self.dir,
            &self.dir.join(".routing"),
            hex::encode(digest).as_bytes(),
        )?;
        Ok(())
    }

    /// Replace a pinned route only before this outbox has ever assigned a
    /// transfer ID. The caller must independently establish that its inbox has
    /// no received transfer or reclaimed root bound to the previous route.
    /// Retained local roots are safe: they can be enrolled after this pin is
    /// durable. A failed rename or directory sync leaves this handle poisoned;
    /// startup can inspect the old or new pin and retry before serving traffic.
    pub(crate) fn replace_routed_manifest_if_pristine(
        &mut self,
        digest: [u8; 32],
    ) -> Result<(), OutboxError> {
        if !self.routed {
            return Err(invalid_data("routing manifest used with legacy outbox").into());
        }
        if self.poisoned
            || self.next_id != 0
            || !self.entries.is_empty()
            || self.closed.before != 0
            || !self.closed.sparse.is_empty()
        {
            return Err(
                invalid_data("routing manifest cannot change after custody enrollment").into(),
            );
        }
        let path = self.dir.join(".routing");
        let bytes = hex::encode(digest);
        if path.exists() {
            let previous = fs::read(&path)?;
            if previous == bytes.as_bytes() {
                return Ok(());
            }
            if !matches!(hex::decode(&previous), Ok(decoded) if decoded.len() == 32) {
                return Err(invalid_data("outbox routing manifest is malformed").into());
            }
        }
        self.poisoned = true;
        let result = if path.exists() {
            atomic_replace(&self.dir, &path, bytes.as_bytes())
        } else {
            atomic_create(&self.dir, &path, bytes.as_bytes())
        };
        if result.is_ok() {
            self.poisoned = false;
        }
        result.map_err(OutboxError::Io)
    }

    pub(crate) fn storage_directory(&self) -> &Path {
        &self.dir
    }
    #[cfg(test)]
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, OutboxError> {
        Self::open_for_node(dir, NodeId("local-test-node".into()))
    }

    pub fn open_for_node(dir: impl AsRef<Path>, sender: NodeId) -> Result<Self, OutboxError> {
        Self::open_mode(dir, sender, false)
    }

    pub fn open_routed_for_node(
        dir: impl AsRef<Path>,
        sender: NodeId,
    ) -> Result<Self, OutboxError> {
        Self::open_mode(dir, sender, true)
    }

    fn open_mode(dir: impl AsRef<Path>, sender: NodeId, routed: bool) -> Result<Self, OutboxError> {
        let dir = dir.as_ref();
        create_dir_all_durable(dir)?;
        let lock_path = dir.join(OUTBOX_LOCK);
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
        Self::load_mode(dir, sender, routed, lock)
    }

    fn load_mode(
        dir: &Path,
        sender: NodeId,
        routed: bool,
        lock: File,
    ) -> Result<Self, OutboxError> {
        let mode_path = dir.join(".mode");
        let mode_bytes: &[u8] = if routed { b"routed-v2" } else { b"neighbor-v1" };
        if mode_path.exists() && fs::read(&mode_path)? != mode_bytes {
            return Err(invalid_data("operation outbox custody mode changed").into());
        }
        let mut entries = BTreeMap::new();
        let mut max_id = None::<u64>;
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(invalid_data("non-UTF8 outbox filename").into());
            };
            if name.starts_with(".tmp-") {
                // A crash before rename never enrolled a send. The old temp
                // file cannot mask or replace a committed transfer.
                fs::remove_file(path)?;
                continue;
            }
            if name == ".mode" || name == OUTBOX_LOCK {
                continue;
            }
            if name == ".routing"
                || name == ".next-id"
                || name == ".index"
                || name == ".link-manifest"
            {
                continue;
            }
            let Some(stem) = name.strip_suffix(".send") else {
                if name.ends_with(".ack") || name.ends_with(".done") || name.ends_with(".closed") {
                    continue;
                }
                return Err(invalid_data("unknown outbox file").into());
            };
            let id = stem
                .parse::<u64>()
                .map_err(|_| invalid_data("invalid transfer ID"))?;
            if stem != id.to_string() {
                return Err(invalid_data("noncanonical transfer ID").into());
            }
            let bytes = fs::read(path)?;
            let raw: serde_json::Value = serde_json::from_slice(&bytes)?;
            if raw.get("version").and_then(serde_json::Value::as_u64) == Some(1) {
                return Err(OutboxError::LegacyFormat);
            }
            let version = raw.get("version").and_then(serde_json::Value::as_u64);
            let mut entry = if version == Some(5) {
                let record: CompactNeighborSendRecord = serde_json::from_slice(&bytes)?;
                if routed
                    || record.id != id
                    || record.peer_addr.is_empty()
                    || record.sender != sender
                    || record.receiver == sender
                {
                    return Err(invalid_data("invalid compact neighbor send record").into());
                }
                OutboxEntry {
                    id,
                    peer_addr: record.peer_addr,
                    sender: record.sender,
                    receiver: record.receiver,
                    destination: None,
                    parent: None,
                    depth: 0,
                    subject: record.subject,
                    commitment: record.commitment,
                    packet: None,
                    receipt: None,
                    completion: None,
                    closed: false,
                }
            } else if version == Some(4) {
                let record: CompactSendRecord = serde_json::from_slice(&bytes)?;
                if !routed
                    || record.id != id
                    || record.peer_addr.is_empty()
                    || record.sender != sender
                    || record.receiver == sender
                    || record.parent.is_some() != (record.depth != 0)
                {
                    return Err(invalid_data("invalid compact send record").into());
                }
                OutboxEntry {
                    id,
                    peer_addr: record.peer_addr,
                    sender: record.sender,
                    receiver: record.receiver,
                    destination: Some(record.destination),
                    parent: record.parent,
                    depth: record.depth,
                    subject: record.subject,
                    commitment: record.commitment,
                    packet: None,
                    receipt: None,
                    completion: None,
                    closed: false,
                }
            } else {
                let record: SendRecord = serde_json::from_slice(&bytes)?;
                if record.version != if routed { 3 } else { 2 }
                    || record.id != id
                    || record.peer_addr.is_empty()
                    || record.sender != sender
                    || record.receiver == sender
                    || (routed
                        && (record.destination.is_none()
                            || record.depth == 0 && record.parent.is_some()))
                    || (!routed
                        && (record.destination.is_some()
                            || record.parent.is_some()
                            || record.depth != 0))
                {
                    return Err(invalid_data("invalid committed send record").into());
                }
                OutboxEntry {
                    id,
                    peer_addr: record.peer_addr,
                    sender: record.sender,
                    receiver: record.receiver,
                    destination: record.destination,
                    parent: record.parent,
                    depth: record.depth,
                    subject: record.packet.signed.bound.operation.subject.clone(),
                    commitment: record.packet.commitment(),
                    packet: Some(record.packet),
                    receipt: None,
                    completion: None,
                    closed: false,
                }
            };
            let receipt_path = ack_path(dir, id);
            let receipt: Option<OperationTransferReceipt> = if receipt_path.exists() {
                let receipt = serde_json::from_slice(&fs::read(receipt_path)?)?;
                if !matches_entry(&entry, &receipt) {
                    return Err(OutboxError::WrongReceipt(id));
                }
                Some(receipt)
            } else {
                None
            };
            let completion = if done_path(dir, id).exists() {
                let report: OperationCompletionReport =
                    serde_json::from_slice(&fs::read(done_path(dir, id))?)?;
                if !routed || report.version != 1 || !matches_transfer(&entry, &report.transfer) {
                    return Err(invalid_data("completion report does not match transfer").into());
                }
                Some(report)
            } else {
                None
            };
            max_id = Some(max_id.map_or(id, |max| max.max(id)));
            entry.receipt = receipt;
            entry.completion = completion;
            entries.insert(id, entry);
        }
        // A receipt without its exact send body is never accepted.
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let filename = name.to_string_lossy();
            if let Some(stem) = filename
                .strip_suffix(".ack")
                .or_else(|| filename.strip_suffix(".done"))
                .or_else(|| filename.strip_suffix(".closed"))
            {
                let id = stem
                    .parse::<u64>()
                    .map_err(|_| invalid_data("invalid receipt ID"))?;
                if !entries.contains_key(&id) || stem != id.to_string() {
                    return Err(invalid_data("orphan or noncanonical receipt").into());
                }
            }
        }
        fsync_dir(dir)?; // includes any torn temporary-file cleanup
        atomic_create(dir, &mode_path, mode_bytes)?;
        let derived_next = match max_id {
            Some(id) => id.checked_add(1).ok_or(OutboxError::Exhausted)?,
            None => 0,
        };
        // All descriptors remain present in this sender-side phase. Reject
        // gaps so a corrupt high-water or missing descriptor cannot alias an
        // archived transfer after restart.
        if entries.len() as u64 != derived_next
            || entries
                .keys()
                .copied()
                .enumerate()
                .any(|(i, id)| i as u64 != id)
        {
            return Err(invalid_data("outbox descriptor sequence has a hole").into());
        }
        let high_water_path = dir.join(".next-id");
        let saved_next = if high_water_path.exists() {
            fs::read_to_string(&high_water_path)?
                .parse::<u64>()
                .map_err(|_| invalid_data("invalid outbox next ID"))?
        } else {
            0
        };
        if saved_next > derived_next {
            return Err(invalid_data("outbox next ID exceeds retained descriptors").into());
        }
        // A crash can leave a durable .send before the high-water update.
        // Repair it before any new ID is handed out.
        if saved_next != derived_next || !high_water_path.exists() {
            atomic_replace(dir, &high_water_path, derived_next.to_string().as_bytes())?;
        }
        let index_path = dir.join(".index");
        let mut closed: ClosedIndex = if index_path.exists() {
            serde_json::from_slice(&fs::read(&index_path)?)?
        } else {
            ClosedIndex {
                version: 1,
                ..Default::default()
            }
        };
        if closed.version != 1
            || closed.before > derived_next
            || closed
                .sparse
                .iter()
                .any(|id| *id < closed.before || *id >= derived_next)
        {
            return Err(invalid_data("invalid outbox closed index").into());
        }
        if entries
            .values()
            .any(|entry| entry.packet.is_none() && !closed.contains(entry.id))
        {
            return Err(invalid_data("compact send record is not closed").into());
        }
        let mut journals = Vec::new();
        for file in fs::read_dir(dir)? {
            let file = file?;
            let filename = file.file_name();
            let Some(stem) = filename
                .to_str()
                .and_then(|name| name.strip_suffix(".closed"))
            else {
                continue;
            };
            let id: u64 = stem
                .parse()
                .map_err(|_| invalid_data("invalid closure journal ID"))?;
            let journal: ClosureJournal = serde_json::from_slice(&fs::read(file.path())?)?;
            let entry = entries
                .get(&id)
                .ok_or_else(|| invalid_data("orphan closure journal"))?;
            if stem != id.to_string()
                || journal.version != 1
                || journal.id != id
                || journal.commitment != entry.commitment
            {
                return Err(invalid_data("closure journal does not match descriptor").into());
            }
            if !closed.contains(id)
                && (entry.receipt.is_none() || (routed && entry.completion.is_none()))
            {
                return Err(invalid_data("unfinished closure journal").into());
            }
            closed.insert(id)?;
            journals.push(file.path());
        }
        // This snapshot is durable before removing any ACK/DONE detail.
        atomic_replace(dir, &index_path, &serde_json::to_vec(&closed)?)?;
        for entry in entries.values_mut() {
            if closed.contains(entry.id) {
                entry.closed = true;
                if entry.packet.is_some() {
                    atomic_replace(
                        dir,
                        &send_path(dir, entry.id),
                        &compact_record_for_mode(entry, routed)?,
                    )?;
                    entry.packet = None;
                }
                remove_if_exists(&ack_path(dir, entry.id))?;
                remove_if_exists(&done_path(dir, entry.id))?;
                entry.receipt = None;
                entry.completion = None;
            }
        }
        fsync_dir(dir)?;
        for path in journals {
            remove_if_exists(&path)?;
        }
        fsync_dir(dir)?;
        Ok(Self {
            dir: dir.to_owned(),
            lock,
            sender,
            routed,
            next_id: derived_next,
            entries,
            closed,
            poisoned: false,
        })
    }

    /// Re-read all durable descriptors, ACKs, completions, and closure
    /// journals after a write whose outcome is uncertain. Keep this handle
    /// poisoned if validation or recovery fails, so no pending send or new
    /// transfer ID can be inferred from stale memory.
    pub fn recover_if_poisoned(&mut self) -> Result<(), OutboxError> {
        if self.poisoned {
            let recovered = Self::load_mode(
                &self.dir,
                self.sender.clone(),
                self.routed,
                self.lock.try_clone()?,
            )?;
            *self = recovered;
        }
        Ok(())
    }

    /// Enroll a complete packet and destination before any network send.
    /// Repeated enrollment of the same obligation reuses its transfer ID.
    #[cfg(test)]
    pub fn enqueue(
        &mut self,
        peer_addr: String,
        packet: OperationPacket,
    ) -> Result<u64, OutboxError> {
        self.ensure_enqueued(peer_addr, packet)
    }

    /// Reconcile a required neighbor obligation with durable transport state.
    /// An existing ACK remains complete; a pending send keeps the exact prior
    /// packet bytes. The same operation ID cannot be rebound to another body
    /// or signature for this peer, even after the earlier send was ACKed.
    #[cfg(test)]
    pub fn ensure_enqueued(
        &mut self,
        peer_addr: String,
        packet: OperationPacket,
    ) -> Result<u64, OutboxError> {
        self.ensure_enqueued_to(NodeId(peer_addr.clone()), peer_addr, packet)
    }

    pub fn ensure_enqueued_to(
        &mut self,
        receiver: NodeId,
        peer_addr: String,
        packet: OperationPacket,
    ) -> Result<u64, OutboxError> {
        if self.routed {
            return Err(invalid_data("legacy neighbor enqueue in routed outbox").into());
        }
        self.ensure_enqueued_transfer(receiver, peer_addr, packet, None, None, 0)
    }

    pub fn ensure_routed(
        &mut self,
        receiver: NodeId,
        peer_addr: String,
        packet: OperationPacket,
        destination: NodeId,
        parent: Option<TransferId>,
        depth: u64,
    ) -> Result<u64, OutboxError> {
        if !self.routed || (parent.is_none() != (depth == 0)) {
            return Err(invalid_data("invalid routed enrollment mode or depth").into());
        }
        self.ensure_enqueued_transfer(
            receiver,
            peer_addr,
            packet,
            Some(destination),
            parent,
            depth,
        )
    }

    fn ensure_enqueued_transfer(
        &mut self,
        receiver: NodeId,
        peer_addr: String,
        packet: OperationPacket,
        destination: Option<NodeId>,
        parent: Option<TransferId>,
        depth: u64,
    ) -> Result<u64, OutboxError> {
        if self.poisoned {
            return Err(invalid_data("outbox must be reopened after uncertain write").into());
        }
        if peer_addr.is_empty() {
            return Err(invalid_data("empty peer address").into());
        }
        let subject = &packet.signed.bound.operation.subject;
        for entry in self.entries.values().filter(|entry| {
            if self.routed {
                entry.destination == destination && entry.parent == parent
            } else {
                entry.receiver == receiver
            }
        }) {
            if entry.subject == *subject {
                return if entry.commitment == packet.commitment()
                    && entry.peer_addr == peer_addr
                    && entry.receiver == receiver
                    && entry.depth == depth
                {
                    Ok(entry.id)
                } else {
                    Err(OutboxError::ConflictingOperation {
                        peer_addr: peer_addr.clone(),
                        transfer_id: entry.id,
                    })
                };
            }
        }
        let id = self.next_id;
        let next = id.checked_add(1).ok_or(OutboxError::Exhausted)?;
        let record = SendRecord {
            version: if self.routed { 3 } else { 2 },
            id,
            peer_addr: peer_addr.clone(),
            sender: self.sender.clone(),
            receiver: receiver.clone(),
            destination: destination.clone(),
            parent: parent.clone(),
            depth,
            packet: packet.clone(),
        };
        self.poisoned = true;
        atomic_create(
            &self.dir,
            &send_path(&self.dir, id),
            &serde_json::to_vec(&record)?,
        )?;
        atomic_replace(
            &self.dir,
            &self.dir.join(".next-id"),
            next.to_string().as_bytes(),
        )?;
        self.entries.insert(
            id,
            OutboxEntry {
                id,
                peer_addr,
                sender: self.sender.clone(),
                receiver,
                destination,
                parent,
                depth,
                subject: packet.signed.bound.operation.subject.clone(),
                commitment: packet.commitment(),
                packet: Some(packet),
                receipt: None,
                completion: None,
                closed: false,
            },
        );
        self.next_id = next;
        self.poisoned = false;
        Ok(id)
    }

    pub fn get(&self, id: u64) -> Option<&OutboxEntry> {
        self.entries.get(&id)
    }

    pub fn pending(&self) -> impl Iterator<Item = &OutboxEntry> {
        self.entries
            .values()
            .filter(|entry| !self.poisoned && !entry.closed && entry.receipt.is_none())
    }

    /// Inspect durable send descriptors while deciding whether an independent
    /// ledger copy can be replaced by an ID receipt. A poisoned outbox has an
    /// uncertain write and cannot establish transport release.
    pub fn gc_entries(&self) -> Option<impl Iterator<Item = &OutboxEntry>> {
        (!self.poisoned).then(|| self.entries.values())
    }

    /// Completion is durable metadata independent of the custody ACK. A
    /// report never removes an entry from the send retry set.
    pub fn record_completion(
        &mut self,
        report: OperationCompletionReport,
    ) -> Result<(), OutboxError> {
        if self.poisoned {
            return Err(invalid_data("outbox must be reopened after uncertain write").into());
        }
        let id = report.transfer.transfer_id;
        let entry = self.entries.get_mut(&id).ok_or(OutboxError::Unknown(id))?;
        if !self.routed || report.version != 1 || !matches_transfer(entry, &report.transfer) {
            return Err(invalid_data("completion report does not match transfer").into());
        }
        if entry.closed {
            return Ok(());
        }
        self.poisoned = true;
        atomic_create(
            &self.dir,
            &done_path(&self.dir, id),
            &serde_json::to_vec(&report)?,
        )?;
        entry.completion = Some(report);
        self.poisoned = false;
        Ok(())
    }

    pub fn completed_child(
        &self,
        parent: &TransferId,
        destination: &NodeId,
        packet: &OperationPacket,
    ) -> bool {
        !self.poisoned
            && self.entries.values().any(|entry| {
                entry.parent.as_ref() == Some(parent)
                    && entry.destination.as_ref() == Some(destination)
                    && entry.subject == packet.signed.bound.operation.subject
                    && entry.commitment == packet.commitment()
                    && (entry.closed || (entry.receipt.is_some() && entry.completion.is_some()))
            })
    }

    /// Record a receipt for the exact packet. This is the only transition
    /// that removes an obligation from `pending`; ACK metadata and body remain.
    pub fn record_receipt(
        &mut self,
        id: u64,
        receipt: OperationTransferReceipt,
    ) -> Result<(), OutboxError> {
        if self.poisoned {
            return Err(invalid_data("outbox must be reopened after uncertain write").into());
        }
        let entry = self.entries.get_mut(&id).ok_or(OutboxError::Unknown(id))?;
        if !matches_entry(entry, &receipt) {
            return Err(OutboxError::WrongReceipt(id));
        }
        if entry.closed || entry.receipt.is_some() {
            return Ok(());
        }
        self.poisoned = true;
        let path = ack_path(&self.dir, id);
        if path.exists() {
            // A previous attempt may have written this ACK before failing
            // directory sync. Its `already_present` flag can differ from a
            // later receiver retry, while its packet commitment must match.
            let stored: OperationTransferReceipt = serde_json::from_slice(&fs::read(&path)?)?;
            if !matches_entry(entry, &stored) {
                return Err(OutboxError::WrongReceipt(id));
            }
            File::open(&path)?.sync_all()?;
            fsync_dir(&self.dir)?;
            entry.receipt = Some(stored);
            self.poisoned = false;
            return Ok(());
        }
        atomic_create(&self.dir, &path, &serde_json::to_vec(&receipt)?)?;
        entry.receipt = Some(receipt);
        self.poisoned = false;
        Ok(())
    }

    /// Close one ACKed transfer (also completed when routed), then compact
    /// its ACK and completion body.
    /// A journal survives crashes until the closed-index snapshot is durable.
    /// The `.send` identity descriptor remains for route reconciliation and
    /// parent-child matching. Its packet bytes can be replaced only after the
    /// closed index is durable.
    pub fn compact_one(&mut self) -> Result<Option<u64>, OutboxError> {
        if self.poisoned {
            return Err(invalid_data("outbox must be reopened after uncertain write").into());
        }
        let Some((&id, entry)) = self.entries.iter().find(|(_, entry)| {
            !entry.closed && entry.receipt.is_some() && (!self.routed || entry.completion.is_some())
        }) else {
            return Ok(None);
        };
        let journal = ClosureJournal {
            version: 1,
            id,
            commitment: entry.commitment,
        };
        self.poisoned = true;
        atomic_create(
            &self.dir,
            &closed_path(&self.dir, id),
            &serde_json::to_vec(&journal)?,
        )?;
        let mut next = self.closed.clone();
        next.insert(id)?;
        atomic_replace(
            &self.dir,
            &self.dir.join(".index"),
            &serde_json::to_vec(&next)?,
        )?;
        self.closed = next;
        self.entries.get_mut(&id).unwrap().closed = true;
        let entry = self.entries.get_mut(&id).unwrap();
        atomic_replace(
            &self.dir,
            &send_path(&self.dir, id),
            &compact_record_for_mode(entry, self.routed)?,
        )?;
        entry.packet = None;
        remove_if_exists(&ack_path(&self.dir, id))?;
        remove_if_exists(&done_path(&self.dir, id))?;
        fsync_dir(&self.dir)?;
        let entry = self.entries.get_mut(&id).unwrap();
        entry.receipt = None;
        entry.completion = None;
        remove_if_exists(&closed_path(&self.dir, id))?;
        fsync_dir(&self.dir)?;
        self.poisoned = false;
        Ok(Some(id))
    }

    pub fn closed_before(&self) -> u64 {
        self.closed.before
    }

    /// A single sender-wide saved prefix is safe to announce to every
    /// original receiver represented in that prefix, even when IDs alternate
    /// between neighbors. Send identity descriptors remain on disk.
    pub fn retirement_notices(&self) -> Vec<(String, OperationRetirementNotice)> {
        if !self.routed || self.poisoned || self.closed.before == 0 {
            return Vec::new();
        }
        let mut peers = BTreeMap::new();
        for entry in self
            .entries
            .range(..self.closed.before)
            .map(|(_, entry)| entry)
        {
            peers
                .entry(entry.receiver.clone())
                .or_insert_with(|| entry.peer_addr.clone());
        }
        peers
            .into_iter()
            .map(|(receiver, addr)| {
                (
                    addr,
                    OperationRetirementNotice {
                        version: 1,
                        sender: self.sender.clone(),
                        receiver,
                        closed_before: self.closed.before,
                    },
                )
            })
            .collect()
    }

    pub fn is_closed(&self, id: u64) -> bool {
        self.closed.contains(id)
    }

    /// Attempt one enrolled transfer. A timeout, rejected request, or failed
    /// receipt fsync leaves the exact packet available for the next retry.
    pub async fn deliver(
        &mut self,
        id: u64,
        client: &OperationTransportClient,
    ) -> Result<(), OutboxError> {
        let entry = self.entries.get(&id).ok_or(OutboxError::Unknown(id))?;
        if entry.closed || entry.receipt.is_some() {
            return Ok(());
        }
        let peer_addr = entry.peer_addr.clone();
        let transfer = entry_transfer(entry);
        let receipt = client.send_transfer(&peer_addr, &transfer).await?;
        self.record_receipt(id, receipt)
    }

    /// Retry all obligations known at this instant; failed sends stay queued.
    pub async fn retry_pending(
        &mut self,
        client: &OperationTransportClient,
    ) -> Vec<(u64, OutboxError)> {
        let ids: Vec<_> = self.pending().map(|entry| entry.id).collect();
        let mut failures = Vec::new();
        for id in ids {
            if let Err(error) = self.deliver(id, client).await {
                failures.push((id, error));
            }
        }
        failures
    }
}

#[cfg(test)]
mod routed_mode_tests {
    use super::*;

    #[test]
    fn link_manifest_rejects_legacy_routed_records_and_key_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let outbox = OperationOutbox::open_routed_for_node(dir.path(), NodeId("a".into())).unwrap();
        outbox.verify_link_manifest([1; 32]).unwrap();
        assert!(!dir.path().join(".link-manifest").exists());
        outbox.ensure_link_manifest([1; 32]).unwrap();
        outbox.verify_link_manifest([1; 32]).unwrap();
        assert!(outbox.verify_link_manifest([2; 32]).is_err());
        assert!(outbox.ensure_link_manifest([2; 32]).is_err());
        drop(outbox);
        let outbox = OperationOutbox::open_routed_for_node(dir.path(), NodeId("a".into())).unwrap();
        outbox.ensure_link_manifest([1; 32]).unwrap();

        let old = tempfile::tempdir().unwrap();
        let outbox = OperationOutbox::open_routed_for_node(old.path(), NodeId("a".into())).unwrap();
        std::fs::write(old.path().join("0.send"), b"unsigned legacy descriptor").unwrap();
        assert!(outbox.verify_link_manifest([1; 32]).is_err());
        assert!(!old.path().join(".link-manifest").exists());
        assert!(outbox.ensure_link_manifest([1; 32]).is_err());
    }

    #[test]
    fn direct_outbox_link_pin_rejects_unsigned_records_and_key_change() {
        let dir = tempfile::tempdir().unwrap();
        let outbox = OperationOutbox::open_for_node(dir.path(), NodeId("a".into())).unwrap();
        assert!(!outbox.is_routed());
        outbox.ensure_link_manifest([1; 32]).unwrap();
        drop(outbox);
        let outbox = OperationOutbox::open_for_node(dir.path(), NodeId("a".into())).unwrap();
        outbox.verify_link_manifest([1; 32]).unwrap();
        assert!(outbox.verify_link_manifest([2; 32]).is_err());

        let unsigned = tempfile::tempdir().unwrap();
        let outbox = OperationOutbox::open_for_node(unsigned.path(), NodeId("a".into())).unwrap();
        std::fs::write(unsigned.path().join("0.send"), b"unsigned descriptor").unwrap();
        assert!(outbox.ensure_link_manifest([1; 32]).is_err());
        assert!(!unsigned.path().join(".link-manifest").exists());
    }

    #[test]
    fn empty_outbox_cannot_change_custody_mode_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        OperationOutbox::open_routed_for_node(dir.path(), NodeId("a".into())).unwrap();
        assert!(OperationOutbox::open_for_node(dir.path(), NodeId("a".into())).is_err());
    }

    #[test]
    fn routed_manifest_cannot_change_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let outbox = OperationOutbox::open_routed_for_node(dir.path(), NodeId("a".into())).unwrap();
        outbox.verify_routed_manifest([1; 32]).unwrap();
        assert!(!dir.path().join(".routing").exists());
        outbox.ensure_routed_manifest([1; 32]).unwrap();
        outbox.verify_routed_manifest([1; 32]).unwrap();
        assert!(outbox.verify_routed_manifest([2; 32]).is_err());
        drop(outbox);
        let outbox = OperationOutbox::open_routed_for_node(dir.path(), NodeId("a".into())).unwrap();
        outbox.ensure_routed_manifest([1; 32]).unwrap();
        assert!(outbox.ensure_routed_manifest([2; 32]).is_err());
    }
}

/// Start retrying persisted sends immediately after recovery, then once per
/// second. One packet is attempted per tick in rotating ID order so a dead
/// neighbor cannot starve other obligations. All network I/O runs without
/// holding the outbox lock. The returned task exits when shutdown is sent.
pub fn spawn_operation_retry_worker(
    outbox: Arc<tokio::sync::Mutex<OperationOutbox>>,
    token: String,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    spawn_operation_retry_worker_with_auth(outbox, token, None, shutdown)
}

pub fn spawn_operation_retry_worker_with_auth(
    outbox: Arc<tokio::sync::Mutex<OperationOutbox>>,
    token: String,
    link: Option<OperationLinkAuth>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_retry_loop(
        outbox,
        token,
        link,
        shutdown,
        Duration::from_secs(1),
    ))
}

async fn run_retry_loop(
    outbox: Arc<tokio::sync::Mutex<OperationOutbox>>,
    token: String,
    link: Option<OperationLinkAuth>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    interval: Duration,
) {
    let client = match link {
        Some(link) => OperationTransportClient::authenticated(token, link),
        None => OperationTransportClient::new(token),
    };
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut cursor = crate::network::retry_cursor::RetryCursor::default();
    let mut retirement_cursor = crate::network::retry_cursor::RetryCursor::default();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            _ = ticker.tick() => {
                if *shutdown.borrow() { break; }
                if let Err(error) = outbox.lock().await.recover_if_poisoned() {
                    tracing::warn!(error = %error,
                        "operation sender outbox recovery failed; retrying next tick");
                    continue;
                }
                let compact_result = outbox.lock().await.compact_one();
                if let Err(error) = compact_result {
                    tracing::warn!(error = %error,
                        "operation sender detail compaction failed; recovering before retry");
                    // Compaction can fail after publishing its closure journal.
                    // Re-read that durable state before using any pending send;
                    // a different ready transfer must not wait for compaction
                    // of this one to succeed on every tick.
                    if let Err(error) = outbox.lock().await.recover_if_poisoned() {
                        tracing::warn!(error = %error,
                            "operation sender outbox recovery failed; retrying next tick");
                        continue;
                    }
                }
                let notices = outbox.lock().await.retirement_notices();
                let selected = retirement_cursor.next(notices.iter().map(|(_, notice)| notice.receiver.0.clone()));
                if let Some((addr, notice)) = selected.and_then(|receiver| notices.iter()
                    .find(|(_, notice)| notice.receiver.0 == receiver))
                    && let Err(error) = client.send_retirement(addr, notice).await
                {
                    tracing::debug!(error = %error, "retirement fence retry remains pending");
                }
                let entry = {
                    let guard = outbox.lock().await;
                    cursor.next(guard.pending().map(|entry| entry.id))
                        .and_then(|id| guard.pending().find(|entry| entry.id == id).cloned())
                };
                let Some(entry) = entry else { continue; };
                match client.send_transfer(&entry.peer_addr, &entry_transfer(&entry)).await {
                    Ok(receipt) => {
                        // The fsync inside record_receipt completes before
                        // this obligation leaves the pending set.
                        if let Err(error) = outbox.lock().await.record_receipt(entry.id, receipt) {
                            tracing::warn!(transfer_id = entry.id, error = %error,
                                "operation receipt persistence failed; retaining send obligation");
                        }
                    }
                    Err(error) => {
                        tracing::debug!(transfer_id = entry.id, error = %error,
                            "operation send failed; retaining send obligation");
                    }
                }
            }
        }
    }
}

fn matches_entry(entry: &OutboxEntry, receipt: &OperationTransferReceipt) -> bool {
    receipt.version == if entry.destination.is_some() { 2 } else { 1 }
        && receipt.commitment == entry.commitment
        && receipt.sender == entry.sender
        && receipt.receiver == entry.receiver
        && receipt.transfer_id == entry.id
}

fn matches_transfer(entry: &OutboxEntry, transfer: &OperationTransfer) -> bool {
    transfer.version == if entry.destination.is_some() { 2 } else { 1 }
        && transfer.sender == entry.sender
        && transfer.transfer_id == entry.id
        && transfer.receiver == entry.receiver
        && transfer.destination == entry.destination
        && transfer.parent == entry.parent
        && transfer.depth == entry.depth
        && transfer.packet.signed.bound.operation.subject == entry.subject
        && transfer.packet.commitment() == entry.commitment
}

fn entry_transfer(entry: &OutboxEntry) -> OperationTransfer {
    OperationTransfer {
        version: if entry.destination.is_some() { 2 } else { 1 },
        sender: entry.sender.clone(),
        transfer_id: entry.id,
        receiver: entry.receiver.clone(),
        destination: entry.destination.clone(),
        parent: entry.parent.clone(),
        depth: entry.depth,
        packet: entry.packet.clone().expect("pending send has a packet"),
    }
}

fn compact_record(entry: &OutboxEntry) -> CompactSendRecord {
    CompactSendRecord {
        version: 4,
        id: entry.id,
        peer_addr: entry.peer_addr.clone(),
        sender: entry.sender.clone(),
        receiver: entry.receiver.clone(),
        destination: entry
            .destination
            .clone()
            .expect("routed send has destination"),
        parent: entry.parent.clone(),
        depth: entry.depth,
        subject: entry.subject.clone(),
        commitment: entry.commitment,
    }
}

fn compact_record_for_mode(
    entry: &OutboxEntry,
    routed: bool,
) -> Result<Vec<u8>, serde_json::Error> {
    if routed {
        serde_json::to_vec(&compact_record(entry))
    } else {
        serde_json::to_vec(&CompactNeighborSendRecord {
            version: 5,
            id: entry.id,
            peer_addr: entry.peer_addr.clone(),
            sender: entry.sender.clone(),
            receiver: entry.receiver.clone(),
            subject: entry.subject.clone(),
            commitment: entry.commitment,
        })
    }
}

fn send_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id}.send"))
}
fn ack_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id}.ack"))
}
fn done_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id}.done"))
}
fn closed_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id}.closed"))
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Write a file, sync its bytes, rename into place, and sync the directory.
/// A prior attempt can have renamed successfully and then failed its final
/// directory fsync. If its exact bytes are present, retry that fsync; a
/// different existing record is a conflict. One writer owns this directory.
fn atomic_create(dir: &Path, final_path: &Path, bytes: &[u8]) -> io::Result<()> {
    if final_path.exists() {
        if fs::read(final_path)? != bytes {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "outbox record exists",
            ));
        }
        File::open(final_path)?.sync_all()?;
        return fsync_dir(dir);
    }
    let (temp_path, mut file) = loop {
        let seq = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!(".tmp-{}-{seq}", std::process::id()));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => break (path, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_path, final_path)?;
        fsync_dir(dir)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp_path);
    }
    result
}

/// Replace a monotone metadata snapshot using a same-directory rename.
fn atomic_replace(dir: &Path, final_path: &Path, bytes: &[u8]) -> io::Result<()> {
    let (temp_path, mut file) = loop {
        let seq = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!(".tmp-{}-{seq}", std::process::id()));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => break (path, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_path, final_path)?;
        fsync_dir(dir)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp_path);
    }
    result
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::authority::writer_auth::{SignedBoundOperation, SignedCreation};
    use crate::hlc::HlcTimestamp;
    use crate::network::operation_transport::{
        OperationAdmission, OperationInbox, receiver_router_for_node,
    };
    use crate::operation::{
        BoundOperation, CandidateKvAction, CertificationMode, CrdtSubject, Declaration, EventDot,
        OperationLeaf,
    };
    use crate::types::NodeId;
    use ed25519_dalek::SigningKey;

    struct Allow;
    impl OperationAdmission for Allow {
        fn admit(&self, _signed: &SignedBoundOperation) -> Result<(), String> {
            Ok(())
        }
    }

    fn packet() -> OperationPacket {
        packet_with_value(b"exact body")
    }

    fn packet_with_value(value: &[u8]) -> OperationPacket {
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 1,
            },
        };
        let bound = BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: "key".into(),
                    value_bytes: value.to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 1,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject,
                key: "key".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let key = SigningKey::from_bytes(&[7; 32]);
        let creation = SignedCreation::sign(&bound, &key).unwrap();
        OperationPacket {
            signed: SignedBoundOperation::sign(bound, &key, creation).unwrap(),
        }
    }

    fn ack(
        outbox: &OperationOutbox,
        id: u64,
        commitment: [u8; 32],
        already_present: bool,
    ) -> OperationTransferReceipt {
        let entry = outbox.get(id).unwrap();
        OperationTransferReceipt {
            version: 1,
            sender: entry.sender.clone(),
            receiver: entry.receiver.clone(),
            transfer_id: id,
            commitment,
            already_present,
        }
    }

    #[test]
    fn neighbor_ack_compacts_sender_body_and_preserves_identity_after_restart() {
        let root = tempfile::tempdir().unwrap();
        let original = packet();
        let mut outbox = OperationOutbox::open(root.path()).unwrap();
        let id = outbox
            .ensure_enqueued("receiver:1234".into(), original.clone())
            .unwrap();
        assert_eq!(outbox.compact_one().unwrap(), None);
        assert!(outbox.get(id).unwrap().packet.is_some());
        outbox
            .record_receipt(id, ack(&outbox, id, original.commitment(), false))
            .unwrap();
        assert_eq!(outbox.compact_one().unwrap(), Some(id));
        assert!(outbox.get(id).unwrap().packet.is_none());
        assert!(!ack_path(root.path(), id).exists());
        assert!(outbox.retirement_notices().is_empty());
        let descriptor: serde_json::Value =
            serde_json::from_slice(&fs::read(send_path(root.path(), id)).unwrap()).unwrap();
        assert_eq!(descriptor["version"], 5);
        assert!(descriptor.get("packet").is_none());
        drop(outbox);

        let mut reopened = OperationOutbox::open(root.path()).unwrap();
        assert!(reopened.is_closed(id));
        assert_eq!(reopened.pending().count(), 0);
        assert_eq!(
            reopened
                .ensure_enqueued("receiver:1234".into(), original)
                .unwrap(),
            id
        );
        assert!(matches!(
            reopened.ensure_enqueued("receiver:1234".into(), packet_with_value(b"different")),
            Err(OutboxError::ConflictingOperation { transfer_id, .. }) if transfer_id == id
        ));
        drop(reopened);
        fs::remove_file(root.path().join(".index")).unwrap();
        assert!(OperationOutbox::open(root.path()).is_err());
    }

    #[test]
    fn neighbor_closure_journal_and_closed_index_resume_interrupted_compaction() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox = OperationOutbox::open(root.path()).unwrap();
        let id = outbox.enqueue("receiver:1234".into(), packet()).unwrap();
        outbox
            .record_receipt(id, ack(&outbox, id, packet().commitment(), false))
            .unwrap();
        let journal = ClosureJournal {
            version: 1,
            id,
            commitment: packet().commitment(),
        };
        atomic_create(
            root.path(),
            &closed_path(root.path(), id),
            &serde_json::to_vec(&journal).unwrap(),
        )
        .unwrap();
        drop(outbox); // crash before index snapshot
        let recovered = OperationOutbox::open(root.path()).unwrap();
        assert!(recovered.is_closed(id));
        assert!(recovered.get(id).unwrap().packet.is_none());
        assert!(!ack_path(root.path(), id).exists());
        assert!(!closed_path(root.path(), id).exists());
        drop(recovered);

        let mut outbox = OperationOutbox::open(root.path()).unwrap();
        let second = outbox
            .enqueue("other:1234".into(), packet_with_value(b"other"))
            .unwrap();
        outbox
            .record_receipt(
                second,
                ack(
                    &outbox,
                    second,
                    packet_with_value(b"other").commitment(),
                    false,
                ),
            )
            .unwrap();
        drop(outbox);
        let mut saved: ClosedIndex =
            serde_json::from_slice(&fs::read(root.path().join(".index")).unwrap()).unwrap();
        saved.insert(second).unwrap();
        atomic_replace(
            root.path(),
            &root.path().join(".index"),
            &serde_json::to_vec(&saved).unwrap(),
        )
        .unwrap();
        let recovered = OperationOutbox::open(root.path()).unwrap();
        assert!(recovered.is_closed(second));
        assert!(recovered.get(second).unwrap().packet.is_none());
        assert!(!ack_path(root.path(), second).exists());
    }

    fn routed_report(outbox: &OperationOutbox, id: u64) -> OperationCompletionReport {
        OperationCompletionReport {
            version: 1,
            transfer: entry_transfer(outbox.get(id).unwrap()),
        }
    }

    fn routed_ack(outbox: &OperationOutbox, id: u64) -> OperationTransferReceipt {
        let entry = outbox.get(id).unwrap();
        OperationTransferReceipt {
            version: 2,
            sender: entry.sender.clone(),
            transfer_id: id,
            receiver: entry.receiver.clone(),
            commitment: entry.commitment,
            already_present: false,
        }
    }

    fn routed_send(outbox: &mut OperationOutbox, destination: &str) -> u64 {
        outbox
            .ensure_routed(
                NodeId("b".into()),
                "b:1234".into(),
                packet(),
                NodeId(destination.into()),
                None,
                0,
            )
            .unwrap()
    }

    #[test]
    fn pristine_routed_manifest_repair_is_durable_and_never_rebinds_a_send() {
        let dir = tempfile::tempdir().unwrap();
        let mut outbox =
            OperationOutbox::open_routed_for_node(dir.path(), NodeId("a".into())).unwrap();
        outbox.ensure_routed_manifest([1; 32]).unwrap();
        outbox.replace_routed_manifest_if_pristine([2; 32]).unwrap();
        outbox.verify_routed_manifest([2; 32]).unwrap();
        assert!(outbox.verify_routed_manifest([1; 32]).is_err());
        drop(outbox);

        let mut reopened =
            OperationOutbox::open_routed_for_node(dir.path(), NodeId("a".into())).unwrap();
        reopened.verify_routed_manifest([2; 32]).unwrap();
        routed_send(&mut reopened, "c");
        assert!(
            reopened
                .replace_routed_manifest_if_pristine([3; 32])
                .is_err()
        );
        reopened.verify_routed_manifest([2; 32]).unwrap();
        drop(reopened);

        let reopened =
            OperationOutbox::open_routed_for_node(dir.path(), NodeId("a".into())).unwrap();
        reopened.verify_routed_manifest([2; 32]).unwrap();
        assert!(reopened.verify_routed_manifest([3; 32]).is_err());
    }

    #[test]
    fn interrupted_routing_pin_replacement_keeps_old_pin_after_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let outbox = OperationOutbox::open_routed_for_node(dir.path(), NodeId("a".into())).unwrap();
        outbox.ensure_routed_manifest([1; 32]).unwrap();
        // A crash before rename leaves only the temporary replacement. The
        // outbox recovery removes it and retains the committed old route.
        let temp = dir.path().join(".tmp-interrupted-routing-repair");
        fs::write(&temp, hex::encode([2; 32])).unwrap();
        drop(outbox);

        let mut recovered =
            OperationOutbox::open_routed_for_node(dir.path(), NodeId("a".into())).unwrap();
        assert!(!temp.exists());
        recovered.verify_routed_manifest([1; 32]).unwrap();
        recovered
            .replace_routed_manifest_if_pristine([2; 32])
            .unwrap();
        recovered.verify_routed_manifest([2; 32]).unwrap();
    }

    #[test]
    fn malformed_routing_pin_cannot_be_repaired_as_a_configuration_change() {
        let dir = tempfile::tempdir().unwrap();
        let mut outbox =
            OperationOutbox::open_routed_for_node(dir.path(), NodeId("a".into())).unwrap();
        fs::write(dir.path().join(".routing"), b"incomplete").unwrap();
        assert!(outbox.replace_routed_manifest_if_pristine([2; 32]).is_err());
        assert_eq!(
            fs::read(dir.path().join(".routing")).unwrap(),
            b"incomplete"
        );
    }

    #[test]
    fn sender_compaction_needs_both_ack_and_completion_and_preserves_holes() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        let zero = routed_send(&mut outbox, "c");
        let one = routed_send(&mut outbox, "d");
        let two = routed_send(&mut outbox, "e");
        assert_eq!((zero, one, two), (0, 1, 2));
        outbox
            .record_receipt(zero, routed_ack(&outbox, zero))
            .unwrap();
        outbox
            .record_completion(routed_report(&outbox, one))
            .unwrap();
        assert_eq!(outbox.compact_one().unwrap(), None);
        assert_eq!(
            outbox.pending().map(|entry| entry.id).collect::<Vec<_>>(),
            vec![1, 2]
        );
        outbox
            .record_receipt(one, routed_ack(&outbox, one))
            .unwrap();
        assert_eq!(outbox.compact_one().unwrap(), Some(one));
        assert!(outbox.get(one).unwrap().packet.is_none());
        assert!(
            serde_json::from_slice::<serde_json::Value>(
                &fs::read(send_path(root.path(), one)).unwrap()
            )
            .unwrap()
            .get("packet")
            .is_none()
        );
        assert_eq!(outbox.closed_before(), 0);
        assert!(outbox.retirement_notices().is_empty());
        assert!(outbox.is_closed(one));
        assert!(!ack_path(root.path(), one).exists());
        assert!(!done_path(root.path(), one).exists());
        assert!(send_path(root.path(), one).exists());
        assert_eq!(
            outbox.pending().map(|entry| entry.id).collect::<Vec<_>>(),
            vec![2]
        );
        outbox
            .record_completion(routed_report(&outbox, zero))
            .unwrap();
        assert_eq!(outbox.compact_one().unwrap(), Some(zero));
        assert_eq!(outbox.closed_before(), 2);
        assert!(!outbox.is_closed(two));
        drop(outbox);
        let mut recovered =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        assert_eq!(recovered.closed_before(), 2);
        assert_eq!(
            recovered.retirement_notices(),
            vec![(
                "b:1234".to_owned(),
                OperationRetirementNotice {
                    version: 1,
                    sender: NodeId("a".into()),
                    receiver: NodeId("b".into()),
                    closed_before: 2,
                },
            )]
        );
        assert!(!recovered.completed_child(
            &TransferId {
                sender: NodeId("x".into()),
                sequence: 4
            },
            &NodeId("d".into()),
            &packet()
        ));
        assert_eq!(
            recovered
                .pending()
                .map(|entry| entry.id)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(routed_send(&mut recovered, "f"), 3);
    }

    #[test]
    fn closure_journal_recovers_before_index_and_after_partial_detail_deletion() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        let id = routed_send(&mut outbox, "c");
        outbox.record_receipt(id, routed_ack(&outbox, id)).unwrap();
        outbox
            .record_completion(routed_report(&outbox, id))
            .unwrap();
        let journal = ClosureJournal {
            version: 1,
            id,
            commitment: packet().commitment(),
        };
        atomic_create(
            root.path(),
            &closed_path(root.path(), id),
            &serde_json::to_vec(&journal).unwrap(),
        )
        .unwrap();
        drop(outbox); // crash before index snapshot
        let recovered =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        assert!(recovered.is_closed(id));
        assert_eq!(recovered.closed_before(), 1);
        assert!(!ack_path(root.path(), id).exists());
        assert!(!done_path(root.path(), id).exists());
        assert!(!closed_path(root.path(), id).exists());
        drop(recovered);
        let mut outbox =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        let second = routed_send(&mut outbox, "d");
        outbox
            .record_receipt(second, routed_ack(&outbox, second))
            .unwrap();
        outbox
            .record_completion(routed_report(&outbox, second))
            .unwrap();
        drop(outbox);
        // Simulate a snapshot committed, then a crash after removing only ACK.
        let mut saved: ClosedIndex =
            serde_json::from_slice(&fs::read(root.path().join(".index")).unwrap()).unwrap();
        saved.insert(second).unwrap();
        atomic_replace(
            root.path(),
            &root.path().join(".index"),
            &serde_json::to_vec(&saved).unwrap(),
        )
        .unwrap();
        fs::remove_file(ack_path(root.path(), second)).unwrap();
        fsync_dir(root.path()).unwrap();
        let recovered =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        assert!(recovered.is_closed(second));
        assert!(recovered.get(second).unwrap().packet.is_none());
        assert!(!done_path(root.path(), second).exists());
    }

    #[test]
    fn compact_send_requires_durable_closed_index_and_rejects_rebinding() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        let id = routed_send(&mut outbox, "c");
        let original = packet();
        outbox.record_receipt(id, routed_ack(&outbox, id)).unwrap();
        outbox
            .record_completion(routed_report(&outbox, id))
            .unwrap();
        assert_eq!(outbox.compact_one().unwrap(), Some(id));
        assert!(outbox.get(id).unwrap().packet.is_none());
        assert_eq!(
            outbox
                .ensure_routed(
                    NodeId("b".into()),
                    "b:1234".into(),
                    original.clone(),
                    NodeId("c".into()),
                    None,
                    0,
                )
                .unwrap(),
            id
        );
        assert!(matches!(
            outbox.ensure_routed(
                NodeId("b".into()),
                "b:1234".into(),
                packet_with_value(b"rebound"),
                NodeId("c".into()),
                None,
                0,
            ),
            Err(OutboxError::ConflictingOperation { transfer_id, .. }) if transfer_id == id
        ));
        drop(outbox);
        fs::remove_file(root.path().join(".index")).unwrap();
        assert!(OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).is_err());
    }

    #[test]
    fn recovery_cleans_details_left_after_compact_send_rewrite() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        let id = routed_send(&mut outbox, "c");
        let ack = routed_ack(&outbox, id);
        let done = routed_report(&outbox, id);
        outbox.record_receipt(id, ack.clone()).unwrap();
        outbox.record_completion(done.clone()).unwrap();
        outbox.compact_one().unwrap();
        drop(outbox);
        // The index and compact descriptor are durable, but deletion of the
        // old details was interrupted before their directory fsync.
        atomic_create(
            root.path(),
            &ack_path(root.path(), id),
            &serde_json::to_vec(&ack).unwrap(),
        )
        .unwrap();
        atomic_create(
            root.path(),
            &done_path(root.path(), id),
            &serde_json::to_vec(&done).unwrap(),
        )
        .unwrap();
        let recovered =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        assert!(recovered.is_closed(id));
        assert!(recovered.get(id).unwrap().packet.is_none());
        assert!(!ack_path(root.path(), id).exists());
        assert!(!done_path(root.path(), id).exists());
    }

    #[tokio::test]
    async fn direct_delivery_of_closed_id_is_a_noop_after_restart() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        let id = routed_send(&mut outbox, "c");
        outbox.record_receipt(id, routed_ack(&outbox, id)).unwrap();
        outbox
            .record_completion(routed_report(&outbox, id))
            .unwrap();
        outbox.compact_one().unwrap();
        drop(outbox);
        let mut outbox =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        assert!(outbox.get(id).unwrap().packet.is_none());
        outbox
            .deliver(id, &OperationTransportClient::new("unused".into()))
            .await
            .unwrap();
        assert_eq!(outbox.pending().count(), 0);
    }

    #[test]
    fn high_water_repairs_crash_gap_but_rejects_forward_jump_or_missing_descriptor() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        assert_eq!(routed_send(&mut outbox, "c"), 0);
        drop(outbox);
        atomic_replace(root.path(), &root.path().join(".next-id"), b"0").unwrap();
        let mut recovered =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        assert_eq!(routed_send(&mut recovered, "d"), 1);
        drop(recovered);
        atomic_replace(root.path(), &root.path().join(".next-id"), b"9").unwrap();
        assert!(OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).is_err());
        atomic_replace(root.path(), &root.path().join(".next-id"), b"2").unwrap();
        fs::remove_file(send_path(root.path(), 0)).unwrap();
        assert!(OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).is_err());
    }

    #[test]
    fn archived_child_still_proves_parent_completion_after_restart() {
        let root = tempfile::tempdir().unwrap();
        let parent = TransferId {
            sender: NodeId("upstream".into()),
            sequence: 8,
        };
        let body = packet();
        let mut outbox =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        let id = outbox
            .ensure_routed(
                NodeId("b".into()),
                "b:1234".into(),
                body.clone(),
                NodeId("c".into()),
                Some(parent.clone()),
                1,
            )
            .unwrap();
        outbox.record_receipt(id, routed_ack(&outbox, id)).unwrap();
        outbox
            .record_completion(routed_report(&outbox, id))
            .unwrap();
        assert_eq!(outbox.compact_one().unwrap(), Some(id));
        drop(outbox);
        let mut outbox =
            OperationOutbox::open_routed_for_node(root.path(), NodeId("a".into())).unwrap();
        assert!(outbox.completed_child(&parent, &NodeId("c".into()), &body));
        assert!(!outbox.completed_child(&parent, &NodeId("other".into()), &body));
        assert_eq!(outbox.get(id).unwrap().packet, None);
        assert_eq!(outbox.get(id).unwrap().commitment, body.commitment());
        assert_eq!(outbox.get(id).unwrap().parent, Some(parent.clone()));
        outbox.record_receipt(id, routed_ack(&outbox, id)).unwrap();
        outbox
            .record_completion(OperationCompletionReport {
                version: 1,
                transfer: OperationTransfer {
                    version: 2,
                    sender: NodeId("a".into()),
                    transfer_id: id,
                    receiver: NodeId("b".into()),
                    destination: Some(NodeId("c".into())),
                    parent: Some(parent.clone()),
                    depth: 1,
                    packet: body.clone(),
                },
            })
            .unwrap();
        assert!(!ack_path(root.path(), id).exists());
        assert!(!done_path(root.path(), id).exists());
    }

    #[test]
    fn lost_reply_is_retried_after_sender_restart() {
        let root = tempfile::tempdir().unwrap();
        let out_dir = root.path().join("outbox");
        let in_dir = root.path().join("inbox");
        let sent = packet();
        let id = {
            let mut outbox = OperationOutbox::open(&out_dir).unwrap();
            outbox
                .enqueue("receiver:1234".into(), sent.clone())
                .unwrap()
        };
        // The receiver saves and replies, but the sender crashes before it
        // records that reply. The duplicate receiver write is idempotent.
        {
            let mut inbox = OperationInbox::open(&in_dir, Arc::new(Allow)).unwrap();
            assert!(!inbox.receive(sent.clone()).unwrap().already_present);
        }
        let mut outbox = OperationOutbox::open(&out_dir).unwrap();
        let retry = outbox.pending().next().unwrap();
        assert_eq!(retry.id, id);
        assert_eq!(
            serde_json::to_vec(&retry.packet).unwrap(),
            serde_json::to_vec(&sent).unwrap()
        );
        let mut inbox = OperationInbox::open(&in_dir, Arc::new(Allow)).unwrap();
        let receipt = inbox
            .receive_transfer(entry_transfer(retry), &retry.receiver)
            .unwrap();
        assert!(receipt.already_present);
        outbox.record_receipt(id, receipt.clone()).unwrap();
        assert_eq!(outbox.pending().count(), 0);
        drop(outbox);
        let outbox = OperationOutbox::open(&out_dir).unwrap();
        assert_eq!(outbox.pending().count(), 0);
        assert_eq!(outbox.get(id).unwrap().receipt, Some(receipt));
        assert_eq!(outbox.get(id).unwrap().commitment, sent.commitment());
    }

    #[test]
    fn mismatched_receipt_never_finishes_send() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox = OperationOutbox::open(root.path()).unwrap();
        let id = outbox.enqueue("receiver:1234".into(), packet()).unwrap();
        assert!(matches!(
            outbox.record_receipt(id, ack(&outbox, id, [0; 32], false)),
            Err(OutboxError::WrongReceipt(_))
        ));
        assert_eq!(outbox.pending().count(), 1);
        drop(outbox);
        assert_eq!(
            OperationOutbox::open(root.path())
                .unwrap()
                .pending()
                .count(),
            1
        );
    }

    #[test]
    fn receipt_for_other_receiver_or_transfer_cannot_stop_retry() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox = OperationOutbox::open(root.path()).unwrap();
        let packet = packet();
        let b = outbox
            .ensure_enqueued_to(NodeId("b".into()), "b:1234".into(), packet.clone())
            .unwrap();
        let c = outbox
            .ensure_enqueued_to(NodeId("c".into()), "c:1234".into(), packet.clone())
            .unwrap();
        let b_ack = ack(&outbox, b, packet.commitment(), false);
        assert!(matches!(
            outbox.record_receipt(c, b_ack.clone()),
            Err(OutboxError::WrongReceipt(_))
        ));
        let mut wrong_id = ack(&outbox, c, packet.commitment(), false);
        wrong_id.transfer_id = b;
        assert!(matches!(
            outbox.record_receipt(c, wrong_id),
            Err(OutboxError::WrongReceipt(_))
        ));
        assert_eq!(outbox.pending().count(), 2);
        outbox.record_receipt(b, b_ack).unwrap();
        assert_eq!(
            outbox
                .pending()
                .map(|entry| entry.receiver.clone())
                .collect::<Vec<_>>(),
            vec![NodeId("c".into())]
        );
        drop(outbox);
        let reopened = OperationOutbox::open(root.path()).unwrap();
        assert_eq!(reopened.pending().count(), 1);
    }

    #[test]
    fn uncertain_ack_directory_sync_can_be_retried() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox = OperationOutbox::open(root.path()).unwrap();
        let packet = packet();
        let id = outbox
            .enqueue("receiver:1234".into(), packet.clone())
            .unwrap();
        let first_reply = ack(&outbox, id, packet.commitment(), false);
        // Model a rename that reached disk before directory fsync returned
        // an error. The next receiver reply may say this is a duplicate.
        fs::write(
            ack_path(root.path(), id),
            serde_json::to_vec(&first_reply).unwrap(),
        )
        .unwrap();
        outbox
            .record_receipt(id, ack(&outbox, id, packet.commitment(), true))
            .unwrap();
        assert_eq!(outbox.get(id).unwrap().receipt, Some(first_reply));
        assert_eq!(outbox.pending().count(), 0);
    }

    #[test]
    fn ensure_enqueued_reuses_pending_and_acknowledged_transfers_after_reopen() {
        let root = tempfile::tempdir().unwrap();
        let original = packet();
        let id = {
            let mut outbox = OperationOutbox::open(root.path()).unwrap();
            let id = outbox
                .ensure_enqueued("receiver:1234".into(), original.clone())
                .unwrap();
            assert_eq!(
                outbox
                    .ensure_enqueued("receiver:1234".into(), original.clone())
                    .unwrap(),
                id
            );
            assert_eq!(outbox.pending().count(), 1);
            id
        };
        let mut outbox = OperationOutbox::open(root.path()).unwrap();
        assert_eq!(
            outbox
                .ensure_enqueued("receiver:1234".into(), original.clone())
                .unwrap(),
            id
        );
        outbox
            .record_receipt(id, ack(&outbox, id, original.commitment(), false))
            .unwrap();
        drop(outbox);
        let mut outbox = OperationOutbox::open(root.path()).unwrap();
        assert_eq!(
            outbox
                .ensure_enqueued("receiver:1234".into(), original.clone())
                .unwrap(),
            id
        );
        assert_eq!(outbox.pending().count(), 0);
        assert!(matches!(
            outbox.ensure_enqueued("receiver:1234".into(), packet_with_value(b"changed")),
            Err(OutboxError::ConflictingOperation { transfer_id, .. }) if transfer_id == id
        ));
        assert_eq!(outbox.pending().count(), 0);
        // A distinct neighbor is a distinct transport obligation.
        assert_ne!(
            outbox
                .ensure_enqueued("another:1234".into(), original)
                .unwrap(),
            id
        );
    }

    #[test]
    fn uncertain_send_directory_sync_reuses_exact_file() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox = OperationOutbox::open(root.path()).unwrap();
        let original = packet();
        let record = SendRecord {
            version: 2,
            id: 0,
            peer_addr: "receiver:1234".into(),
            sender: NodeId("local-test-node".into()),
            receiver: NodeId("receiver:1234".into()),
            destination: None,
            parent: None,
            depth: 0,
            packet: original.clone(),
        };
        // Model rename success followed by a failed directory fsync, before
        // the in-memory transfer was installed.
        fs::write(
            send_path(root.path(), 0),
            serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        let id = outbox
            .ensure_enqueued("receiver:1234".into(), original)
            .unwrap();
        assert_eq!(id, 0);
        assert_eq!(outbox.pending().count(), 1);
        drop(outbox);
        assert_eq!(
            OperationOutbox::open(root.path())
                .unwrap()
                .pending()
                .count(),
            1
        );
    }

    #[test]
    fn poisoned_handle_recovers_published_send_and_ack_before_retry() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox = OperationOutbox::open(root.path()).unwrap();
        let body = packet();
        let record = SendRecord {
            version: 2,
            id: 0,
            peer_addr: "receiver:1234".into(),
            sender: NodeId("local-test-node".into()),
            receiver: NodeId("receiver:1234".into()),
            destination: None,
            parent: None,
            depth: 0,
            packet: body.clone(),
        };
        // The send rename succeeded, but its caller saw an uncertain fsync.
        fs::write(
            send_path(root.path(), 0),
            serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        outbox.poisoned = true;
        assert_eq!(outbox.pending().count(), 0);
        outbox.recover_if_poisoned().unwrap();
        assert_eq!(outbox.next_id, 1);
        assert_eq!(
            outbox.pending().map(|entry| entry.id).collect::<Vec<_>>(),
            vec![0]
        );
        assert_eq!(
            outbox
                .ensure_enqueued("receiver:1234".into(), body.clone())
                .unwrap(),
            0
        );

        // The ACK rename likewise succeeded before the caller could update
        // memory; recovery must not retransmit or allocate a replacement ID.
        let receipt = ack(&outbox, 0, body.commitment(), false);
        fs::write(
            ack_path(root.path(), 0),
            serde_json::to_vec(&receipt).unwrap(),
        )
        .unwrap();
        outbox.poisoned = true;
        outbox.recover_if_poisoned().unwrap();
        assert_eq!(outbox.pending().count(), 0);
        assert_eq!(outbox.get(0).unwrap().receipt.as_ref(), Some(&receipt));
        assert_eq!(
            outbox
                .ensure_enqueued("receiver:1234".into(), body)
                .unwrap(),
            0
        );
    }

    #[test]
    fn poisoned_handle_rejects_mismatched_ack_and_stays_stopped() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox = OperationOutbox::open(root.path()).unwrap();
        let id = outbox
            .ensure_enqueued("receiver:1234".into(), packet())
            .unwrap();
        let receipt = ack(&outbox, id, [0; 32], false);
        fs::write(
            ack_path(root.path(), id),
            serde_json::to_vec(&receipt).unwrap(),
        )
        .unwrap();
        outbox.poisoned = true;
        assert!(matches!(
            outbox.recover_if_poisoned(),
            Err(OutboxError::WrongReceipt(0))
        ));
        assert_eq!(outbox.pending().count(), 0);
        assert!(
            outbox
                .ensure_enqueued("another:1234".into(), packet())
                .is_err()
        );
    }

    #[test]
    fn outbox_lock_excludes_a_second_writer_through_recovery() {
        let root = tempfile::tempdir().unwrap();
        let mut first = OperationOutbox::open(root.path()).unwrap();
        assert!(matches!(
            OperationOutbox::open(root.path()),
            Err(OutboxError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));
        first.poisoned = true;
        first.recover_if_poisoned().unwrap();
        assert!(matches!(
            OperationOutbox::open(root.path()),
            Err(OutboxError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));
        drop(first);
        OperationOutbox::open(root.path()).unwrap();
    }

    #[test]
    fn receipt_without_body_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        fs::write(ack_path(root.path(), 1), b"{}").unwrap();
        assert!(OperationOutbox::open(root.path()).is_err());
    }

    #[test]
    fn legacy_send_requires_explicit_replay_migration() {
        let root = tempfile::tempdir().unwrap();
        let legacy = serde_json::json!({"version": 1, "id": 0,
            "peer_addr": "receiver:1234", "packet": packet()});
        fs::write(
            send_path(root.path(), 0),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            OperationOutbox::open(root.path()),
            Err(OutboxError::LegacyFormat)
        ));
    }

    #[test]
    fn ordinary_store_checkpoint_pruning_does_not_touch_outbox() {
        use crate::store::wal::{self, SyncPolicy, WalConfig, WalRecord, WalWriter};
        let root = tempfile::tempdir().unwrap();
        let out_dir = root.path().join("outbox");
        let mut outbox = OperationOutbox::open(&out_dir).unwrap();
        let id = outbox.enqueue("receiver:1234".into(), packet()).unwrap();
        let wal_dir = root.path().join("wal").join("certified");
        let mut wal = WalWriter::open(WalConfig::new(&wal_dir, SyncPolicy::Always)).unwrap();
        wal.append(&WalRecord::MergeFailed {
            keys: vec!["key".into()],
        })
        .unwrap();
        let sealed = wal.rotate().unwrap();
        wal::remove_segments_up_to(&wal_dir, sealed).unwrap();
        drop(outbox);
        assert!(OperationOutbox::open(&out_dir).unwrap().get(id).is_some());
    }

    #[tokio::test]
    async fn client_delivery_records_durable_ack_after_receiver_save() {
        let root = tempfile::tempdir().unwrap();
        let in_dir = root.path().join("inbox");
        let out_dir = root.path().join("outbox");
        let inbox = Arc::new(tokio::sync::Mutex::new(
            OperationInbox::open(&in_dir, Arc::new(Allow)).unwrap(),
        ));
        let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = server.local_addr().unwrap().to_string();
        let app = receiver_router_for_node(inbox, "secret".into(), NodeId(address.clone()));
        let task = tokio::spawn(async move { axum::serve(server, app).await.unwrap() });

        let sent = packet();
        let id = {
            let mut outbox = OperationOutbox::open(&out_dir).unwrap();
            outbox.enqueue(address, sent.clone()).unwrap()
        };
        let mut outbox = OperationOutbox::open(&out_dir).unwrap();
        outbox
            .deliver(id, &OperationTransportClient::new("secret".into()))
            .await
            .unwrap();
        assert_eq!(outbox.pending().count(), 0);
        drop(outbox);
        task.abort();
        let _ = task.await;

        let outbox = OperationOutbox::open(&out_dir).unwrap();
        assert_eq!(
            outbox.get(id).unwrap().receipt.as_ref().unwrap().commitment,
            sent.commitment()
        );
        let inbox = OperationInbox::open(&in_dir, Arc::new(Allow)).unwrap();
        assert_eq!(
            inbox.get(&sent.signed.bound.operation.subject),
            Some(sent.signed.bound)
        );
    }

    #[tokio::test]
    async fn retry_worker_replays_after_restart_and_stops_on_durable_receipt() {
        let root = tempfile::tempdir().unwrap();
        let in_dir = root.path().join("inbox");
        let out_dir = root.path().join("outbox");
        let inbox = Arc::new(tokio::sync::Mutex::new(
            OperationInbox::open(&in_dir, Arc::new(Allow)).unwrap(),
        ));
        let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = server.local_addr().unwrap().to_string();
        let app = receiver_router_for_node(inbox, "secret".into(), NodeId(address.clone()));
        let server_task = tokio::spawn(async move { axum::serve(server, app).await.unwrap() });

        let id = {
            let mut outbox = OperationOutbox::open(&out_dir).unwrap();
            outbox.enqueue(address, packet()).unwrap()
        };
        let restored = Arc::new(tokio::sync::Mutex::new(
            OperationOutbox::open(&out_dir).unwrap(),
        ));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let worker = tokio::spawn(run_retry_loop(
            Arc::clone(&restored),
            "secret".into(),
            None,
            shutdown_rx,
            Duration::from_millis(20),
        ));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let guard = restored.lock().await;
                let entry = guard.get(id).unwrap();
                if entry.receipt.is_some() || entry.closed {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        shutdown_tx.send(true).unwrap();
        worker.await.unwrap();
        server_task.abort();
        drop(restored);
        assert_eq!(
            OperationOutbox::open(&out_dir).unwrap().pending().count(),
            0
        );
    }

    #[tokio::test]
    async fn retry_worker_sends_after_recovering_failed_compaction_on_same_tick() {
        let root = tempfile::tempdir().unwrap();
        let in_dir = root.path().join("inbox");
        let out_dir = root.path().join("outbox");
        let inbox = Arc::new(tokio::sync::Mutex::new(
            OperationInbox::open(&in_dir, Arc::new(Allow)).unwrap(),
        ));
        let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = server.local_addr().unwrap().to_string();
        let app = receiver_router_for_node(inbox, "secret".into(), NodeId(address.clone()));
        let server_task = tokio::spawn(async move { axum::serve(server, app).await.unwrap() });

        let mut saved = OperationOutbox::open(&out_dir).unwrap();
        let body = packet();
        let closed_id = saved.enqueue("other:1234".into(), body.clone()).unwrap();
        saved
            .record_receipt(closed_id, ack(&saved, closed_id, body.commitment(), false))
            .unwrap();
        let pending_id = saved.enqueue(address, body).unwrap();
        let journal = ClosureJournal {
            version: 1,
            id: closed_id,
            commitment: saved.get(closed_id).unwrap().commitment,
        };
        // A valid journal with different JSON spacing makes compact_one fail
        // its exact-byte create. Recovery validates the journal, closes the
        // first transfer, and the second can still be sent on this tick.
        fs::write(
            closed_path(&out_dir, closed_id),
            serde_json::to_vec_pretty(&journal).unwrap(),
        )
        .unwrap();
        let outbox = Arc::new(tokio::sync::Mutex::new(saved));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let worker = tokio::spawn(run_retry_loop(
            Arc::clone(&outbox),
            "secret".into(),
            None,
            shutdown_rx,
            Duration::from_secs(3),
        ));
        let first_tick = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if outbox
                    .lock()
                    .await
                    .get(pending_id)
                    .unwrap()
                    .receipt
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let guard = outbox.lock().await;
        assert!(
            first_tick.is_ok(),
            "outbox after first tick: poisoned={}, closed={}, pending={:?}, receipt={:?}",
            guard.poisoned,
            guard.is_closed(closed_id),
            guard.pending().map(|entry| entry.id).collect::<Vec<_>>(),
            guard.get(pending_id).unwrap().receipt
        );
        drop(guard);
        shutdown_tx.send(true).unwrap();
        worker.await.unwrap();
        server_task.abort();
        assert!(outbox.lock().await.is_closed(closed_id));
    }
}
