//! Durable sender obligations for individual Authority Merkle root supports.
//!
//! A support is not a quorum certificate. The receiver must verify its
//! signature, scope and inclusion proof before returning a receipt. This
//! module keeps the exact body and support outside checkpoint-pruned WALs and
//! retries until a matching receipt is itself durable at the sender.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::authority::certificate::KeysetVersion;
use crate::authority::operation_merkle::OperationRootSupport;
use crate::network::operation_link_auth::{
    OperationLinkAuth, RECEIVER as LINK_RECEIVER, REQUEST_DIGEST as LINK_REQUEST_DIGEST,
    SENDER as LINK_SENDER, SIGNATURE as LINK_SIGNATURE,
};
use crate::operation::BoundOperation;
use crate::operation::CrdtSubject;
use crate::store::backend::{create_dir_all_durable, fsync_dir};
use crate::types::NodeId;

pub const SUPPORT_RECEIVE_PATH: &str = "/api/internal/operations/supports/receive";
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const OUTBOX_LOCK: &str = ".lock";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportPacket {
    pub operation: BoundOperation,
    pub support: OperationRootSupport,
}

impl SupportPacket {
    /// Versioned commitment over every operation and support field. The
    /// operation's canonical encoding includes its creation declaration.
    pub fn commitment(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"asteroidb/operation-support-packet/v1");
        put_bytes(&mut digest, &self.operation.canonical_bytes());
        let root = &self.support.signed_root;
        digest.update(root.root);
        digest.update(root.leaf_count.to_be_bytes());
        put_bytes(&mut digest, root.scope.namespace_id.as_bytes());
        put_bytes(&mut digest, root.scope.key_prefix.as_bytes());
        digest.update(root.keyset_version.0.to_be_bytes());
        put_bytes(&mut digest, root.authority_id.0.as_bytes());
        digest.update(root.signature);
        let proof = &self.support.proof;
        digest.update(proof.leaf_index.to_be_bytes());
        digest.update(proof.leaf_count.to_be_bytes());
        digest.update((proof.siblings.len() as u64).to_be_bytes());
        for sibling in &proof.siblings {
            digest.update(sibling);
        }
        digest.finalize().into()
    }
}

fn put_bytes(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportReceipt {
    pub commitment: [u8; 32],
    pub already_present: bool,
    #[serde(default)]
    pub certificate_ready: bool,
}

pub struct SupportTransportClient {
    client: reqwest::Client,
    token: String,
    link: Option<OperationLinkAuth>,
}

#[derive(Debug, thiserror::Error)]
pub enum SupportSendError {
    #[error("support transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("support receiver rejected packet with HTTP {0}")]
    Rejected(reqwest::StatusCode),
    #[error("support receiver acknowledged different packet")]
    WrongCommitment,
    #[error("support peer authentication failed")]
    WrongPeer,
    #[error("support encoding failed: {0}")]
    Encode(#[from] serde_json::Error),
}

impl SupportTransportClient {
    pub fn new(token: String) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("support transport client"),
            token,
            link: None,
        }
    }

    pub fn authenticated(token: String, link: OperationLinkAuth) -> Self {
        let mut client = Self::new(token);
        client.link = Some(link);
        client
    }

    pub async fn send(
        &self,
        peer_addr: &str,
        packet: &SupportPacket,
    ) -> Result<SupportReceipt, SupportSendError> {
        let body = serde_json::to_vec(packet)?;
        let mut request = self
            .client
            .post(format!("http://{peer_addr}{SUPPORT_RECEIVE_PATH}"))
            .bearer_auth(&self.token)
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        let mut expected = None;
        if let Some(link) = &self.link {
            let peer = link
                .peer_for_addr(peer_addr)
                .ok_or(SupportSendError::WrongPeer)?;
            let (digest, signature) = link.sign_request(peer, "POST", SUPPORT_RECEIVE_PATH, &body);
            expected = Some((peer.clone(), digest));
            request = request
                .header(LINK_SENDER, &link.self_id().0)
                .header(LINK_RECEIVER, &peer.0)
                .header(LINK_SIGNATURE, signature);
        }
        let response = request.body(body).send().await?;
        let status = response.status();
        let sender = response
            .headers()
            .get(LINK_SENDER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let receiver = response
            .headers()
            .get(LINK_RECEIVER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let signature = response
            .headers()
            .get(LINK_SIGNATURE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let bound_request = response
            .headers()
            .get(LINK_REQUEST_DIGEST)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let bytes = response.bytes().await?;
        if let (Some(link), Some((peer, digest))) = (&self.link, expected)
            && (sender.as_deref() != Some(peer.0.as_str())
                || receiver.as_deref() != Some(link.self_id().0.as_str())
                || bound_request.as_deref() != Some(hex::encode(digest).as_str())
                || !signature.as_deref().is_some_and(|signature| {
                    link.verify_response(
                        &peer,
                        link.self_id(),
                        "POST",
                        SUPPORT_RECEIVE_PATH,
                        &digest,
                        status.as_u16(),
                        &bytes,
                        signature,
                    )
                }))
        {
            return Err(SupportSendError::WrongPeer);
        }
        if !status.is_success() {
            return Err(SupportSendError::Rejected(status));
        }
        let receipt: SupportReceipt = serde_json::from_slice(&bytes)?;
        if receipt.commitment != packet.commitment() {
            return Err(SupportSendError::WrongCommitment);
        }
        Ok(receipt)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SendRecord {
    version: u32,
    id: u64,
    peer_addr: String,
    packet: SupportPacket,
}

/// An acknowledged obligation needs its identity and exact packet digest for
/// future enrollment checks, but no longer needs the value-bearing packet.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactSendRecord {
    version: u32,
    id: u64,
    peer_addr: String,
    subject: CrdtSubject,
    authority_id: NodeId,
    keyset_version: KeysetVersion,
    operation_commitment: [u8; 32],
    packet_commitment: [u8; 32],
    receipt: SupportReceipt,
}

impl CompactSendRecord {
    fn from_entry(entry: &SupportOutboxEntry) -> Self {
        Self {
            version: 2,
            id: entry.id,
            peer_addr: entry.peer_addr.clone(),
            subject: entry.packet.operation.operation.subject.clone(),
            authority_id: entry.packet.support.signed_root.authority_id.clone(),
            keyset_version: entry.packet.support.signed_root.keyset_version.clone(),
            operation_commitment: entry.packet.operation.commitment(),
            packet_commitment: entry.packet.commitment(),
            receipt: entry
                .receipt
                .clone()
                .expect("only acknowledged sends compact"),
        }
    }

    fn same_identity(&self, peer_addr: &str, packet: &SupportPacket) -> bool {
        self.peer_addr == peer_addr
            && self.subject == packet.operation.operation.subject
            && self.authority_id == packet.support.signed_root.authority_id
            && self.keyset_version == packet.support.signed_root.keyset_version
            && self.packet_commitment == packet.commitment()
    }
}

#[derive(Debug, Clone)]
pub struct SupportOutboxEntry {
    pub id: u64,
    pub peer_addr: String,
    pub packet: SupportPacket,
    pub receipt: Option<SupportReceipt>,
}

#[derive(Debug, thiserror::Error)]
pub enum SupportOutboxError {
    #[error("support outbox I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("support outbox serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("support send failed: {0}")]
    Send(#[from] SupportSendError),
    #[error("unknown support transfer {0}")]
    Unknown(u64),
    #[error("support receipt does not match transfer {0}")]
    WrongReceipt(u64),
    #[error(
        "same Authority support identity has different content for peer {peer_addr} (transfer {transfer_id})"
    )]
    ConflictingSupport { peer_addr: String, transfer_id: u64 },
    #[error("support transfer sequence exhausted")]
    Exhausted,
}

/// A single process owns this directory. It must remain outside normal store
/// checkpoint pruning for the entire transport-obligation lifetime.
pub struct SupportOutbox {
    dir: PathBuf,
    lock: File,
    next_id: u64,
    entries: BTreeMap<u64, SupportOutboxEntry>,
    compact: BTreeMap<u64, CompactSendRecord>,
    poisoned: bool,
}

impl SupportOutbox {
    /// Rebuild after an uncertain replacement write. The caller holds the
    /// outbox mutex, so startup recovery cannot race another local writer.
    /// Keep the poisoned handle if recovery itself fails and retry later.
    pub fn recover_if_poisoned(&mut self) -> Result<(), SupportOutboxError> {
        if self.poisoned {
            let recovered = Self::load(&self.dir, self.lock.try_clone()?)?;
            *self = recovered;
        }
        Ok(())
    }

    pub fn ensure_link_manifest(&self, digest: [u8; 32]) -> Result<(), SupportOutboxError> {
        let path = self.dir.join(".link-manifest");
        if path.exists() {
            if fs::read(&path)? != digest {
                return Err(invalid("support link identities changed").into());
            }
            File::open(&path)?.sync_all()?;
            fsync_dir(&self.dir)?;
            return Ok(());
        }
        if !self.entries.is_empty() || !self.compact.is_empty() {
            return Err(invalid(
                "legacy support outbox requires migration before authenticated delivery",
            )
            .into());
        }
        atomic_create(&self.dir, &path, &digest)?;
        Ok(())
    }

    pub fn open(dir: impl AsRef<Path>) -> Result<Self, SupportOutboxError> {
        let dir = dir.as_ref();
        create_dir_all_durable(dir)?;
        let path = dir.join(OUTBOX_LOCK);
        let fresh = !path.exists();
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        lock.try_lock()
            .map_err(|error| io::Error::new(io::ErrorKind::WouldBlock, error.to_string()))?;
        if fresh {
            lock.sync_all()?;
            fsync_dir(dir)?;
        }
        Self::load(dir, lock)
    }

    fn load(dir: &Path, lock: File) -> Result<Self, SupportOutboxError> {
        let mut entries = BTreeMap::new();
        let mut compact = BTreeMap::new();
        let mut max_id = None::<u64>;
        for item in fs::read_dir(dir)? {
            let item = item?;
            let name = item.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| invalid("non-UTF8 support outbox filename"))?;
            if name.starts_with(".tmp-") {
                fs::remove_file(item.path())?;
                continue;
            }
            if name == ".link-manifest" || name == OUTBOX_LOCK {
                continue;
            }
            let Some(stem) = name.strip_suffix(".send") else {
                if name.ends_with(".ack") {
                    continue;
                }
                return Err(invalid("unknown support outbox file").into());
            };
            let id = parse_id(stem)?;
            let bytes = fs::read(item.path())?;
            let version = serde_json::from_slice::<serde_json::Value>(&bytes)?
                .get("version")
                .and_then(serde_json::Value::as_u64);
            if version == Some(2) {
                let record: CompactSendRecord = serde_json::from_slice(&bytes)?;
                if record.id != id || record.peer_addr.is_empty() {
                    return Err(invalid("invalid compact support send record").into());
                }
                let receipt: SupportReceipt =
                    serde_json::from_slice(&fs::read(ack_path(dir, id))?)?;
                if receipt != record.receipt || receipt.commitment != record.packet_commitment {
                    return Err(SupportOutboxError::WrongReceipt(id));
                }
                max_id = Some(max_id.map_or(id, |max| max.max(id)));
                compact.insert(id, record);
                continue;
            }
            let record: SendRecord = serde_json::from_slice(&bytes)?;
            if record.version != 1 || record.id != id || record.peer_addr.is_empty() {
                return Err(invalid("invalid support send record").into());
            }
            let receipt = if ack_path(dir, id).exists() {
                let receipt: SupportReceipt =
                    serde_json::from_slice(&fs::read(ack_path(dir, id))?)?;
                if receipt.commitment != record.packet.commitment() {
                    return Err(SupportOutboxError::WrongReceipt(id));
                }
                Some(receipt)
            } else {
                None
            };
            max_id = Some(max_id.map_or(id, |max| max.max(id)));
            entries.insert(
                id,
                SupportOutboxEntry {
                    id,
                    peer_addr: record.peer_addr,
                    packet: record.packet,
                    receipt,
                },
            );
        }
        for item in fs::read_dir(dir)? {
            let item = item?;
            let name = item.file_name();
            if let Some(stem) = name.to_string_lossy().strip_suffix(".ack") {
                let id = parse_id(stem)?;
                if !entries.contains_key(&id) && !compact.contains_key(&id) {
                    return Err(invalid("orphan support ACK").into());
                }
            }
        }
        // Reject conflicting persisted obligations, including completed ones.
        for (id, entry) in &entries {
            if entries.iter().any(|(other_id, other)| {
                other_id != id
                    && same_identity(
                        &entry.peer_addr,
                        &entry.packet,
                        &other.peer_addr,
                        &other.packet,
                    )
                    && entry.packet != other.packet
            }) {
                return Err(invalid("conflicting persisted support obligations").into());
            }
            if compact.values().any(|old| {
                old.same_identity(&entry.peer_addr, &entry.packet)
                    && old.operation_commitment != entry.packet.operation.commitment()
            }) {
                return Err(invalid("conflicting persisted support obligations").into());
            }
        }
        for (id, record) in &compact {
            if compact.iter().any(|(other_id, other)| {
                other_id != id
                    && other.peer_addr == record.peer_addr
                    && other.subject == record.subject
                    && other.authority_id == record.authority_id
                    && other.keyset_version == record.keyset_version
                    && other.packet_commitment == record.packet_commitment
                    && other.operation_commitment != record.operation_commitment
            }) {
                return Err(invalid("conflicting persisted support obligations").into());
            }
        }
        fsync_dir(dir)?;
        let next_id = max_id
            .map_or(Some(0), |id| id.checked_add(1))
            .ok_or(SupportOutboxError::Exhausted)?;
        Ok(Self {
            dir: dir.to_owned(),
            lock,
            next_id,
            entries,
            compact,
            poisoned: false,
        })
    }

    /// Idempotently enroll one exact neighbor send. Distinct signed roots or
    /// inclusion proofs have distinct obligations even when one Authority
    /// supports the same operation more than once.
    pub fn ensure_enqueued(
        &mut self,
        peer_addr: String,
        packet: SupportPacket,
    ) -> Result<u64, SupportOutboxError> {
        self.recover_if_poisoned()?;
        if peer_addr.is_empty() {
            return Err(invalid("empty peer address").into());
        }
        for entry in self.entries.values() {
            if same_identity(&peer_addr, &packet, &entry.peer_addr, &entry.packet) {
                return if entry.packet == packet {
                    Ok(entry.id)
                } else {
                    Err(SupportOutboxError::ConflictingSupport {
                        peer_addr,
                        transfer_id: entry.id,
                    })
                };
            }
        }
        for record in self.compact.values() {
            if record.same_identity(&peer_addr, &packet) {
                return if record.operation_commitment == packet.operation.commitment()
                    && record.packet_commitment == packet.commitment()
                {
                    Ok(record.id)
                } else {
                    Err(SupportOutboxError::ConflictingSupport {
                        peer_addr,
                        transfer_id: record.id,
                    })
                };
            }
        }
        let id = self.next_id;
        let next_id = id.checked_add(1).ok_or(SupportOutboxError::Exhausted)?;
        let record = SendRecord {
            version: 1,
            id,
            peer_addr: peer_addr.clone(),
            packet: packet.clone(),
        };
        // A failed directory sync may still have published this ID. Re-read
        // the directory before another enrollment can reuse the sequence.
        self.poisoned = true;
        atomic_create(
            &self.dir,
            &send_path(&self.dir, id),
            &serde_json::to_vec(&record)?,
        )?;
        self.entries.insert(
            id,
            SupportOutboxEntry {
                id,
                peer_addr,
                packet,
                receipt: None,
            },
        );
        self.next_id = next_id;
        self.poisoned = false;
        Ok(id)
    }

    pub fn get(&self, id: u64) -> Option<&SupportOutboxEntry> {
        self.entries.get(&id)
    }
    pub fn pending(&self) -> impl Iterator<Item = &SupportOutboxEntry> {
        self.entries
            .values()
            .filter(|entry| !self.poisoned && entry.receipt.is_none())
    }

    /// Exact, durably acknowledged delivery, including compacted entries.
    pub fn has_durable_receipt(&self, peer_addr: &str, packet: &SupportPacket) -> bool {
        !self.poisoned
            && (self.entries.values().any(|entry| {
                entry.peer_addr == peer_addr && entry.packet == *packet && entry.receipt.is_some()
            }) || self.compact.values().any(|record| {
                record.same_identity(peer_addr, packet)
                    && record.operation_commitment == packet.operation.commitment()
                    && record.packet_commitment == packet.commitment()
            }))
    }

    /// Replace acknowledged sends with value-free identity tombstones. The
    /// separate ACK remains durable and is checked again during recovery.
    pub fn compact_acked_and_sync(&mut self) -> Result<usize, SupportOutboxError> {
        self.recover_if_poisoned()?;
        let ids: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(id, entry)| entry.receipt.as_ref().map(|_| *id))
            .collect();
        for id in &ids {
            let record = CompactSendRecord::from_entry(&self.entries[id]);
            self.poisoned = true;
            replace_file(
                &self.dir,
                &send_path(&self.dir, *id),
                &serde_json::to_vec(&record)?,
            )?;
            self.entries.remove(id);
            self.compact.insert(*id, record);
            self.poisoned = false;
        }
        Ok(ids.len())
    }

    pub fn record_receipt(
        &mut self,
        id: u64,
        receipt: SupportReceipt,
    ) -> Result<(), SupportOutboxError> {
        self.recover_if_poisoned()?;
        if let Some(record) = self.compact.get(&id) {
            return if record.packet_commitment == receipt.commitment {
                Ok(())
            } else {
                Err(SupportOutboxError::WrongReceipt(id))
            };
        }
        let entry = self
            .entries
            .get_mut(&id)
            .ok_or(SupportOutboxError::Unknown(id))?;
        if receipt.commitment != entry.packet.commitment() {
            return Err(SupportOutboxError::WrongReceipt(id));
        }
        if entry.receipt.is_some() {
            return Ok(());
        }
        let path = ack_path(&self.dir, id);
        if path.exists() {
            let stored: SupportReceipt = serde_json::from_slice(&fs::read(&path)?)?;
            if stored.commitment != entry.packet.commitment() {
                return Err(SupportOutboxError::WrongReceipt(id));
            }
            File::open(&path)?.sync_all()?;
            fsync_dir(&self.dir)?;
            entry.receipt = Some(stored);
            return Ok(());
        }
        self.poisoned = true;
        atomic_create(&self.dir, &path, &serde_json::to_vec(&receipt)?)?;
        entry.receipt = Some(receipt);
        self.poisoned = false;
        Ok(())
    }

    pub async fn deliver(
        &mut self,
        id: u64,
        client: &SupportTransportClient,
    ) -> Result<(), SupportOutboxError> {
        self.recover_if_poisoned()?;
        if self.compact.contains_key(&id) {
            return Ok(());
        }
        let entry = self
            .entries
            .get(&id)
            .ok_or(SupportOutboxError::Unknown(id))?;
        if entry.receipt.is_some() {
            return Ok(());
        }
        let addr = entry.peer_addr.clone();
        let packet = entry.packet.clone();
        let receipt = client.send(&addr, &packet).await?;
        self.record_receipt(id, receipt)
    }
}

fn same_identity(a_addr: &str, a: &SupportPacket, b_addr: &str, b: &SupportPacket) -> bool {
    a_addr == b_addr
        && a.operation.operation.subject == b.operation.operation.subject
        && a.support.signed_root.authority_id == b.support.signed_root.authority_id
        && a.support.signed_root.keyset_version == b.support.signed_root.keyset_version
        && a.commitment() == b.commitment()
}

fn parse_id(stem: &str) -> io::Result<u64> {
    let id = stem
        .parse::<u64>()
        .map_err(|_| invalid("invalid support transfer ID"))?;
    if stem != id.to_string() {
        return Err(invalid("noncanonical support transfer ID"));
    }
    Ok(id)
}
fn invalid(reason: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}
fn send_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id}.send"))
}
fn ack_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id}.ack"))
}

fn atomic_create(dir: &Path, final_path: &Path, bytes: &[u8]) -> io::Result<()> {
    if final_path.exists() {
        if fs::read(final_path)? != bytes {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "support record exists",
            ));
        }
        File::open(final_path)?.sync_all()?;
        return fsync_dir(dir);
    }
    let (temp_path, mut file) = loop {
        let path = dir.join(format!(
            ".tmp-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
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

fn replace_file(dir: &Path, final_path: &Path, bytes: &[u8]) -> io::Result<()> {
    let (temp_path, mut file) = loop {
        let path = dir.join(format!(
            ".tmp-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
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

pub fn spawn_support_retry_worker(
    outbox: Arc<tokio::sync::Mutex<SupportOutbox>>,
    token: String,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_retry_loop(
        outbox,
        token,
        None,
        shutdown,
        Duration::from_secs(1),
    ))
}

pub fn spawn_support_retry_worker_with_auth(
    outbox: Arc<tokio::sync::Mutex<SupportOutbox>>,
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
    outbox: Arc<tokio::sync::Mutex<SupportOutbox>>,
    token: String,
    link: Option<OperationLinkAuth>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    interval: Duration,
) {
    let client = match link {
        Some(link) => SupportTransportClient::authenticated(token, link),
        None => SupportTransportClient::new(token),
    };
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut cursor = crate::network::retry_cursor::RetryCursor::default();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            _ = ticker.tick() => {
                if *shutdown.borrow() { break; }
                let entry = {
                    let mut guard = outbox.lock().await;
                    if let Err(error) = guard.recover_if_poisoned() {
                        tracing::warn!(error = %error, "support outbox recovery failed; retaining send obligations");
                        continue;
                    }
                    cursor.next(guard.pending().map(|entry| entry.id))
                        .and_then(|id| guard.pending().find(|entry| entry.id == id).cloned())
                };
                let Some(entry) = entry else { continue; };
                match client.send(&entry.peer_addr, &entry.packet).await {
                    Ok(receipt) => {
                        if let Err(error) = outbox.lock().await.record_receipt(entry.id, receipt) {
                            tracing::warn!(transfer_id = entry.id, error = %error, "support ACK persistence failed; retaining send obligation");
                        }
                    }
                    Err(error) => tracing::debug!(transfer_id = entry.id, error = %error, "support send failed; retaining send obligation"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::certificate::{EpochConfig, KeysetRegistry};
    use crate::authority::operation_admission::{OperationAdmissionPolicy, admit_for_signing};
    use crate::authority::operation_merkle::OperationMerkleTree;
    use crate::operation::OperationLedger;
    use ed25519_dalek::SigningKey;

    #[test]
    fn outbox_directory_is_exclusive_through_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let mut first = SupportOutbox::open(dir.path()).unwrap();
        assert!(matches!(
            SupportOutbox::open(dir.path()),
            Err(SupportOutboxError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));
        first.poisoned = true;
        first.recover_if_poisoned().unwrap();
        assert!(matches!(
            SupportOutbox::open(dir.path()),
            Err(SupportOutboxError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));
        drop(first);
        SupportOutbox::open(dir.path()).unwrap();
    }
    use crate::authority::certificate::KeysetVersion;
    use crate::authority::operation_merkle::{InclusionProof, OperationScope, SignedOperationRoot};
    use crate::hlc::HlcTimestamp;
    use crate::network::operation_neighbors::OperationNeighborManifest;
    use crate::operation::{
        CandidateKvAction, CertificationMode, CrdtSubject, Declaration, EventDot, OperationLeaf,
    };
    use crate::types::NodeId;

    fn packet() -> SupportPacket {
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 1,
            },
        };
        SupportPacket {
            operation: BoundOperation {
                operation: OperationLeaf {
                    subject: subject.clone(),
                    action: CandidateKvAction::Create {
                        key: "key".into(),
                        value_bytes: b"body".to_vec(),
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
            },
            support: OperationRootSupport {
                signed_root: SignedOperationRoot {
                    root: [5; 32],
                    leaf_count: 1,
                    scope: OperationScope {
                        namespace_id: "ns".into(),
                        key_prefix: "".into(),
                    },
                    keyset_version: KeysetVersion(1),
                    authority_id: NodeId("auth-1".into()),
                    signature: [7; 64],
                },
                proof: InclusionProof {
                    leaf_index: 0,
                    leaf_count: 1,
                    siblings: vec![],
                },
            },
        }
    }

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

    #[test]
    fn two_valid_roots_for_same_operation_have_separate_durable_deliveries() {
        let root = tempfile::tempdir().unwrap();
        let mut ledger = OperationLedger::open(root.path().join("admitted")).unwrap();
        let target = packet().operation;
        let mut other = target.clone();
        other.operation.subject.event_dot.counter = 2;
        other.declaration.creation_id = other.operation.subject.clone();
        if let CandidateKvAction::Create { key, .. } = &mut other.operation.action {
            *key = "other".into();
        }
        other.declaration.key = "other".into();
        let first = admit_for_signing(&Allow, &mut ledger, target.clone()).unwrap();
        let second = admit_for_signing(&Allow, &mut ledger, other).unwrap();
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "".into(),
        };
        let one = OperationMerkleTree::from_signable(scope.clone(), std::slice::from_ref(&first))
            .unwrap();
        let two = OperationMerkleTree::from_signable(scope.clone(), &[first, second]).unwrap();
        let signer = SigningKey::from_bytes(&[1; 32]);
        let authority = NodeId("auth-1".into());
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![(authority.clone(), signer.verifying_key())],
            )
            .unwrap();
        let make_packet = |tree: &OperationMerkleTree| SupportPacket {
            operation: target.clone(),
            support: OperationRootSupport {
                signed_root: SignedOperationRoot::sign(
                    tree,
                    KeysetVersion(1),
                    authority.clone(),
                    &signer,
                ),
                proof: tree.proof(&target.operation.subject).unwrap(),
            },
        };
        let first_packet = make_packet(&one);
        let second_packet = make_packet(&two);
        assert_eq!(
            first_packet.operation.commitment(),
            second_packet.operation.commitment()
        );
        assert_ne!(first_packet.commitment(), second_packet.commitment());
        for packet in [&first_packet, &second_packet] {
            packet
                .support
                .verify_individual(
                    &packet.operation,
                    &registry,
                    &scope,
                    0,
                    &EpochConfig::default(),
                )
                .unwrap();
        }
        let (first_id, second_id) = {
            let mut outbox = SupportOutbox::open(root.path().join("outbox")).unwrap();
            let first_id = outbox
                .ensure_enqueued("receiver:1234".into(), first_packet.clone())
                .unwrap();
            let second_id = outbox
                .ensure_enqueued("receiver:1234".into(), second_packet.clone())
                .unwrap();
            assert_ne!(first_id, second_id);
            (first_id, second_id)
        };
        let mut outbox = SupportOutbox::open(root.path().join("outbox")).unwrap();
        assert_eq!(outbox.pending().count(), 2);
        for (id, packet) in [(first_id, &first_packet), (second_id, &second_packet)] {
            assert_eq!(
                outbox
                    .ensure_enqueued("receiver:1234".into(), packet.clone())
                    .unwrap(),
                id
            );
            outbox
                .record_receipt(
                    id,
                    SupportReceipt {
                        commitment: packet.commitment(),
                        already_present: false,
                        certificate_ready: false,
                    },
                )
                .unwrap();
        }
        assert_eq!(outbox.compact_acked_and_sync().unwrap(), 2);
        drop(outbox);
        let mut outbox = SupportOutbox::open(root.path().join("outbox")).unwrap();
        assert_eq!(outbox.pending().count(), 0);
        for (id, packet) in [(first_id, &first_packet), (second_id, &second_packet)] {
            assert_eq!(
                outbox
                    .ensure_enqueued("receiver:1234".into(), packet.clone())
                    .unwrap(),
                id
            );
            assert!(outbox.has_durable_receipt("receiver:1234", packet));
        }
    }

    #[test]
    fn commitment_covers_body_root_signature_and_inclusion_path() {
        let original = packet();
        let mut changed = original.clone();
        if let CandidateKvAction::Create { value_bytes, .. } =
            &mut changed.operation.operation.action
        {
            *value_bytes = b"other".to_vec();
        }
        assert_ne!(original.commitment(), changed.commitment());
        let mut changed = original.clone();
        changed.support.signed_root.signature[0] ^= 1;
        assert_ne!(original.commitment(), changed.commitment());
        let mut changed = original.clone();
        changed.support.proof.siblings.push([9; 32]);
        assert_ne!(original.commitment(), changed.commitment());
    }

    #[test]
    fn ensure_reuses_exact_support_across_restart_and_enrolls_distinct_support() {
        let root = tempfile::tempdir().unwrap();
        let original = packet();
        let id = {
            let mut outbox = SupportOutbox::open(root.path()).unwrap();
            let id = outbox
                .ensure_enqueued("receiver:1234".into(), original.clone())
                .unwrap();
            assert_eq!(
                outbox
                    .ensure_enqueued("receiver:1234".into(), original.clone())
                    .unwrap(),
                id
            );
            id
        };
        let mut outbox = SupportOutbox::open(root.path()).unwrap();
        assert_eq!(outbox.pending().count(), 1);
        assert_eq!(
            outbox
                .ensure_enqueued("receiver:1234".into(), original.clone())
                .unwrap(),
            id
        );
        let mut distinct = original.clone();
        distinct.support.signed_root.root[0] ^= 1;
        assert_ne!(
            outbox
                .ensure_enqueued("receiver:1234".into(), distinct)
                .unwrap(),
            id
        );
        outbox
            .record_receipt(
                id,
                SupportReceipt {
                    commitment: original.commitment(),
                    already_present: false,
                    certificate_ready: false,
                },
            )
            .unwrap();
        drop(outbox);
        let mut outbox = SupportOutbox::open(root.path()).unwrap();
        assert_eq!(outbox.pending().count(), 1);
        assert_eq!(
            outbox
                .ensure_enqueued("receiver:1234".into(), original)
                .unwrap(),
            id
        );
    }

    #[test]
    fn wrong_ack_does_not_complete_support() {
        let root = tempfile::tempdir().unwrap();
        let mut outbox = SupportOutbox::open(root.path()).unwrap();
        let id = outbox
            .ensure_enqueued("receiver:1234".into(), packet())
            .unwrap();
        assert!(matches!(
            outbox.record_receipt(
                id,
                SupportReceipt {
                    commitment: [0; 32],
                    already_present: false,
                    certificate_ready: true,
                }
            ),
            Err(SupportOutboxError::WrongReceipt(_))
        ));
        assert_eq!(outbox.pending().count(), 1);
    }

    #[test]
    fn acknowledged_send_compacts_without_losing_exact_packet_identity() {
        let root = tempfile::tempdir().unwrap();
        let original = packet();
        let id;
        {
            let mut outbox = SupportOutbox::open(root.path()).unwrap();
            id = outbox
                .ensure_enqueued("receiver:1234".into(), original.clone())
                .unwrap();
            assert_eq!(outbox.compact_acked_and_sync().unwrap(), 0);
            outbox
                .record_receipt(
                    id,
                    SupportReceipt {
                        commitment: original.commitment(),
                        already_present: false,
                        certificate_ready: true,
                    },
                )
                .unwrap();
            assert_eq!(outbox.compact_acked_and_sync().unwrap(), 1);
            assert!(outbox.get(id).is_none());
            assert!(outbox.has_durable_receipt("receiver:1234", &original));
        }
        let send = fs::read(root.path().join(format!("{id}.send"))).unwrap();
        assert!(!send.windows(b"body".len()).any(|window| window == b"body"));
        let mut outbox = SupportOutbox::open(root.path()).unwrap();
        assert_eq!(outbox.pending().count(), 0);
        assert_eq!(
            outbox
                .ensure_enqueued("receiver:1234".into(), original.clone())
                .unwrap(),
            id
        );
        assert!(outbox.has_durable_receipt("receiver:1234", &original));
        let mut distinct = original.clone();
        distinct.support.signed_root.root[0] ^= 1;
        assert_ne!(
            outbox
                .ensure_enqueued("receiver:1234".into(), distinct)
                .unwrap(),
            id
        );
        let next = outbox
            .ensure_enqueued("other:1234".into(), original)
            .unwrap();
        assert!(next > id);
        drop(outbox);
        let mut receipt: SupportReceipt =
            serde_json::from_slice(&fs::read(root.path().join(format!("{id}.ack"))).unwrap())
                .unwrap();
        receipt.commitment[0] ^= 1;
        fs::write(
            root.path().join(format!("{id}.ack")),
            serde_json::to_vec(&receipt).unwrap(),
        )
        .unwrap();
        assert!(
            matches!(SupportOutbox::open(root.path()), Err(SupportOutboxError::WrongReceipt(found)) if found == id)
        );
    }

    #[test]
    fn different_commitments_from_one_authority_survive_recovery_and_compaction() {
        let root = tempfile::tempdir().unwrap();
        let original = packet();
        let mut alternate = original.clone();
        if let CandidateKvAction::Create { value_bytes, .. } =
            &mut alternate.operation.operation.action
        {
            *value_bytes = b"alternate".to_vec();
        }
        assert_ne!(
            original.operation.commitment(),
            alternate.operation.commitment()
        );
        let (first, second) = {
            let mut outbox = SupportOutbox::open(root.path()).unwrap();
            let first = outbox
                .ensure_enqueued("receiver:1234".into(), original.clone())
                .unwrap();
            let second = outbox
                .ensure_enqueued("receiver:1234".into(), alternate.clone())
                .unwrap();
            assert_ne!(first, second);
            (first, second)
        };
        {
            let mut outbox = SupportOutbox::open(root.path()).unwrap();
            assert_eq!(outbox.pending().count(), 2);
            assert_eq!(
                outbox
                    .ensure_enqueued("receiver:1234".into(), original.clone())
                    .unwrap(),
                first
            );
            assert_eq!(
                outbox
                    .ensure_enqueued("receiver:1234".into(), alternate.clone())
                    .unwrap(),
                second
            );
            for (id, packet) in [(first, &original), (second, &alternate)] {
                outbox
                    .record_receipt(
                        id,
                        SupportReceipt {
                            commitment: packet.commitment(),
                            already_present: false,
                            certificate_ready: false,
                        },
                    )
                    .unwrap();
            }
            assert_eq!(outbox.compact_acked_and_sync().unwrap(), 2);
        }
        let mut outbox = SupportOutbox::open(root.path()).unwrap();
        assert_eq!(outbox.pending().count(), 0);
        for (id, packet) in [(first, &original), (second, &alternate)] {
            assert_eq!(
                outbox
                    .ensure_enqueued("receiver:1234".into(), packet.clone())
                    .unwrap(),
                id
            );
            assert!(outbox.has_durable_receipt("receiver:1234", packet));
        }
    }

    #[test]
    fn poisoned_outbox_recovers_published_compaction_before_enrolling_again() {
        let root = tempfile::tempdir().unwrap();
        let original = packet();
        let mut outbox = SupportOutbox::open(root.path()).unwrap();
        let id = outbox
            .ensure_enqueued("receiver:1234".into(), original.clone())
            .unwrap();
        outbox
            .record_receipt(
                id,
                SupportReceipt {
                    commitment: original.commitment(),
                    already_present: false,
                    certificate_ready: true,
                },
            )
            .unwrap();
        let compact = CompactSendRecord::from_entry(outbox.get(id).unwrap());
        replace_file(
            root.path(),
            &send_path(root.path(), id),
            &serde_json::to_vec(&compact).unwrap(),
        )
        .unwrap();
        // Simulate a failure after the replacement was published but before
        // the in-memory index was updated.
        outbox.poisoned = true;
        assert_eq!(
            outbox
                .ensure_enqueued("receiver:1234".into(), original.clone())
                .unwrap(),
            id
        );
        assert!(outbox.get(id).is_none());
        assert_eq!(outbox.pending().count(), 0);
        assert!(outbox.has_durable_receipt("receiver:1234", &original));
    }

    #[test]
    fn poisoned_outbox_recovers_published_send_before_reusing_id() {
        let root = tempfile::tempdir().unwrap();
        let original = packet();
        let mut outbox = SupportOutbox::open(root.path()).unwrap();
        let record = SendRecord {
            version: 1,
            id: 0,
            peer_addr: "first:1234".into(),
            packet: original.clone(),
        };
        atomic_create(
            root.path(),
            &send_path(root.path(), 0),
            &serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        // The file can be published even if the caller observed an I/O error.
        outbox.poisoned = true;
        assert_eq!(outbox.pending().count(), 0);
        assert_eq!(
            outbox
                .ensure_enqueued("second:1234".into(), original.clone())
                .unwrap(),
            1
        );
        assert_eq!(outbox.pending().count(), 2);
        assert_eq!(
            outbox
                .ensure_enqueued("first:1234".into(), original)
                .unwrap(),
            0
        );
    }

    #[test]
    fn authenticated_outbox_rejects_legacy_ack_and_changed_keys() {
        let root = tempfile::tempdir().unwrap();
        let mut old = SupportOutbox::open(root.path()).unwrap();
        old.ensure_enqueued("peer:1234".into(), packet()).unwrap();
        assert!(old.ensure_link_manifest([1; 32]).is_err());

        let fresh = tempfile::tempdir().unwrap();
        let outbox = SupportOutbox::open(fresh.path()).unwrap();
        outbox.ensure_link_manifest([1; 32]).unwrap();
        assert!(outbox.ensure_link_manifest([2; 32]).is_err());
        drop(outbox);
        assert!(SupportOutbox::open(fresh.path()).is_ok());
    }

    #[tokio::test]
    async fn authenticated_client_rejects_unsigned_matching_ack() {
        use axum::{Json, Router, routing::post};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let original = packet();
        let commitment = original.commitment();
        let app = Router::new().route(
            SUPPORT_RECEIVE_PATH,
            post(move || async move {
                Json(SupportReceipt {
                    commitment,
                    already_present: false,
                    certificate_ready: false,
                })
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let neighbors = OperationNeighborManifest::from_json(
            &serde_json::json!([{"node_id":"peer","addr":addr,"key_prefixes":[""]}]).to_string(),
            &NodeId("self".into()),
        )
        .unwrap();
        let peer_key = ed25519_dalek::SigningKey::from_bytes(&[2; 32]).verifying_key();
        let link = OperationLinkAuth::from_manifest(
            NodeId("self".into()),
            &hex::encode([1; 32]),
            &serde_json::json!([{"node_id":"peer","public_key":hex::encode(peer_key.as_bytes())}])
                .to_string(),
            &neighbors,
        )
        .unwrap();
        let client = SupportTransportClient::authenticated("secret".into(), link);
        assert!(matches!(
            client.send(&addr, &original).await,
            Err(SupportSendError::WrongPeer)
        ));
        server.abort();
    }

    #[tokio::test]
    async fn worker_retries_recovered_support_and_persists_ack() {
        use axum::{Json, Router, routing::post};
        let root = tempfile::tempdir().unwrap();
        let original = packet();
        let digest = original.commitment();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let app = Router::new().route(
            SUPPORT_RECEIVE_PATH,
            post(move || async move {
                Json(SupportReceipt {
                    commitment: digest,
                    already_present: false,
                    certificate_ready: false,
                })
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let id = {
            let mut outbox = SupportOutbox::open(root.path().join("support-outbox")).unwrap();
            outbox.ensure_enqueued(address, original.clone()).unwrap()
        };
        let restored = Arc::new(tokio::sync::Mutex::new(
            SupportOutbox::open(root.path().join("support-outbox")).unwrap(),
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
                if restored.lock().await.get(id).unwrap().receipt.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        shutdown_tx.send(true).unwrap();
        worker.await.unwrap();
        server.abort();
        drop(restored);
        let reopened = SupportOutbox::open(root.path().join("support-outbox")).unwrap();
        assert_eq!(reopened.pending().count(), 0);
        assert_eq!(reopened.get(id).unwrap().packet, original);
    }

    #[tokio::test]
    async fn client_rejects_wrong_http_receipt_and_keeps_retry_copy() {
        use axum::{Json, Router, routing::post};
        let root = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let app = Router::new().route(
            SUPPORT_RECEIVE_PATH,
            post(|| async {
                Json(SupportReceipt {
                    commitment: [0; 32],
                    already_present: false,
                    certificate_ready: true,
                })
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut outbox = SupportOutbox::open(root.path()).unwrap();
        let id = outbox.ensure_enqueued(address, packet()).unwrap();
        assert!(matches!(
            outbox
                .deliver(id, &SupportTransportClient::new("secret".into()))
                .await,
            Err(SupportOutboxError::Send(SupportSendError::WrongCommitment))
        ));
        assert_eq!(outbox.pending().count(), 1);
        drop(outbox);
        server.abort();
        assert_eq!(
            SupportOutbox::open(root.path()).unwrap().pending().count(),
            1
        );
    }
}
