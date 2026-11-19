//! Durable sender obligations for complete operation certificates.
//!
//! The receiver verifies the quorum and exact operation before returning a
//! receipt. This module keeps the exact certificate outside checkpoint-pruned WALs and
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

use crate::authority::operation_merkle::OperationMerkleCertificate;
use crate::network::operation_link_auth::{
    OperationLinkAuth, RECEIVER as LINK_RECEIVER, REQUEST_DIGEST as LINK_REQUEST_DIGEST,
    SENDER as LINK_SENDER, SIGNATURE as LINK_SIGNATURE,
};
use crate::operation::CrdtSubject;
use crate::store::backend::{create_dir_all_durable, fsync_dir};

pub const CERTIFICATE_RECEIVE_PATH: &str = "/api/internal/operations/certificates/receive";
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const OUTBOX_LOCK: &str = ".lock";

pub fn certificate_commitment(
    packet: &OperationMerkleCertificate,
) -> Result<[u8; 32], serde_json::Error> {
    let bytes = serde_json::to_vec(packet)?;
    let mut hash = Sha256::new();
    hash.update(b"asteroidb/operation-certificate-packet/v1");
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(hash.finalize().into())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertificateReceipt {
    pub commitment: [u8; 32],
    pub already_present: bool,
}

pub struct CertificateTransportClient {
    client: reqwest::Client,
    token: String,
    link: Option<OperationLinkAuth>,
}

#[derive(Debug, thiserror::Error)]
pub enum CertificateSendError {
    #[error("certificate encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("certificate transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("certificate receiver rejected packet with HTTP {0}")]
    Rejected(reqwest::StatusCode),
    #[error("certificate receiver acknowledged different packet")]
    WrongCommitment,
    #[error("certificate peer authentication failed")]
    WrongPeer,
}

impl CertificateTransportClient {
    pub fn new(token: String) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("certificate transport client"),
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
        packet: &OperationMerkleCertificate,
    ) -> Result<CertificateReceipt, CertificateSendError> {
        let body = serde_json::to_vec(packet).map_err(CertificateSendError::Encode)?;
        let mut request = self
            .client
            .post(format!("http://{peer_addr}{CERTIFICATE_RECEIVE_PATH}"))
            .bearer_auth(&self.token)
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        let mut expected = None;
        if let Some(link) = &self.link {
            let peer = link
                .peer_for_addr(peer_addr)
                .ok_or(CertificateSendError::WrongPeer)?;
            let (digest, signature) =
                link.sign_request(peer, "POST", CERTIFICATE_RECEIVE_PATH, &body);
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
                        CERTIFICATE_RECEIVE_PATH,
                        &digest,
                        status.as_u16(),
                        &bytes,
                        signature,
                    )
                }))
        {
            return Err(CertificateSendError::WrongPeer);
        }
        if !status.is_success() {
            return Err(CertificateSendError::Rejected(status));
        }
        let receipt: CertificateReceipt =
            serde_json::from_slice(&bytes).map_err(CertificateSendError::Encode)?;
        if receipt.commitment
            != certificate_commitment(packet).map_err(CertificateSendError::Encode)?
        {
            return Err(CertificateSendError::WrongCommitment);
        }
        Ok(receipt)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SendRecord {
    version: u32,
    id: u64,
    peer_addr: String,
    packet: OperationMerkleCertificate,
}

/// A completed transfer keeps its identity and authenticated packet digest,
/// without retaining the operation value after the peer has acknowledged it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactSendRecord {
    version: u32,
    id: u64,
    peer_addr: String,
    subject: CrdtSubject,
    commitment: [u8; 32],
    receipt: CertificateReceipt,
}

#[derive(Debug, Clone)]
pub struct CertificateOutboxEntry {
    pub id: u64,
    pub peer_addr: String,
    pub packet: OperationMerkleCertificate,
    pub receipt: Option<CertificateReceipt>,
}

#[derive(Debug, thiserror::Error)]
pub enum CertificateOutboxError {
    #[error("certificate outbox I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("certificate outbox serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("certificate send failed: {0}")]
    Send(#[from] CertificateSendError),
    #[error("unknown certificate transfer {0}")]
    Unknown(u64),
    #[error("certificate receipt does not match transfer {0}")]
    WrongReceipt(u64),
    #[error(
        "same operation certificate identity has different content for peer {peer_addr} (transfer {transfer_id})"
    )]
    ConflictingCertificate { peer_addr: String, transfer_id: u64 },
    #[error("certificate transfer sequence exhausted")]
    Exhausted,
}

/// A single process owns this directory. It must remain outside normal store
/// checkpoint pruning for the entire transport-obligation lifetime.
pub struct CertificateOutbox {
    dir: PathBuf,
    lock: File,
    next_id: u64,
    entries: BTreeMap<u64, CertificateOutboxEntry>,
    compact: BTreeMap<u64, CompactSendRecord>,
    poisoned: bool,
}

impl CertificateOutbox {
    /// Rebuild after an uncertain replacement write while the caller holds
    /// the outbox mutex. Failed recovery leaves the old handle poisoned.
    pub fn recover_if_poisoned(&mut self) -> Result<(), CertificateOutboxError> {
        if self.poisoned {
            let recovered = Self::load(&self.dir, self.lock.try_clone()?)?;
            *self = recovered;
        }
        Ok(())
    }

    pub fn ensure_link_manifest(&self, digest: [u8; 32]) -> Result<(), CertificateOutboxError> {
        let path = self.dir.join(".link-manifest");
        if path.exists() {
            if fs::read(&path)? != digest {
                return Err(invalid("certificate link identities changed").into());
            }
            File::open(&path)?.sync_all()?;
            fsync_dir(&self.dir)?;
            return Ok(());
        }
        if !self.entries.is_empty() || !self.compact.is_empty() {
            return Err(invalid(
                "legacy certificate outbox requires migration before authenticated delivery",
            )
            .into());
        }
        atomic_create(&self.dir, &path, &digest)?;
        Ok(())
    }

    pub fn open(dir: impl AsRef<Path>) -> Result<Self, CertificateOutboxError> {
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

    fn load(dir: &Path, lock: File) -> Result<Self, CertificateOutboxError> {
        let mut entries = BTreeMap::new();
        let mut compact = BTreeMap::new();
        let mut max_id = None::<u64>;
        for item in fs::read_dir(dir)? {
            let item = item?;
            let name = item.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| invalid("non-UTF8 certificate outbox filename"))?;
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
                return Err(invalid("unknown certificate outbox file").into());
            };
            let id = parse_id(stem)?;
            let bytes = fs::read(item.path())?;
            let value: serde_json::Value = serde_json::from_slice(&bytes)?;
            if value.get("version").and_then(|value| value.as_u64()) == Some(2) {
                let record: CompactSendRecord = serde_json::from_value(value)?;
                if record.version != 2
                    || record.id != id
                    || record.peer_addr.is_empty()
                    || record.receipt.commitment != record.commitment
                {
                    return Err(invalid("invalid compact certificate send record").into());
                }
                let receipt: CertificateReceipt =
                    serde_json::from_slice(&fs::read(ack_path(dir, id))?)?;
                if receipt != record.receipt {
                    return Err(CertificateOutboxError::WrongReceipt(id));
                }
                max_id = Some(max_id.map_or(id, |max| max.max(id)));
                compact.insert(id, record);
                continue;
            }
            let record: SendRecord = serde_json::from_slice(&bytes)?;
            if record.version != 1 || record.id != id || record.peer_addr.is_empty() {
                return Err(invalid("invalid certificate send record").into());
            }
            let receipt = if ack_path(dir, id).exists() {
                let receipt: CertificateReceipt =
                    serde_json::from_slice(&fs::read(ack_path(dir, id))?)?;
                if receipt.commitment != certificate_commitment(&record.packet)? {
                    return Err(CertificateOutboxError::WrongReceipt(id));
                }
                Some(receipt)
            } else {
                None
            };
            max_id = Some(max_id.map_or(id, |max| max.max(id)));
            entries.insert(
                id,
                CertificateOutboxEntry {
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
                    return Err(invalid("orphan certificate ACK").into());
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
                return Err(invalid("conflicting persisted certificate obligations").into());
            }
            if compact.values().any(|other| {
                entry.peer_addr == other.peer_addr
                    && entry.packet.operation.operation.subject == other.subject
                    && certificate_commitment(&entry.packet).ok() != Some(other.commitment)
            }) {
                return Err(invalid("conflicting persisted certificate obligations").into());
            }
        }
        for (id, entry) in &compact {
            if compact.iter().any(|(other_id, other)| {
                other_id != id
                    && entry.peer_addr == other.peer_addr
                    && entry.subject == other.subject
                    && entry.commitment != other.commitment
            }) {
                return Err(invalid("conflicting persisted certificate obligations").into());
            }
        }
        fsync_dir(dir)?;
        let next_id = max_id
            .map_or(Some(0), |id| id.checked_add(1))
            .ok_or(CertificateOutboxError::Exhausted)?;
        Ok(Self {
            dir: dir.to_owned(),
            lock,
            next_id,
            entries,
            compact,
            poisoned: false,
        })
    }

    /// Idempotently enroll one exact neighbor send. Supports from different
    /// Authorities or keyset versions have distinct obligations.
    pub fn ensure_enqueued(
        &mut self,
        peer_addr: String,
        packet: OperationMerkleCertificate,
    ) -> Result<u64, CertificateOutboxError> {
        self.recover_if_poisoned()?;
        if peer_addr.is_empty() {
            return Err(invalid("empty peer address").into());
        }
        for entry in self.entries.values() {
            if same_identity(&peer_addr, &packet, &entry.peer_addr, &entry.packet) {
                return if entry.packet == packet {
                    Ok(entry.id)
                } else {
                    Err(CertificateOutboxError::ConflictingCertificate {
                        peer_addr,
                        transfer_id: entry.id,
                    })
                };
            }
        }
        for entry in self.compact.values() {
            if entry.peer_addr == peer_addr && entry.subject == packet.operation.operation.subject {
                return if entry.commitment == certificate_commitment(&packet)? {
                    Ok(entry.id)
                } else {
                    Err(CertificateOutboxError::ConflictingCertificate {
                        peer_addr,
                        transfer_id: entry.id,
                    })
                };
            }
        }
        let id = self.next_id;
        let next_id = id.checked_add(1).ok_or(CertificateOutboxError::Exhausted)?;
        let record = SendRecord {
            version: 1,
            id,
            peer_addr: peer_addr.clone(),
            packet: packet.clone(),
        };
        // An I/O error after rename can leave this sequence on disk while
        // memory still points at the previous next ID.
        self.poisoned = true;
        atomic_create(
            &self.dir,
            &send_path(&self.dir, id),
            &serde_json::to_vec(&record)?,
        )?;
        self.entries.insert(
            id,
            CertificateOutboxEntry {
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

    pub fn get(&self, id: u64) -> Option<&CertificateOutboxEntry> {
        self.entries.get(&id)
    }
    pub fn entries(&self) -> impl Iterator<Item = &CertificateOutboxEntry> {
        self.entries.values()
    }
    /// True when this exact certificate was durably enrolled for a peer.
    /// Includes completed compact transfers, for evidence GC reconciliation.
    pub fn has_enqueued(
        &self,
        peer_addr: &str,
        packet: &OperationMerkleCertificate,
    ) -> Result<bool, CertificateOutboxError> {
        if self.poisoned {
            return Err(invalid("certificate outbox writer must be reopened").into());
        }
        if self
            .entries
            .values()
            .any(|entry| entry.peer_addr == peer_addr && entry.packet == *packet)
        {
            return Ok(true);
        }
        let commitment = certificate_commitment(packet)?;
        Ok(self.compact.values().any(|entry| {
            entry.peer_addr == peer_addr
                && entry.subject == packet.operation.operation.subject
                && entry.commitment == commitment
        }))
    }
    /// Replace acknowledged value-bearing sends with durable identity records.
    /// A completed entry remains present for idempotent enrollment and conflict checks.
    pub fn compact_acked(&mut self) -> Result<usize, CertificateOutboxError> {
        self.recover_if_poisoned()?;
        let ids: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(id, entry)| entry.receipt.as_ref().map(|_| *id))
            .collect();
        for id in &ids {
            let entry = self.entries.get(id).expect("selected entry exists");
            let receipt = entry
                .receipt
                .clone()
                .expect("selected entry is acknowledged");
            let record = CompactSendRecord {
                version: 2,
                id: *id,
                peer_addr: entry.peer_addr.clone(),
                subject: entry.packet.operation.operation.subject.clone(),
                commitment: certificate_commitment(&entry.packet)?,
                receipt,
            };
            let path = send_path(&self.dir, *id);
            self.poisoned = true;
            atomic_replace(&self.dir, &path, &serde_json::to_vec(&record)?)?;
            self.compact.insert(*id, record);
            self.entries.remove(id);
            self.poisoned = false;
        }
        Ok(ids.len())
    }
    pub fn pending(&self) -> impl Iterator<Item = &CertificateOutboxEntry> {
        self.entries
            .values()
            .filter(|entry| !self.poisoned && entry.receipt.is_none())
    }

    pub fn record_receipt(
        &mut self,
        id: u64,
        receipt: CertificateReceipt,
    ) -> Result<(), CertificateOutboxError> {
        self.recover_if_poisoned()?;
        if let Some(entry) = self.compact.get(&id) {
            return if entry.receipt.commitment == receipt.commitment {
                Ok(())
            } else {
                Err(CertificateOutboxError::WrongReceipt(id))
            };
        }
        let entry = self
            .entries
            .get_mut(&id)
            .ok_or(CertificateOutboxError::Unknown(id))?;
        if receipt.commitment != certificate_commitment(&entry.packet)? {
            return Err(CertificateOutboxError::WrongReceipt(id));
        }
        if entry.receipt.is_some() {
            return Ok(());
        }
        let path = ack_path(&self.dir, id);
        if path.exists() {
            let stored: CertificateReceipt = serde_json::from_slice(&fs::read(&path)?)?;
            if stored.commitment != certificate_commitment(&entry.packet)? {
                return Err(CertificateOutboxError::WrongReceipt(id));
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
        client: &CertificateTransportClient,
    ) -> Result<(), CertificateOutboxError> {
        self.recover_if_poisoned()?;
        if self.compact.contains_key(&id) {
            return Ok(());
        }
        let entry = self
            .entries
            .get(&id)
            .ok_or(CertificateOutboxError::Unknown(id))?;
        if entry.receipt.is_some() {
            return Ok(());
        }
        let addr = entry.peer_addr.clone();
        let packet = entry.packet.clone();
        let receipt = client.send(&addr, &packet).await?;
        self.record_receipt(id, receipt)
    }
}

fn same_identity(
    a_addr: &str,
    a: &OperationMerkleCertificate,
    b_addr: &str,
    b: &OperationMerkleCertificate,
) -> bool {
    a_addr == b_addr && a.operation.operation.subject == b.operation.operation.subject
}

fn parse_id(stem: &str) -> io::Result<u64> {
    let id = stem
        .parse::<u64>()
        .map_err(|_| invalid("invalid certificate transfer ID"))?;
    if stem != id.to_string() {
        return Err(invalid("noncanonical certificate transfer ID"));
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
                "certificate record exists",
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

fn atomic_replace(dir: &Path, final_path: &Path, bytes: &[u8]) -> io::Result<()> {
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

pub fn spawn_certificate_retry_worker(
    outbox: Arc<tokio::sync::Mutex<CertificateOutbox>>,
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

pub fn spawn_certificate_retry_worker_with_auth(
    outbox: Arc<tokio::sync::Mutex<CertificateOutbox>>,
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
    outbox: Arc<tokio::sync::Mutex<CertificateOutbox>>,
    token: String,
    link: Option<OperationLinkAuth>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    interval: Duration,
) {
    let client = match link {
        Some(link) => CertificateTransportClient::authenticated(token, link),
        None => CertificateTransportClient::new(token),
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
                        tracing::warn!(error = %error, "certificate outbox recovery failed; retaining send obligations");
                        continue;
                    }
                    cursor.next(guard.pending().map(|entry| entry.id))
                        .and_then(|id| guard.pending().find(|entry| entry.id == id).cloned())
                };
                let Some(entry) = entry else { continue; };
                match client.send(&entry.peer_addr, &entry.packet).await {
                    Ok(receipt) => {
                        if let Err(error) = outbox.lock().await.record_receipt(entry.id, receipt) {
                            tracing::warn!(transfer_id = entry.id, error = %error, "certificate ACK persistence failed; retaining send obligation");
                        }
                    }
                    Err(error) => tracing::debug!(transfer_id = entry.id, error = %error, "certificate send failed; retaining send obligation"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbox_directory_is_exclusive_through_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let mut first = CertificateOutbox::open(dir.path()).unwrap();
        assert!(matches!(
            CertificateOutbox::open(dir.path()),
            Err(CertificateOutboxError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));
        first.poisoned = true;
        first.recover_if_poisoned().unwrap();
        assert!(matches!(
            CertificateOutbox::open(dir.path()),
            Err(CertificateOutboxError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));
        drop(first);
        CertificateOutbox::open(dir.path()).unwrap();
    }

    #[test]
    fn authenticated_outbox_rejects_legacy_receipt_and_changed_pin() {
        let old_dir = tempfile::tempdir().unwrap();
        let mut old = CertificateOutbox::open(old_dir.path()).unwrap();
        old.ensure_enqueued("peer:1234".into(), certificate())
            .unwrap();
        assert!(old.ensure_link_manifest([1; 32]).is_err());

        let new_dir = tempfile::tempdir().unwrap();
        let fresh = CertificateOutbox::open(new_dir.path()).unwrap();
        fresh.ensure_link_manifest([1; 32]).unwrap();
        assert!(fresh.ensure_link_manifest([2; 32]).is_err());
        drop(fresh);
        assert!(CertificateOutbox::open(new_dir.path()).is_ok());
    }
    use crate::hlc::HlcTimestamp;
    use crate::operation::{
        BoundOperation, CandidateKvAction, CertificationMode, CrdtSubject, Declaration, EventDot,
        OperationLeaf,
    };
    use crate::types::NodeId;

    fn certificate() -> OperationMerkleCertificate {
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 1,
            },
        };
        OperationMerkleCertificate {
            operation: BoundOperation {
                operation: OperationLeaf {
                    subject: subject.clone(),
                    action: CandidateKvAction::Create {
                        key: "key".into(),
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
                    key: "key".into(),
                    mode: CertificationMode::Enabled,
                },
            },
            supports: Vec::new(),
        }
    }

    #[test]
    fn durable_obligation_rejects_wrong_ack_and_conflicting_reenrollment() {
        let dir = tempfile::tempdir().unwrap();
        let packet = certificate();
        let mut outbox = CertificateOutbox::open(dir.path()).unwrap();
        let id = outbox
            .ensure_enqueued("peer:1234".into(), packet.clone())
            .unwrap();
        assert_eq!(
            outbox
                .ensure_enqueued("peer:1234".into(), packet.clone())
                .unwrap(),
            id
        );
        assert!(matches!(
            outbox.record_receipt(
                id,
                CertificateReceipt {
                    commitment: [0; 32],
                    already_present: false
                }
            ),
            Err(CertificateOutboxError::WrongReceipt(_))
        ));
        let mut changed = packet.clone();
        changed.operation.operation.action = CandidateKvAction::Delete { key: "key".into() };
        assert!(matches!(
            outbox.ensure_enqueued("peer:1234".into(), changed),
            Err(CertificateOutboxError::ConflictingCertificate { .. })
        ));
        drop(outbox);
        let mut recovered = CertificateOutbox::open(dir.path()).unwrap();
        assert_eq!(recovered.pending().count(), 1);
        recovered
            .record_receipt(
                id,
                CertificateReceipt {
                    commitment: certificate_commitment(&packet).unwrap(),
                    already_present: false,
                },
            )
            .unwrap();
        drop(recovered);
        assert_eq!(
            CertificateOutbox::open(dir.path())
                .unwrap()
                .pending()
                .count(),
            0
        );
    }

    #[test]
    fn acknowledged_send_compacts_value_and_recovers_identity() {
        let dir = tempfile::tempdir().unwrap();
        let packet = certificate();
        let mut outbox = CertificateOutbox::open(dir.path()).unwrap();
        let id = outbox
            .ensure_enqueued("peer:1234".into(), packet.clone())
            .unwrap();
        assert_eq!(outbox.compact_acked().unwrap(), 0);
        let receipt = CertificateReceipt {
            commitment: certificate_commitment(&packet).unwrap(),
            already_present: false,
        };
        outbox.record_receipt(id, receipt).unwrap();
        assert_eq!(outbox.compact_acked().unwrap(), 1);
        assert_eq!(outbox.compact_acked().unwrap(), 0);
        assert!(outbox.has_enqueued("peer:1234", &packet).unwrap());
        let bytes = fs::read(send_path(dir.path(), id)).unwrap();
        assert!(!bytes.windows(b"value".len()).any(|part| part == b"value"));
        drop(outbox);

        let mut recovered = CertificateOutbox::open(dir.path()).unwrap();
        assert_eq!(recovered.pending().count(), 0);
        assert_eq!(
            recovered
                .ensure_enqueued("peer:1234".into(), packet.clone())
                .unwrap(),
            id
        );
        let mut changed = packet;
        changed.operation.operation.action = CandidateKvAction::Delete { key: "key".into() };
        assert!(matches!(
            recovered.ensure_enqueued("peer:1234".into(), changed),
            Err(CertificateOutboxError::ConflictingCertificate { transfer_id, .. }) if transfer_id == id
        ));
        drop(recovered);
        let mut changed_receipt: CertificateReceipt =
            serde_json::from_slice(&fs::read(ack_path(dir.path(), id)).unwrap()).unwrap();
        changed_receipt.commitment[0] ^= 1;
        fs::write(
            ack_path(dir.path(), id),
            serde_json::to_vec(&changed_receipt).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            CertificateOutbox::open(dir.path()),
            Err(CertificateOutboxError::WrongReceipt(found)) if found == id
        ));
    }

    #[test]
    fn poisoned_outbox_recovers_published_compaction_before_enrolling_again() {
        let dir = tempfile::tempdir().unwrap();
        let packet = certificate();
        let mut outbox = CertificateOutbox::open(dir.path()).unwrap();
        let id = outbox
            .ensure_enqueued("peer:1234".into(), packet.clone())
            .unwrap();
        let receipt = CertificateReceipt {
            commitment: certificate_commitment(&packet).unwrap(),
            already_present: false,
        };
        outbox.record_receipt(id, receipt.clone()).unwrap();
        let compact = CompactSendRecord {
            version: 2,
            id,
            peer_addr: "peer:1234".into(),
            subject: packet.operation.operation.subject.clone(),
            commitment: receipt.commitment,
            receipt,
        };
        atomic_replace(
            dir.path(),
            &send_path(dir.path(), id),
            &serde_json::to_vec(&compact).unwrap(),
        )
        .unwrap();
        // The replacement reached disk, but the in-memory entry is stale.
        outbox.poisoned = true;
        assert_eq!(
            outbox
                .ensure_enqueued("peer:1234".into(), packet.clone())
                .unwrap(),
            id
        );
        assert_eq!(outbox.pending().count(), 0);
        assert!(outbox.has_enqueued("peer:1234", &packet).unwrap());
    }

    #[test]
    fn poisoned_outbox_recovers_published_send_before_reusing_id() {
        let root = tempfile::tempdir().unwrap();
        let original = certificate();
        let mut outbox = CertificateOutbox::open(root.path()).unwrap();
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

    #[tokio::test]
    async fn retry_worker_resends_recovered_certificate() {
        use axum::{Json, Router, routing::post};
        let dir = tempfile::tempdir().unwrap();
        let packet = certificate();
        let commitment = certificate_commitment(&packet).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    CERTIFICATE_RECEIVE_PATH,
                    post(move || async move {
                        Json(CertificateReceipt {
                            commitment,
                            already_present: false,
                        })
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let id = CertificateOutbox::open(dir.path())
            .unwrap()
            .ensure_enqueued(addr, packet)
            .unwrap();
        let outbox = Arc::new(tokio::sync::Mutex::new(
            CertificateOutbox::open(dir.path()).unwrap(),
        ));
        let (tx, rx) = tokio::sync::watch::channel(false);
        let worker = tokio::spawn(run_retry_loop(
            Arc::clone(&outbox),
            "secret".into(),
            None,
            rx,
            Duration::from_millis(20),
        ));
        tokio::time::timeout(Duration::from_secs(3), async {
            while outbox.lock().await.get(id).unwrap().receipt.is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tx.send(true).unwrap();
        worker.await.unwrap();
        server.abort();
        drop(outbox);
        assert_eq!(
            CertificateOutbox::open(dir.path())
                .unwrap()
                .pending()
                .count(),
            0
        );
    }
}
