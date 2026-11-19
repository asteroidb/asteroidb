//! Operation-body transport and durable receiver inbox.
//!
//! A receipt confirms that the exact signed packet was durably stored at the
//! receiver; it does not certify the operation.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Json;
use axum::extract::{DefaultBodyLimit, Path as RoutePath, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Router, middleware};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::api::operation_gc::{deleted_value_candidates, dominated_value_candidates};
use crate::api::operation_view::{
    CertifiedOperationRead, CertifiedReadResult, OperationRead, certified_read_with_known_mode,
    ordinary_read,
};
use crate::authority::certificate::KeysetRegistry;
use crate::authority::creation_policy::CreationPolicyManifest;
use crate::authority::operation_evidence::ensure_certificate_capacity;
use crate::authority::operation_merkle::OperationScope;
use crate::authority::operation_merkle::VerifiedOperation;
use crate::authority::writer_auth::{
    CompactSignedCreation, SignedBoundOperation, WriterRegistry, check_compact_creation_witness,
    check_local_create_conflict,
};
use crate::control_plane::system_namespace::{OperationAssignment, SystemNamespace};
use crate::http::auth::require_bearer_token;
use crate::network::operation_certificate_transport::{
    CertifiedOperationResponse, MultiOperationCertificateState, OperationCertificateState,
    non_owner_certified_result,
};
use crate::network::operation_link_auth::{
    OperationLinkAuth, RECEIVER as LINK_RECEIVER, REQUEST_DIGEST as LINK_REQUEST_DIGEST,
    SENDER as LINK_SENDER, SIGNATURE as LINK_SIGNATURE,
};
use crate::network::operation_neighbors::{OperationNeighborManifest, OperationOwnedPrefixes};
use crate::network::operation_outbox::{OperationOutbox, OutboxEntry, OutboxError};
use crate::network::operation_policy_transport::{POLICY_WITNESS_PATH, PolicyWitnessTransfer};
use crate::network::operation_routes::{OperationRequiredDestinations, OperationRouteManifest};
use crate::operation::{
    BoundOperation, CandidateKvAction, CertificationMode, CrdtSubject, OperationLedger,
    PersistOperationError, RecordOutcome,
};
use crate::types::NodeId;

pub const OPERATION_RECEIVE_PATH: &str = "/api/internal/operations/receive";
pub const OPERATION_TRANSFER_PATH: &str = "/api/internal/operations/transfer";
pub const OPERATION_COMPLETION_PATH: &str = "/api/internal/operations/completion";
pub const OPERATION_RETIREMENT_PATH: &str = "/api/internal/operations/retirement";
pub const OPERATION_READ_PATH: &str = "/api/internal/operations/read/{namespace_id}/{*key}";
pub const PUBLIC_OPERATION_READ_PATH: &str = "/api/operations/read/{namespace_id}/{*key}";
pub const PUBLIC_OPERATION_VIEW_PATH: &str = "/api/operations/view/{namespace_id}/{*key}";
pub const OPERATION_SUBMIT_PATH: &str = "/api/operations/submit";
/// Routed envelopes add transfer or completion metadata to a packet admitted
/// through the 2 MiB public submit limit. Only authenticated routed routes
/// need this larger limit; legacy direct receive routes retain the 2 MiB cap.
pub(crate) const INTERNAL_OPERATION_BODY_LIMIT: usize = 8 * 1024 * 1024;
const DIRECT_OPERATION_BODY_LIMIT: usize = 2 * 1024 * 1024;
const INBOX_LOCK: &str = "operation-inbox.lock";
const MOUNTED_AUTHORITY_POLICY: &str = ".mounted-authority-policy.json";
static PACKET_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MountedAuthorityPolicySnapshot {
    version: u8,
    namespace: SystemNamespace,
    assignments: Vec<OperationAssignment>,
}

fn mounted_authority_policy_bytes(namespace: &SystemNamespace) -> io::Result<Vec<u8>> {
    let mut assignments = namespace
        .all_operation_assignments()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    assignments.sort_by(|left, right| {
        (&left.namespace_id, &left.policy.key_range.prefix)
            .cmp(&(&right.namespace_id, &right.policy.key_range.prefix))
    });
    let mut value = serde_json::to_value(MountedAuthorityPolicySnapshot {
        version: 1,
        namespace: namespace.clone(),
        assignments,
    })
    .map_err(|error| invalid_packet_data(&error.to_string()))?;
    canonicalize_policy_tags(&mut value);
    serde_json::to_vec(&value).map_err(|error| invalid_packet_data(&error.to_string()))
}

fn canonicalize_policy_tags(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            for (key, field) in fields {
                if (key == "required_tags" || key == "forbidden_tags")
                    && let serde_json::Value::Array(tags) = field
                {
                    tags.sort_by_key(|tag| tag.to_string());
                } else {
                    canonicalize_policy_tags(field);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                canonicalize_policy_tags(item);
            }
        }
        _ => {}
    }
}

fn invalid_packet_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_owned())
}

fn packet_path(dir: &Path, subject: &CrdtSubject) -> PathBuf {
    let mut digest = Sha256::new();
    digest.update(b"asteroidb/operation-packet-path/v1");
    for bytes in [
        subject.namespace_id.as_bytes(),
        subject.event_dot.writer.0.as_bytes(),
    ] {
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    digest.update(subject.event_dot.counter.to_be_bytes());
    dir.join(format!("{}.json", hex::encode(digest.finalize())))
}

fn write_packet_file(
    dir: &Path,
    subject: &CrdtSubject,
    packet: &OperationPacket,
) -> io::Result<()> {
    use std::io::Write;

    let path = packet_path(dir, subject);
    let bytes = serde_json::to_vec(packet).map_err(|e| invalid_packet_data(&e.to_string()))?;
    if path.exists() {
        let existing = std::fs::read(&path)?;
        if existing != bytes {
            return Err(invalid_packet_data(
                "conflicting signed packet already on disk",
            ));
        }
        std::fs::File::open(&path)?.sync_all()?;
        crate::store::backend::fsync_dir(dir)?;
        return Ok(());
    }
    let tmp = dir.join(format!(
        ".packet-{}-{}.tmp",
        std::process::id(),
        PACKET_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed),
    ));
    let result = (|| -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        crate::store::backend::fsync_dir(dir)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn transfer_path(dir: &Path, sender: &NodeId, transfer_id: u64) -> PathBuf {
    let mut digest = Sha256::new();
    digest.update(b"asteroidb/operation-transfer-path/v1");
    digest.update((sender.0.len() as u64).to_be_bytes());
    digest.update(sender.0.as_bytes());
    digest.update(transfer_id.to_be_bytes());
    dir.join(format!("{}.json", hex::encode(digest.finalize())))
}

fn retirement_path(dir: &Path, sender: &NodeId) -> PathBuf {
    let mut digest = Sha256::new();
    digest.update(b"asteroidb/operation-retirement-sender/v1");
    digest.update((sender.0.len() as u64).to_be_bytes());
    digest.update(sender.0.as_bytes());
    dir.join(format!("{}.json", hex::encode(digest.finalize())))
}

fn replace_file(dir: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let tmp = dir.join(format!(
        ".packet-{}-{}.tmp",
        std::process::id(),
        PACKET_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)?;
        crate::store::backend::fsync_dir(dir)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn write_exact_file(dir: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    if path.exists() {
        if std::fs::read(path)? != bytes {
            return Err(invalid_packet_data(
                "transfer ID rebound to different packet or receiver",
            ));
        }
        std::fs::File::open(path)?.sync_all()?;
        return crate::store::backend::fsync_dir(dir);
    }
    let tmp = dir.join(format!(
        ".packet-{}-{}.tmp",
        std::process::id(),
        PACKET_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)?;
        crate::store::backend::fsync_dir(dir)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// A single immutable operation body. The receiver never serializes a merged
/// CRDT value in its place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationPacket {
    pub signed: SignedBoundOperation,
}

impl OperationPacket {
    /// Binds an ACK to the complete packet, including writer and creator
    /// signatures. Version this encoding before changing its fields.
    pub fn commitment(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        let proof = &self.signed.creation;
        digest.update(b"asteroidb/operation-packet/v3");
        let bytes = self.signed.bound.canonical_bytes();
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
        digest.update(self.signed.operation_signature);
        digest.update(proof.version.to_be_bytes());
        let declaration = serde_json::to_vec(&proof.declaration).expect("declaration serializes");
        digest.update((declaration.len() as u64).to_be_bytes());
        digest.update(declaration);
        digest.update(proof.create_commitment);
        digest.update(proof.creation_policy_digest);
        digest.update(proof.signature);
        digest.finalize().into()
    }
}

/// Value-free identity of a policy-only Create after its full signed packet
/// has been authenticated. The declaration and commitments remain bound to
/// the originally acknowledged packet when its payload is reclaimed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyWitnessIdentity {
    pub subject: CrdtSubject,
    pub declaration: crate::operation::Declaration,
    pub bound_commitment: [u8; 32],
    pub packet_commitment: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyWitnessManifest {
    version: u32,
    witnesses: Vec<PolicyWitnessIdentity>,
}

fn policy_witness_manifest_path(dir: &Path) -> PathBuf {
    dir.join("policy-witness-manifest.json")
}

fn read_policy_witness_manifest(
    dir: &Path,
) -> io::Result<BTreeMap<(String, String), PolicyWitnessIdentity>> {
    let bytes = std::fs::read(policy_witness_manifest_path(dir))?;
    let manifest: PolicyWitnessManifest =
        serde_json::from_slice(&bytes).map_err(|error| invalid_packet_data(&error.to_string()))?;
    if manifest.version != 1 {
        return Err(invalid_packet_data(
            "invalid policy witness manifest version",
        ));
    }
    let mut identities = BTreeMap::new();
    for identity in manifest.witnesses {
        if identity.declaration.creation_id != identity.subject
            || identities
                .insert(
                    (
                        identity.subject.namespace_id.clone(),
                        identity.declaration.key.clone(),
                    ),
                    identity,
                )
                .is_some()
        {
            return Err(invalid_packet_data("invalid policy witness manifest entry"));
        }
    }
    Ok(identities)
}

fn write_policy_witness_manifest(
    dir: &Path,
    identities: &BTreeMap<(String, String), PolicyWitnessIdentity>,
) -> io::Result<()> {
    let bytes = serde_json::to_vec(&PolicyWitnessManifest {
        version: 1,
        witnesses: identities.values().cloned().collect(),
    })
    .map_err(|error| invalid_packet_data(&error.to_string()))?;
    replace_file(dir, &policy_witness_manifest_path(dir), &bytes)
}

impl From<&OperationPacket> for PolicyWitnessIdentity {
    fn from(packet: &OperationPacket) -> Self {
        Self {
            subject: packet.signed.bound.operation.subject.clone(),
            declaration: packet.signed.bound.declaration.clone(),
            bound_commitment: packet.signed.bound.commitment(),
            packet_commitment: packet.commitment(),
        }
    }
}

/// The receiver sends this only after the inbox record has been fsynced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationReceipt {
    pub commitment: [u8; 32],
    pub already_present: bool,
}

/// A single sender-scoped custody handoff. The receiver records this exact
/// identity before acknowledging it; the signed operation packet is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationTransfer {
    pub version: u32,
    pub sender: NodeId,
    pub transfer_id: u64,
    pub receiver: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<TransferId>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub depth: u64,
    pub packet: OperationPacket,
}

/// Historical transfer identity. A sender retirement fence proves that this
/// receive body cannot be needed for forwarding or a fresh custody ACK. The
/// two commitments preserve the packet choice and its ledger receipt without
/// retaining either operation value or signature bytes here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CompactInboundTransfer {
    version: u32,
    sender: NodeId,
    transfer_id: u64,
    receiver: NodeId,
    destination: Option<NodeId>,
    parent: Option<TransferId>,
    depth: u64,
    subject: CrdtSubject,
    packet_commitment: [u8; 32],
    bound_commitment: [u8; 32],
}

impl From<&OperationTransfer> for CompactInboundTransfer {
    fn from(transfer: &OperationTransfer) -> Self {
        Self {
            version: transfer.version,
            sender: transfer.sender.clone(),
            transfer_id: transfer.transfer_id,
            receiver: transfer.receiver.clone(),
            destination: transfer.destination.clone(),
            parent: transfer.parent.clone(),
            depth: transfer.depth,
            subject: transfer.packet.signed.bound.operation.subject.clone(),
            packet_commitment: transfer.packet.commitment(),
            bound_commitment: transfer.packet.signed.bound.commitment(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
enum InboundCatalogRecord {
    Full(OperationTransfer),
    Compact(CompactInboundTransfer),
}

impl InboundCatalogRecord {
    fn identity(&self) -> CompactInboundTransfer {
        match self {
            Self::Full(transfer) => transfer.into(),
            Self::Compact(descriptor) => descriptor.clone(),
        }
    }
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferId {
    pub sender: NodeId,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationTransferReceipt {
    pub version: u32,
    pub sender: NodeId,
    pub transfer_id: u64,
    pub receiver: NodeId,
    pub commitment: [u8; 32],
    pub already_present: bool,
}

/// A receiver's assertion that this exact transfer reached terminal intake.
/// It travels only back to the transfer's original sender.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationCompletionReport {
    pub version: u32,
    pub transfer: OperationTransfer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationCompletionReceipt {
    pub version: u32,
    pub transfer_id: TransferId,
    pub commitment: [u8; 32],
}

/// A sender's durable contiguous closed prefix, addressed to one original
/// adjacent receiver. This is a custody control message, not a packet ACK.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationRetirementNotice {
    pub version: u32,
    pub sender: NodeId,
    pub receiver: NodeId,
    pub closed_before: u64,
}

/// Receiver inbox whose complete history must survive ordinary store
/// checkpoint pruning. The ledger keeps each exact body, including deletes.
pub struct OperationInbox {
    _lock: std::fs::File,
    ledger: OperationLedger,
    packet_dir: PathBuf,
    custody_dir: PathBuf,
    direct_custody_dir: PathBuf,
    custody_receipt_dir: PathBuf,
    transfer_dir: PathBuf,
    catalog_dir: PathBuf,
    fence_dir: PathBuf,
    root_dir: PathBuf,
    root_tombstone_dir: PathBuf,
    known_policy_dir: PathBuf,
    policy_witness_dir: PathBuf,
    terminal_dir: PathBuf,
    signing_dir: PathBuf,
    report_ack_dir: PathBuf,
    packets: BTreeMap<CrdtSubject, OperationPacket>,
    compacted_packets: BTreeMap<CrdtSubject, ([u8; 32], [u8; 32])>,
    custody_receipts: BTreeMap<CrdtSubject, crate::operation::OperationReceipt>,
    known_policy: BTreeMap<CrdtSubject, [u8; 32]>,
    policy_witnesses: BTreeMap<(String, String), OperationPacket>,
    compacted_policy_witnesses: BTreeMap<(String, String), CompactedPolicyWitness>,
    root_tombstones: BTreeMap<CrdtSubject, RootTombstone>,
    /// Rebuilt from a verified local Authority root log at startup and after
    /// each signing pass. A root packet can be released only with this exact
    /// durable-issuance witness; the map itself is never persisted.
    verified_local_issuance: BTreeMap<CrdtSubject, [u8; 32]>,
    admission: Arc<dyn OperationAdmission>,
    fences: BTreeMap<String, u64>,
    policy_publishing: Option<Arc<OperationRequiredDestinations>>,
    poisoned: bool,
}

/// Permanent exact origin identity. The payload can disappear only after
/// every required routed send has a durable completion and the ledger has an
/// independently durable certified receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RootTombstone {
    version: u32,
    subject: CrdtSubject,
    key: String,
    mode: CertificationMode,
    packet_commitment: [u8; 32],
    bound_commitment: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct KnownCreationPolicy {
    version: u32,
    subject: CrdtSubject,
    bound_commitment: [u8; 32],
    creation_commitment: [u8; 32],
}

/// Persisted only after a complete signed Create has passed admission. The
/// creator signature can be reverified without the Create value at startup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactedPolicyWitness {
    version: u32,
    identity: PolicyWitnessIdentity,
    creation: CompactSignedCreation,
    writer_trust_digest: [u8; 32],
}

/// Constructed only from an inbox record that was fully authenticated before
/// compaction and reverified under its pinned trust inputs on recovery.
#[derive(Debug, Clone)]
pub struct VerifiedCompactPolicyWitness {
    identity: PolicyWitnessIdentity,
    creation: CompactSignedCreation,
}

impl VerifiedCompactPolicyWitness {
    pub fn identity(&self) -> &PolicyWitnessIdentity {
        &self.identity
    }

    pub fn creation(&self) -> &CompactSignedCreation {
        &self.creation
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PolicyWitnessDisk {
    Full(OperationPacket),
    Compact(CompactedPolicyWitness),
}

fn compacted_policy_matches_bound(
    witness: &CompactedPolicyWitness,
    bound: &BoundOperation,
) -> bool {
    if bound.operation.subject.namespace_id != witness.identity.subject.namespace_id
        || bound.operation.action.key() != witness.identity.declaration.key
    {
        return true;
    }
    bound.declaration == witness.identity.declaration
        && (!matches!(bound.operation.action, CandidateKvAction::Create { .. })
            || (bound.operation.subject == witness.identity.subject
                && bound.commitment() == witness.identity.bound_commitment))
}

fn creation_commitment(packet: &OperationPacket) -> [u8; 32] {
    packet.signed.creation.create_commitment
}

fn same_creation_identity(
    left: &crate::operation::Declaration,
    right: &crate::operation::Declaration,
) -> bool {
    left.creation_id == right.creation_id
        || (left.creation_id.namespace_id == right.creation_id.namespace_id
            && left.key == right.key)
}

fn policy_witness_key(packet: &OperationPacket) -> (String, String) {
    (
        packet.signed.bound.operation.subject.namespace_id.clone(),
        packet.signed.bound.declaration.key.clone(),
    )
}

fn policy_witness_matches_bound(witness: &OperationPacket, bound: &BoundOperation) -> bool {
    if bound.operation.subject.namespace_id != witness.signed.bound.operation.subject.namespace_id
        || bound.operation.action.key() != witness.signed.bound.declaration.key
    {
        return true;
    }
    bound.declaration == witness.signed.bound.declaration
        && (!matches!(bound.operation.action, CandidateKvAction::Create { .. })
            || bound == &witness.signed.bound)
}

fn policy_witness_matches_packet(witness: &OperationPacket, packet: &OperationPacket) -> bool {
    policy_witness_matches_bound(witness, &packet.signed.bound)
        && (policy_witness_key(witness) != policy_witness_key(packet)
            || packet.signed.creation.matches_create(&witness.signed.bound))
}

/// Authenticate the writer and creation binding, authorize the action and
/// check placement before a receiver stores a packet. Implementations must
/// use trusted local state; the shared HTTP token alone is not proof of an
/// operation's writer or creation event.
pub trait OperationAdmission: Send + Sync {
    fn admit(&self, signed: &SignedBoundOperation) -> Result<(), String>;

    /// Reverify a value-free Create marker under the same pinned trust inputs.
    /// Implementations without a cryptographic writer registry cannot compact
    /// policy witnesses.
    fn compact_creation_trust(&self, _proof: &CompactSignedCreation) -> Result<[u8; 32], String> {
        Err("compact policy witness verification is unavailable".into())
    }
}

/// Admission based on trusted writer keys and namespace grants. A node may
/// retain a signed operation even when it is only a relay; application
/// placement and Authority signing remain separate checks.
pub struct AuthenticatedOperationAdmission {
    registry: Arc<WriterRegistry>,
    namespace_ids: BTreeSet<String>,
    creation_policy: Option<Arc<CreationPolicyManifest>>,
    certificate_capacity_scopes: Vec<(OperationScope, KeysetRegistry)>,
}

impl AuthenticatedOperationAdmission {
    pub fn new(registry: Arc<WriterRegistry>, namespace_id: String) -> Self {
        Self {
            registry,
            namespace_ids: BTreeSet::from([namespace_id]),
            creation_policy: None,
            certificate_capacity_scopes: Vec::new(),
        }
    }

    pub fn new_for_namespaces(
        registry: Arc<WriterRegistry>,
        namespace_ids: BTreeSet<String>,
    ) -> Self {
        Self {
            registry,
            namespace_ids,
            creation_policy: None,
            certificate_capacity_scopes: Vec::new(),
        }
    }

    pub fn new_with_creation_policy(
        registry: Arc<WriterRegistry>,
        namespace_id: String,
        policy: Arc<CreationPolicyManifest>,
    ) -> Self {
        Self {
            registry,
            namespace_ids: BTreeSet::from([namespace_id]),
            creation_policy: Some(policy),
            certificate_capacity_scopes: Vec::new(),
        }
    }

    pub fn new_for_namespaces_with_creation_policy(
        registry: Arc<WriterRegistry>,
        namespace_ids: BTreeSet<String>,
        policy: Arc<CreationPolicyManifest>,
    ) -> Self {
        Self {
            registry,
            namespace_ids,
            creation_policy: Some(policy),
            certificate_capacity_scopes: Vec::new(),
        }
    }

    /// Check the worst-case support and certificate size before a local
    /// submit can return a durable ACK. Scopes use the mounted static keysets.
    pub fn with_certificate_capacity_scopes(
        mut self,
        scopes: Vec<(OperationScope, KeysetRegistry)>,
    ) -> Self {
        self.certificate_capacity_scopes = scopes;
        self
    }
}

impl OperationAdmission for AuthenticatedOperationAdmission {
    fn admit(&self, signed: &SignedBoundOperation) -> Result<(), String> {
        if !self
            .namespace_ids
            .contains(&signed.bound.operation.subject.namespace_id)
        {
            return Err("operation namespace is outside this node's configured scope".into());
        }
        signed.verify(&self.registry).map_err(|e| e.to_string())?;
        if let Some(policy) = &self.creation_policy {
            policy
                .validate_creation(signed)
                .map_err(|e| e.to_string())?;
        }
        if signed.bound.declaration.mode == CertificationMode::Enabled {
            let subject = &signed.bound.operation.subject;
            let key = signed.bound.operation.action.key();
            if let Some((scope, keyset)) = self
                .certificate_capacity_scopes
                .iter()
                .filter(|(scope, _)| {
                    scope.namespace_id == subject.namespace_id && key.starts_with(&scope.key_prefix)
                })
                .max_by_key(|(scope, _)| scope.key_prefix.len())
            {
                ensure_certificate_capacity(
                    &signed.bound,
                    keyset,
                    scope,
                    &keyset.current_version(),
                )?;
            }
        }
        Ok(())
    }

    fn compact_creation_trust(&self, proof: &CompactSignedCreation) -> Result<[u8; 32], String> {
        if !self
            .namespace_ids
            .contains(&proof.declaration.creation_id.namespace_id)
        {
            return Err("Create namespace is outside this node's configured scope".into());
        }
        proof
            .verify(&self.registry)
            .map_err(|error| error.to_string())?;
        if let Some(policy) = &self.creation_policy {
            policy
                .validate_proof(proof)
                .map_err(|error| error.to_string())?;
        }
        let mut digest = Sha256::new();
        digest.update(b"asteroidb/compact-policy-admission/v1");
        digest.update(self.registry.fingerprint());
        digest.update((self.namespace_ids.len() as u64).to_be_bytes());
        for namespace in &self.namespace_ids {
            digest.update((namespace.len() as u64).to_be_bytes());
            digest.update(namespace.as_bytes());
        }
        match &self.creation_policy {
            Some(policy) => {
                digest.update([1]);
                digest.update(policy.digest());
            }
            None => digest.update([0]),
        }
        Ok(digest.finalize().into())
    }
}

#[derive(Debug)]
pub enum ReceiveError {
    Rejected(String),
    Persist(PersistOperationError),
}

impl From<PersistOperationError> for ReceiveError {
    fn from(value: PersistOperationError) -> Self {
        Self::Persist(value)
    }
}

impl OperationInbox {
    /// Read the immutable routing policy before opening operation state.
    /// The snapshot retains operation assignments omitted by the ordinary
    /// SystemNamespace JSON representation.
    pub fn read_mounted_authority_policy_at(dir: &Path) -> io::Result<Option<SystemNamespace>> {
        let path = dir.join(MOUNTED_AUTHORITY_POLICY);
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let snapshot: MountedAuthorityPolicySnapshot = serde_json::from_slice(&bytes)
            .map_err(|error| invalid_packet_data(&error.to_string()))?;
        if snapshot.version != 1 {
            return Err(invalid_packet_data(
                "unsupported mounted Authority policy snapshot",
            ));
        }
        let mut namespace = snapshot.namespace;
        let count = snapshot.assignments.len();
        for assignment in snapshot.assignments {
            namespace
                .set_operation_assignment(assignment)
                .map_err(|error| invalid_packet_data(&error.to_string()))?;
        }
        if namespace.all_operation_assignments().len() != count {
            return Err(invalid_packet_data(
                "duplicate mounted Authority assignment",
            ));
        }
        Ok(Some(namespace))
    }

    /// Fix the policy used for pre-existing routed roots before any local
    /// receipt is returned. Existing roots cannot be reinterpreted under a
    /// later live control-plane snapshot.
    pub fn ensure_mounted_authority_policy(&self, policy: &SystemNamespace) -> io::Result<()> {
        let root = self
            .packet_dir
            .parent()
            .ok_or_else(|| invalid_packet_data("invalid inbox path"))?;
        let path = root.join(MOUNTED_AUTHORITY_POLICY);
        if let Some(saved) = Self::read_mounted_authority_policy_at(root)? {
            let saved: serde_json::Value =
                serde_json::from_slice(&mounted_authority_policy_bytes(&saved)?)
                    .map_err(|error| invalid_packet_data(&error.to_string()))?;
            let requested: serde_json::Value =
                serde_json::from_slice(&mounted_authority_policy_bytes(policy)?)
                    .map_err(|error| invalid_packet_data(&error.to_string()))?;
            if saved != requested {
                return Err(invalid_packet_data("mounted Authority policy changed"));
            }
            std::fs::File::open(path)?.sync_all()?;
            return crate::store::backend::fsync_dir(root);
        }
        if self.ledger.iter().next().is_some()
            || self.ledger.receipts().next().is_some()
            || !self.packets.is_empty()
            || !self.root_tombstones.is_empty()
        {
            return Err(invalid_packet_data(
                "operation inbox has unpinned prior state; refuse Authority policy mount",
            ));
        }
        for dir in [
            &self.packet_dir,
            &self.custody_dir,
            &self.custody_receipt_dir,
            &self.transfer_dir,
            &self.catalog_dir,
            &self.fence_dir,
            &self.root_dir,
            &self.root_tombstone_dir,
            &self.terminal_dir,
            &self.signing_dir,
            &self.report_ack_dir,
        ] {
            if std::fs::read_dir(dir)?.next().is_some() {
                return Err(invalid_packet_data(
                    "operation inbox has unpinned prior state; refuse Authority policy mount",
                ));
            }
        }
        replace_file(root, &path, &mounted_authority_policy_bytes(policy)?)
    }
    fn routed_pin_present(&self) -> io::Result<bool> {
        let root = self
            .packet_dir
            .parent()
            .ok_or_else(|| invalid_packet_data("invalid inbox path"))?;
        root.join(".authority-terminal-manifest").try_exists()
    }

    fn require_certain(&self) -> io::Result<()> {
        if self.poisoned {
            Err(invalid_packet_data(
                "operation inbox must be reopened after uncertain write",
            ))
        } else {
            Ok(())
        }
    }

    /// Pin the exact immutable range defaults and override grants before any
    /// operation enters the inbox. An existing unpinned inbox cannot be
    /// reinterpreted under newly supplied creation policy.
    pub fn verify_creation_policy_manifest(&self, digest: [u8; 32]) -> io::Result<()> {
        let root = self
            .packet_dir
            .parent()
            .ok_or_else(|| invalid_packet_data("invalid inbox path"))?;
        let path = root.join(".creation-policy-manifest");
        if path.exists() {
            if std::fs::read(&path)? != digest {
                return Err(invalid_packet_data("operation creation policy changed"));
            }
            return Ok(());
        }
        if self.ledger.iter().next().is_some()
            || self.ledger.receipts().next().is_some()
            || !self.packets.is_empty()
            || !self.policy_witnesses.is_empty()
            || !self.compacted_policy_witnesses.is_empty()
        {
            return Err(invalid_packet_data(
                "operation inbox has unpinned prior state; refuse creation policy migration",
            ));
        }
        for dir in [
            &self.packet_dir,
            &self.custody_dir,
            &self.custody_receipt_dir,
            &self.transfer_dir,
            &self.catalog_dir,
            &self.fence_dir,
            &self.root_dir,
            &self.root_tombstone_dir,
            &self.policy_witness_dir,
            &self.terminal_dir,
            &self.signing_dir,
            &self.report_ack_dir,
        ] {
            if std::fs::read_dir(dir)?.next().is_some() {
                return Err(invalid_packet_data(
                    "operation inbox has unpinned prior state; refuse creation policy migration",
                ));
            }
        }
        Ok(())
    }

    pub fn ensure_creation_policy_manifest(&self, digest: [u8; 32]) -> io::Result<()> {
        self.verify_creation_policy_manifest(digest)?;
        let root = self
            .packet_dir
            .parent()
            .ok_or_else(|| invalid_packet_data("invalid inbox path"))?;
        let path = root.join(".creation-policy-manifest");
        if path.exists() {
            std::fs::File::open(&path)?.sync_all()?;
            crate::store::backend::fsync_dir(root)
        } else {
            // A crash before rename leaves only an uncommitted temp.
            replace_file(root, &path, &digest)
        }
    }

    /// A deployment cannot silently remove a policy after accepting any
    /// creations whose mode was checked against it.
    pub fn ensure_no_creation_policy_manifest(&self) -> io::Result<()> {
        let root = self
            .packet_dir
            .parent()
            .ok_or_else(|| invalid_packet_data("invalid inbox path"))?;
        if root.join(".creation-policy-manifest").exists() {
            return Err(invalid_packet_data("operation creation policy was removed"));
        }
        Ok(())
    }

    /// Check an existing ownership pin, or that an unpinned inbox is empty.
    pub fn verify_owned_manifest(&self, digest: [u8; 32]) -> io::Result<()> {
        let root = self
            .packet_dir
            .parent()
            .ok_or_else(|| invalid_packet_data("invalid inbox path"))?;
        let path = root.join(".owned-prefixes-manifest");
        if path.exists() {
            if std::fs::read(&path)? != digest {
                return Err(invalid_packet_data("operation owned prefixes changed"));
            }
            return Ok(());
        }
        if self.ledger.iter().next().is_some()
            || self.ledger.receipts().next().is_some()
            || !self.packets.is_empty()
            || !self.root_tombstones.is_empty()
        {
            return Err(invalid_packet_data(
                "operation inbox has unpinned prior state; refuse owned-prefix migration",
            ));
        }
        for dir in [
            &self.packet_dir,
            &self.custody_dir,
            &self.custody_receipt_dir,
            &self.transfer_dir,
            &self.catalog_dir,
            &self.fence_dir,
            &self.root_dir,
            &self.root_tombstone_dir,
            &self.terminal_dir,
            &self.signing_dir,
            &self.report_ack_dir,
        ] {
            if std::fs::read_dir(dir)?.next().is_some() {
                return Err(invalid_packet_data(
                    "operation inbox has unpinned prior state; refuse owned-prefix migration",
                ));
            }
        }
        Ok(())
    }

    /// A held operation must not become application data merely because the
    /// node is reopened with a different static ownership assignment.
    pub fn ensure_owned_manifest(&self, digest: [u8; 32]) -> io::Result<()> {
        self.verify_owned_manifest(digest)?;
        let root = self
            .packet_dir
            .parent()
            .ok_or_else(|| invalid_packet_data("invalid inbox path"))?;
        let path = root.join(".owned-prefixes-manifest");
        if path.exists() {
            std::fs::File::open(&path)?.sync_all()?;
            crate::store::backend::fsync_dir(root)
        } else {
            replace_file(root, &path, &digest)
        }
    }

    /// Check both an existing pin and whether an unpinned inbox can adopt one.
    pub fn verify_authority_manifest(&self, digest: [u8; 32]) -> io::Result<()> {
        let root = self
            .packet_dir
            .parent()
            .ok_or_else(|| invalid_packet_data("invalid inbox path"))?;
        let path = root.join(".authority-terminal-manifest");
        if path.exists() {
            if std::fs::read(&path)? != digest {
                return Err(invalid_packet_data(
                    "operation Authority terminal assignment changed",
                ));
            }
            return Ok(());
        }
        if self.ledger.iter().next().is_some()
            || self.ledger.receipts().next().is_some()
            || !self.packets.is_empty()
            || !self.root_tombstones.is_empty()
        {
            return Err(invalid_packet_data(
                "operation inbox has unpinned prior state; refuse Authority assignment migration",
            ));
        }
        for dir in [
            &self.packet_dir,
            &self.custody_dir,
            &self.custody_receipt_dir,
            &self.transfer_dir,
            &self.catalog_dir,
            &self.fence_dir,
            &self.root_dir,
            &self.root_tombstone_dir,
            &self.terminal_dir,
            &self.signing_dir,
            &self.report_ack_dir,
        ] {
            if std::fs::read_dir(dir)?.next().is_some() {
                return Err(invalid_packet_data(
                    "operation inbox has unpinned prior state; refuse Authority assignment migration",
                ));
            }
        }
        Ok(())
    }

    /// Authority intake roles are part of the fixed routed topology. An
    /// existing inbox without this pin may only adopt them while still empty.
    pub fn ensure_authority_manifest(&self, digest: [u8; 32]) -> io::Result<()> {
        self.verify_authority_manifest(digest)?;
        let root = self
            .packet_dir
            .parent()
            .ok_or_else(|| invalid_packet_data("invalid inbox path"))?;
        let path = root.join(".authority-terminal-manifest");
        if path.exists() {
            std::fs::File::open(&path)?.sync_all()?;
            crate::store::backend::fsync_dir(root)
        } else {
            replace_file(root, &path, &digest)
        }
    }

    /// An unpinned inbox may contain routed transfers admitted using only the
    /// old shared token. Refuse migration when any custody state exists.
    pub fn verify_link_manifest(&self, digest: [u8; 32]) -> io::Result<()> {
        let root = self
            .packet_dir
            .parent()
            .ok_or_else(|| invalid_packet_data("invalid inbox path"))?;
        let path = root.join(".link-manifest");
        if path.exists() {
            if std::fs::read(&path)? != digest {
                return Err(invalid_packet_data(
                    "operation inbox link identities changed",
                ));
            }
            return Ok(());
        }
        for dir in [
            &self.packet_dir,
            &self.transfer_dir,
            &self.catalog_dir,
            &self.fence_dir,
            &self.root_dir,
            &self.root_tombstone_dir,
            &self.terminal_dir,
            &self.signing_dir,
            &self.report_ack_dir,
        ] {
            if std::fs::read_dir(dir)?.next().is_some() {
                return Err(invalid_packet_data(
                    "operation inbox has unsigned prior custody state; refuse link-auth migration",
                ));
            }
        }
        Ok(())
    }

    pub fn ensure_link_manifest(&self, digest: [u8; 32]) -> io::Result<()> {
        self.verify_link_manifest(digest)?;
        let root = self
            .packet_dir
            .parent()
            .ok_or_else(|| invalid_packet_data("invalid inbox path"))?;
        let path = root.join(".link-manifest");
        if path.exists() {
            std::fs::File::open(&path)?.sync_all()?;
            return crate::store::backend::fsync_dir(root);
        }
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)?;
        use std::io::Write;
        file.write_all(&digest)?;
        file.sync_all()?;
        crate::store::backend::fsync_dir(root)
    }

    pub fn verify_transfer_receiver(&self, self_id: &NodeId) -> io::Result<()> {
        for dir in [&self.transfer_dir, &self.catalog_dir] {
            for entry in std::fs::read_dir(dir)? {
                let path = entry?.path();
                let transfer: InboundCatalogRecord = serde_json::from_slice(&std::fs::read(path)?)
                    .map_err(|e| invalid_packet_data(&e.to_string()))?;
                if transfer.identity().receiver != *self_id {
                    return Err(invalid_packet_data(
                        "persisted transfer belongs to another receiver",
                    ));
                }
            }
        }
        for entry in std::fs::read_dir(&self.fence_dir)? {
            let notice: OperationRetirementNotice =
                serde_json::from_slice(&std::fs::read(entry?.path())?)
                    .map_err(|e| invalid_packet_data(&e.to_string()))?;
            if notice.receiver != *self_id {
                return Err(invalid_packet_data(
                    "saved retirement fence belongs to another receiver",
                ));
            }
        }
        Ok(())
    }

    pub fn open(
        dir: impl AsRef<Path>,
        admission: Arc<dyn OperationAdmission>,
    ) -> Result<Self, PersistOperationError> {
        let dir = dir.as_ref();
        crate::store::backend::create_dir_all_durable(dir)?;
        let lock_path = dir.join(INBOX_LOCK);
        let created = !lock_path.exists();
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        lock.try_lock()
            .map_err(|error| io::Error::new(io::ErrorKind::WouldBlock, error.to_string()))?;
        if created {
            lock.sync_all()?;
            crate::store::backend::fsync_dir(dir)?;
        }
        if !policy_witness_manifest_path(dir).exists() {
            // A crash can interrupt the first open after the lock directory
            // entry is durable but before the empty manifest is written. At
            // that point no inbox state has been acknowledged, so finish the
            // initialization. Any other saved entry could contain a learned
            // policy and must keep the missing-manifest failure closed.
            let mut unfinished_temps = Vec::new();
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let name = entry.file_name();
                if name == INBOX_LOCK {
                    continue;
                }
                if name
                    .to_str()
                    .is_some_and(|name| name.starts_with(".packet-") && name.ends_with(".tmp"))
                {
                    unfinished_temps.push(entry.path());
                    continue;
                }
                return Err(invalid_packet_data("missing policy witness manifest").into());
            }
            for path in unfinished_temps {
                std::fs::remove_file(path)?;
            }
            crate::store::backend::fsync_dir(dir)?;
            write_policy_witness_manifest(dir, &BTreeMap::new())?;
        }
        Self::load_locked(dir, admission, lock, None)
    }

    /// Rebuild after an uncertain write without releasing the directory
    /// lease. The cloned handle keeps other processes out throughout replay.
    pub fn recover_if_poisoned(&mut self) -> Result<(), PersistOperationError> {
        if !self.poisoned {
            return Ok(());
        }
        let dir = self
            .packet_dir
            .parent()
            .ok_or_else(|| invalid_packet_data("invalid inbox path"))?
            .to_path_buf();
        let lock = self._lock.try_clone()?;
        let ledger = self.ledger.reopen_for_recovery()?;
        let mut recovered =
            Self::load_locked(&dir, Arc::clone(&self.admission), lock, Some(ledger))?;
        recovered.policy_publishing = self.policy_publishing.clone();
        // This proof was obtained from an independently fsynced and verified
        // Authority root log, not from the uncertain inbox write.
        recovered.verified_local_issuance = self.verified_local_issuance.clone();
        *self = recovered;
        Ok(())
    }

    fn load_locked(
        dir: &Path,
        admission: Arc<dyn OperationAdmission>,
        lock: std::fs::File,
        recovering_ledger: Option<OperationLedger>,
    ) -> Result<Self, PersistOperationError> {
        let mut ledger = match recovering_ledger {
            Some(ledger) => ledger,
            None => OperationLedger::open(dir)?,
        };
        let packet_dir = dir.join("packets");
        crate::store::backend::create_dir_all_durable(&packet_dir)?;
        let custody_dir = dir.join("custody-only");
        crate::store::backend::create_dir_all_durable(&custody_dir)?;
        let direct_custody_dir = dir.join("direct-custody");
        crate::store::backend::create_dir_all_durable(&direct_custody_dir)?;
        let custody_receipt_dir = dir.join("custody-receipts");
        crate::store::backend::create_dir_all_durable(&custody_receipt_dir)?;
        let transfer_dir = dir.join("transfers");
        crate::store::backend::create_dir_all_durable(&transfer_dir)?;
        let catalog_dir = dir.join("transfer-catalog");
        crate::store::backend::create_dir_all_durable(&catalog_dir)?;
        let fence_dir = dir.join("retirement-fences");
        crate::store::backend::create_dir_all_durable(&fence_dir)?;
        let root_dir = dir.join("roots");
        crate::store::backend::create_dir_all_durable(&root_dir)?;
        let root_tombstone_dir = dir.join("root-tombstones");
        crate::store::backend::create_dir_all_durable(&root_tombstone_dir)?;
        let known_policy_dir = dir.join("known-creation-policy");
        crate::store::backend::create_dir_all_durable(&known_policy_dir)?;
        let policy_witness_dir = dir.join("policy-witness");
        crate::store::backend::create_dir_all_durable(&policy_witness_dir)?;
        let mut root_tombstones = BTreeMap::new();
        for entry in std::fs::read_dir(&root_tombstone_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".packet-"))
            {
                std::fs::remove_file(path)?;
                continue;
            }
            let tombstone: RootTombstone = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|e| invalid_packet_data(&e.to_string()))?;
            if tombstone.version != 1
                || path != packet_path(&root_tombstone_dir, &tombstone.subject)
                || ledger.receipt(&tombstone.subject).is_none_or(|receipt| {
                    receipt.commitment != tombstone.bound_commitment
                        || receipt.declaration.key != tombstone.key
                        || receipt.declaration.mode != tombstone.mode
                })
                || root_tombstones
                    .insert(tombstone.subject.clone(), tombstone)
                    .is_some()
            {
                return Err(invalid_packet_data("invalid reclaimed root identity").into());
            }
        }
        crate::store::backend::fsync_dir(&root_tombstone_dir)?;
        for entry in std::fs::read_dir(&packet_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".packet-") && name.ends_with(".tmp"))
            {
                std::fs::remove_file(path)?;
            }
        }
        crate::store::backend::fsync_dir(&packet_dir)?;
        // A local submit writes its root intent before the shared packet and
        // ledger. Replay that durable origin first: a crash between those
        // writes must not turn the local request into an unowned packet that
        // routed startup cannot enroll for delivery.
        for entry in std::fs::read_dir(&root_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".packet-") && name.ends_with(".tmp"))
            {
                std::fs::remove_file(path)?;
                continue;
            }
            let packet: OperationPacket = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|e| invalid_packet_data(&format!("invalid root submission: {e}")))?;
            let subject = &packet.signed.bound.operation.subject;
            if path != packet_path(&root_dir, subject) || admission.admit(&packet.signed).is_err() {
                return Err(invalid_packet_data("invalid root submission record").into());
            }
            if let Some(tombstone) = root_tombstones.get(subject) {
                if tombstone.packet_commitment != packet.commitment()
                    || tombstone.bound_commitment != packet.signed.bound.commitment()
                    || tombstone.key != packet.signed.bound.operation.action.key()
                    || tombstone.mode != packet.signed.bound.declaration.mode
                {
                    return Err(invalid_packet_data("reclaimed root conflicts with payload").into());
                }
            } else {
                write_packet_file(&packet_dir, subject, &packet)?;
            }
        }
        crate::store::backend::fsync_dir(&root_dir)?;
        let terminal_dir = dir.join("terminal");
        crate::store::backend::create_dir_all_durable(&terminal_dir)?;
        let signing_dir = dir.join("signing-inbox");
        crate::store::backend::create_dir_all_durable(&signing_dir)?;
        let report_ack_dir = dir.join("report-acks");
        crate::store::backend::create_dir_all_durable(&report_ack_dir)?;
        let mut packets = BTreeMap::new();
        for entry in std::fs::read_dir(&packet_dir)? {
            let path = entry?.path();
            // The atomic writer can leave an uncommitted temporary file if
            // power is lost before rename. No receipt was sent for it.
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".packet-") && name.ends_with(".tmp"))
            {
                std::fs::remove_file(&path)?;
                continue;
            }
            if !path.is_file() {
                return Err(invalid_packet_data("non-file entry in operation packet inbox").into());
            }
            let packet: OperationPacket = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|e| invalid_packet_data(&format!("invalid operation packet: {e}")))?;
            let subject = &packet.signed.bound.operation.subject;
            let custody_marker = packet_path(&custody_dir, subject);
            let custody_only = custody_marker.exists();
            if custody_only && std::fs::read(&custody_marker)? != packet.commitment() {
                return Err(
                    invalid_packet_data("custody marker conflicts with signed packet").into(),
                );
            }
            if path != packet_path(&packet_dir, subject)
                || admission.admit(&packet.signed).is_err()
                || packets.contains_key(subject)
            {
                return Err(invalid_packet_data("operation packet inbox recovery mismatch").into());
            }
            if let Some(existing) = ledger.get(subject) {
                if existing != &packet.signed.bound {
                    return Err(
                        invalid_packet_data("operation packet conflicts with ledger").into(),
                    );
                }
            } else if let Some(receipt) = ledger.receipt(subject) {
                if receipt != &crate::operation::OperationReceipt::from(&packet.signed.bound) {
                    return Err(invalid_packet_data(
                        "operation packet conflicts with ledger receipt",
                    )
                    .into());
                }
            } else if !custody_only {
                check_local_create_conflict(&ledger, &packet.signed.bound)
                    .map_err(|e| invalid_packet_data(&e.to_string()))?;
                ledger.record_and_sync(packet.signed.bound.clone())?;
            }
            if custody_only && ledger.get(subject).is_none() && ledger.receipt(subject).is_none() {
                let receipt = crate::operation::OperationReceipt::from(&packet.signed.bound);
                write_exact_file(
                    &custody_receipt_dir,
                    &packet_path(&custody_receipt_dir, subject),
                    &serde_json::to_vec(&receipt)
                        .map_err(|error| invalid_packet_data(&error.to_string()))?,
                )?;
            }
            packets.insert(subject.clone(), packet);
        }
        for entry in std::fs::read_dir(&custody_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".packet-"))
            {
                std::fs::remove_file(path)?;
                continue;
            }
            let still_custody = packets.iter().any(|(subject, packet)| {
                path == packet_path(&custody_dir, subject)
                    && std::fs::read(&path).ok().as_deref() == Some(packet.commitment().as_slice())
                    && ledger.get(subject).is_none()
                    && ledger.receipt(subject).is_none()
            });
            if !still_custody {
                // A marker may precede its packet during a crash, or outlive
                // a successful promotion to the application ledger.
                std::fs::remove_file(path)?;
            }
        }
        crate::store::backend::fsync_dir(&custody_dir)?;
        // The signed packet is the recoverable custody copy. A ledger body
        // without its packet could affect reads but could not be authenticated
        // or forwarded after a crash, so never mount that state.
        for bound in ledger.iter() {
            if packets
                .get(&bound.operation.subject)
                .is_none_or(|packet| packet.signed.bound != *bound)
            {
                return Err(invalid_packet_data("operation ledger has no signed packet").into());
            }
        }
        // A compact catalog descriptor may witness the signed packet after
        // receiver payload collection. Validate receipts against that witness
        // after loading the catalog and sender fences below.
        for entry in std::fs::read_dir(&transfer_dir)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_name().to_string_lossy().starts_with(".packet-") {
                std::fs::remove_file(path)?;
                continue;
            }
            let transfer: OperationTransfer = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|e| invalid_packet_data(&format!("invalid transfer record: {e}")))?;
            if !matches!(transfer.version, 1 | 2)
                || (transfer.version == 1
                    && (transfer.destination.is_some()
                        || transfer.parent.is_some()
                        || transfer.depth != 0))
                || (transfer.version == 2
                    && (transfer.destination.is_none()
                        || transfer.parent.is_none() != (transfer.depth == 0)))
                || path != transfer_path(&transfer_dir, &transfer.sender, transfer.transfer_id)
                || transfer.sender == transfer.receiver
                || packets.get(&transfer.packet.signed.bound.operation.subject)
                    != Some(&transfer.packet)
            {
                return Err(invalid_packet_data("operation transfer recovery mismatch").into());
            }
            // Migrate pre-compaction inboxes before a retirement notice can
            // ever authorize deletion of their active transfer detail.
            write_exact_file(
                &catalog_dir,
                &transfer_path(&catalog_dir, &transfer.sender, transfer.transfer_id),
                &std::fs::read(&path)?,
            )?;
        }
        crate::store::backend::fsync_dir(&transfer_dir)?;
        let mut custody_receipts = BTreeMap::new();
        for entry in std::fs::read_dir(&custody_receipt_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".packet-"))
            {
                std::fs::remove_file(path)?;
                continue;
            }
            let receipt: crate::operation::OperationReceipt =
                serde_json::from_slice(&std::fs::read(&path)?)
                    .map_err(|error| invalid_packet_data(&error.to_string()))?;
            if path != packet_path(&custody_receipt_dir, &receipt.subject)
                || packets.get(&receipt.subject).is_some_and(|packet| {
                    crate::operation::OperationReceipt::from(&packet.signed.bound) != receipt
                })
                || custody_receipts
                    .insert(receipt.subject.clone(), receipt)
                    .is_some()
            {
                return Err(invalid_packet_data("invalid custody receipt").into());
            }
        }
        crate::store::backend::fsync_dir(&custody_receipt_dir)?;
        let mut compacted_packets = BTreeMap::new();
        let mut catalog_subjects = BTreeSet::new();
        for entry in std::fs::read_dir(&catalog_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(".packet-"))
            {
                std::fs::remove_file(path)?;
                continue;
            }
            let record: InboundCatalogRecord = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|e| invalid_packet_data(&e.to_string()))?;
            let descriptor = record.identity();
            catalog_subjects.insert(descriptor.subject.clone());
            if path != transfer_path(&catalog_dir, &descriptor.sender, descriptor.transfer_id)
                || !matches!(descriptor.version, 1 | 2)
                || descriptor.sender == descriptor.receiver
                || (descriptor.version == 1
                    && (descriptor.destination.is_some()
                        || descriptor.parent.is_some()
                        || descriptor.depth != 0))
                || (descriptor.version == 2
                    && (descriptor.destination.is_none()
                        || descriptor.parent.is_none() != (descriptor.depth == 0)))
            {
                return Err(invalid_packet_data("invalid exact transfer catalog").into());
            }
            match record {
                InboundCatalogRecord::Full(transfer) => {
                    if packets.get(&descriptor.subject) != Some(&transfer.packet) {
                        return Err(invalid_packet_data("invalid exact transfer catalog").into());
                    }
                }
                InboundCatalogRecord::Compact(_) => {
                    if ledger
                        .receipt(&descriptor.subject)
                        .or_else(|| custody_receipts.get(&descriptor.subject))
                        .is_none_or(|receipt| receipt.commitment != descriptor.bound_commitment)
                        || packets.get(&descriptor.subject).is_some_and(|packet| {
                            packet.commitment() != descriptor.packet_commitment
                                || packet.signed.bound.commitment() != descriptor.bound_commitment
                        })
                        || compacted_packets
                            .insert(
                                descriptor.subject.clone(),
                                (descriptor.packet_commitment, descriptor.bound_commitment),
                            )
                            .is_some_and(|old| {
                                old != (descriptor.packet_commitment, descriptor.bound_commitment)
                            })
                    {
                        return Err(invalid_packet_data("invalid compact transfer catalog").into());
                    }
                }
            }
        }
        crate::store::backend::fsync_dir(&catalog_dir)?;
        // Direct non-owner gossip has no transfer ID or catalog entry. Its
        // own marker is fsynced before ACK; a transfer whose catalog was not
        // yet saved must still be treated as an unacknowledged orphan.
        let mut direct_subjects = BTreeSet::new();
        for entry in std::fs::read_dir(&direct_custody_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".packet-"))
            {
                std::fs::remove_file(path)?;
                continue;
            }
            let Some((subject, _)) = custody_receipts
                .iter()
                .find(|(subject, _)| path == packet_path(&direct_custody_dir, subject))
            else {
                return Err(invalid_packet_data("direct custody marker lacks a receipt").into());
            };
            if packets.get(subject).is_none_or(|packet| {
                std::fs::read(&path).ok().as_deref() != Some(packet.commitment().as_slice())
            }) || ledger.get(subject).is_some()
                || ledger.receipt(subject).is_some()
                || !direct_subjects.insert(subject.clone())
            {
                return Err(invalid_packet_data("invalid direct custody marker").into());
            }
        }
        crate::store::backend::fsync_dir(&direct_custody_dir)?;
        // A custody packet can be fsynced before its transfer catalog. If the
        // process crashes in that window, no ACK was released and the sender
        // retains its retry copy. Do not retain an orphan indefinitely or let
        // its speculative Create block a later valid one. Delete the packet
        // first: after any interrupted cleanup, the next open can finish the
        // receipt and marker removal without recreating application data.
        let orphan_subjects: Vec<_> = custody_receipts
            .keys()
            .filter(|subject| {
                ledger.get(subject).is_none()
                    && ledger.receipt(subject).is_none()
                    && !packet_path(&root_dir, subject).exists()
                    && !catalog_subjects.contains(*subject)
                    && !direct_subjects.contains(*subject)
            })
            .cloned()
            .collect();
        for subject in orphan_subjects {
            let packet = packet_path(&packet_dir, &subject);
            if packet.exists() {
                std::fs::remove_file(packet)?;
                crate::store::backend::fsync_dir(&packet_dir)?;
                packets.remove(&subject);
            }
            let receipt = packet_path(&custody_receipt_dir, &subject);
            if receipt.exists() {
                std::fs::remove_file(receipt)?;
                crate::store::backend::fsync_dir(&custody_receipt_dir)?;
            }
            custody_receipts.remove(&subject);
            let marker = packet_path(&custody_dir, &subject);
            if marker.exists() {
                std::fs::remove_file(marker)?;
                crate::store::backend::fsync_dir(&custody_dir)?;
            }
        }
        for receipt in custody_receipts.values() {
            if !packets.contains_key(&receipt.subject)
                && compacted_packets
                    .get(&receipt.subject)
                    .is_none_or(|(_, bound)| *bound != receipt.commitment)
            {
                return Err(invalid_packet_data("custody receipt has no signed witness").into());
            }
        }
        for tombstone in root_tombstones.values() {
            let subject = &tombstone.subject;
            if packets.get(subject).is_some_and(|packet| {
                packet.commitment() != tombstone.packet_commitment
                    || packet.signed.bound.commitment() != tombstone.bound_commitment
                    || packet.signed.bound.operation.action.key() != tombstone.key
                    || packet.signed.bound.declaration.mode != tombstone.mode
            }) || compacted_packets.get(subject).is_some_and(|commitments| {
                *commitments != (tombstone.packet_commitment, tombstone.bound_commitment)
            }) {
                return Err(
                    invalid_packet_data("reclaimed root conflicts with signed packet").into(),
                );
            }
            compacted_packets.insert(
                subject.clone(),
                (tombstone.packet_commitment, tombstone.bound_commitment),
            );
        }
        let mut known_policy = BTreeMap::new();
        for entry in std::fs::read_dir(&known_policy_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".packet-"))
            {
                std::fs::remove_file(path)?;
                continue;
            }
            let marker: KnownCreationPolicy = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|error| invalid_packet_data(&error.to_string()))?;
            if marker.version != 2
                || path != packet_path(&known_policy_dir, &marker.subject)
                || ledger
                    .receipt(&marker.subject)
                    .or_else(|| custody_receipts.get(&marker.subject))
                    .is_none_or(|receipt| receipt.commitment != marker.bound_commitment)
                || compacted_packets
                    .get(&marker.subject)
                    .is_none_or(|(_, bound)| *bound != marker.bound_commitment)
                || known_policy
                    .insert(marker.subject, marker.creation_commitment)
                    .is_some()
            {
                return Err(invalid_packet_data("invalid known creation policy marker").into());
            }
        }
        crate::store::backend::fsync_dir(&known_policy_dir)?;
        if compacted_packets
            .keys()
            .any(|subject| !packets.contains_key(subject) && !known_policy.contains_key(subject))
        {
            return Err(invalid_packet_data("reclaimed packet has no Create policy marker").into());
        }
        let mut policy_witnesses = BTreeMap::new();
        let mut compacted_policy_witnesses = BTreeMap::new();
        for entry in std::fs::read_dir(&policy_witness_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".packet-"))
            {
                std::fs::remove_file(path)?;
                continue;
            }
            let record: PolicyWitnessDisk = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|error| invalid_packet_data(&error.to_string()))?;
            match record {
                PolicyWitnessDisk::Full(packet) => {
                    let bound = &packet.signed.bound;
                    let subject = &bound.operation.subject;
                    if path != packet_path(&policy_witness_dir, subject)
                        || !matches!(bound.operation.action, CandidateKvAction::Create { .. })
                        || admission.admit(&packet.signed).is_err()
                        || check_local_create_conflict(&ledger, bound).is_err()
                        || ledger
                            .iter()
                            .any(|held| !policy_witness_matches_bound(&packet, held))
                        || packets
                            .values()
                            .any(|held| !policy_witness_matches_packet(&packet, held))
                        || ledger
                            .receipts()
                            .chain(custody_receipts.values())
                            .any(|receipt| {
                                receipt.subject.namespace_id == subject.namespace_id
                                    && receipt.declaration.key == bound.declaration.key
                                    && (receipt.declaration != bound.declaration
                                        || (receipt.action_kind
                                            == crate::operation::ReceiptActionKind::Create
                                            && receipt.commitment != bound.commitment()))
                            })
                        || policy_witnesses
                            .insert(policy_witness_key(&packet), packet.clone())
                            .is_some()
                        || compacted_policy_witnesses.contains_key(&policy_witness_key(&packet))
                    {
                        return Err(
                            invalid_packet_data("invalid policy-only Create witness").into()
                        );
                    }
                }
                PolicyWitnessDisk::Compact(witness) => {
                    let identity = &witness.identity;
                    let subject = &identity.subject;
                    let key = (
                        subject.namespace_id.clone(),
                        identity.declaration.key.clone(),
                    );
                    if witness.version != 1
                        || path != packet_path(&policy_witness_dir, subject)
                        || identity.declaration.creation_id != *subject
                        || identity.declaration != witness.creation.declaration
                        || identity.bound_commitment != witness.creation.create_commitment
                        || admission.compact_creation_trust(&witness.creation)
                            != Ok(witness.writer_trust_digest)
                        || ledger
                            .iter()
                            .any(|held| !compacted_policy_matches_bound(&witness, held))
                        || packets.values().any(|held| {
                            !compacted_policy_matches_bound(&witness, &held.signed.bound)
                        })
                        || ledger
                            .receipts()
                            .chain(custody_receipts.values())
                            .any(|receipt| {
                                receipt.subject.namespace_id == subject.namespace_id
                                    && receipt.declaration.key == identity.declaration.key
                                    && (receipt.declaration != identity.declaration
                                        || (receipt.action_kind
                                            == crate::operation::ReceiptActionKind::Create
                                            && receipt.commitment != identity.bound_commitment))
                            })
                        || policy_witnesses.contains_key(&key)
                        || compacted_policy_witnesses.insert(key, witness).is_some()
                    {
                        return Err(invalid_packet_data("invalid compact policy witness").into());
                    }
                }
            }
        }
        crate::store::backend::fsync_dir(&policy_witness_dir)?;
        let mut manifest = read_policy_witness_manifest(dir)?;
        for (key, identity) in &manifest {
            let retained = policy_witnesses
                .get(key)
                .map(PolicyWitnessIdentity::from)
                .or_else(|| {
                    compacted_policy_witnesses
                        .get(key)
                        .map(|witness| witness.identity.clone())
                });
            if retained.as_ref() != Some(identity) {
                return Err(invalid_packet_data("policy witness missing or changed").into());
            }
        }
        // A crash between the witness fsync and manifest fsync leaves an
        // unacknowledged extra witness. Retain it and finish its index here.
        let old_len = manifest.len();
        for (key, packet) in &policy_witnesses {
            manifest
                .entry(key.clone())
                .or_insert_with(|| PolicyWitnessIdentity::from(packet));
        }
        for (key, witness) in &compacted_policy_witnesses {
            manifest
                .entry(key.clone())
                .or_insert_with(|| witness.identity.clone());
        }
        if manifest.len() != old_len {
            write_policy_witness_manifest(dir, &manifest)?;
        }
        let mut fences = BTreeMap::new();
        for entry in std::fs::read_dir(&fence_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(".packet-"))
            {
                std::fs::remove_file(path)?;
                continue;
            }
            let notice: OperationRetirementNotice = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|e| invalid_packet_data(&e.to_string()))?;
            if notice.version != 1
                || notice.sender == notice.receiver
                || path != retirement_path(&fence_dir, &notice.sender)
                || fences
                    .insert(notice.sender.0, notice.closed_before)
                    .is_some()
            {
                return Err(invalid_packet_data("invalid saved retirement fence").into());
            }
        }
        crate::store::backend::fsync_dir(&fence_dir)?;
        for receipt in ledger.receipts() {
            if packets.get(&receipt.subject).is_none_or(|packet| {
                crate::operation::OperationReceipt::from(&packet.signed.bound) != *receipt
            }) && compacted_packets
                .get(&receipt.subject)
                .is_none_or(|(_, bound)| *bound != receipt.commitment)
            {
                return Err(invalid_packet_data(
                    "operation ledger receipt has no signed packet witness",
                )
                .into());
            }
        }
        // A crash may leave the catalog durable but occur before the active
        // transfer rename. Reconstruct that work unless a saved fence already
        // proves the sender closed this ID. The catalog itself is never GCed.
        for entry in std::fs::read_dir(&catalog_dir)? {
            let path = entry?.path();
            let bytes = std::fs::read(&path)?;
            let record: InboundCatalogRecord =
                serde_json::from_slice(&bytes).map_err(|e| invalid_packet_data(&e.to_string()))?;
            match record {
                InboundCatalogRecord::Full(transfer) => {
                    if transfer.transfer_id >= *fences.get(&transfer.sender.0).unwrap_or(&0) {
                        write_exact_file(
                            &transfer_dir,
                            &transfer_path(&transfer_dir, &transfer.sender, transfer.transfer_id),
                            &bytes,
                        )?;
                    }
                }
                InboundCatalogRecord::Compact(descriptor) => {
                    if descriptor.transfer_id >= *fences.get(&descriptor.sender.0).unwrap_or(&0) {
                        return Err(
                            invalid_packet_data("unretired compact transfer catalog").into()
                        );
                    }
                }
            }
        }
        for entry in std::fs::read_dir(&root_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".packet-"))
            {
                std::fs::remove_file(path)?;
                continue;
            }
            let packet: OperationPacket = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|e| invalid_packet_data(&e.to_string()))?;
            if path != packet_path(&root_dir, &packet.signed.bound.operation.subject)
                || packets.get(&packet.signed.bound.operation.subject) != Some(&packet)
                    && root_tombstones
                        .get(&packet.signed.bound.operation.subject)
                        .is_none_or(|tombstone| {
                            tombstone.packet_commitment != packet.commitment()
                                || tombstone.bound_commitment != packet.signed.bound.commitment()
                                || tombstone.key != packet.signed.bound.operation.action.key()
                                || tombstone.mode != packet.signed.bound.declaration.mode
                        })
            {
                return Err(invalid_packet_data("invalid root submission record").into());
            }
        }
        crate::store::backend::fsync_dir(&root_dir)?;
        for entry in std::fs::read_dir(&terminal_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".packet-"))
            {
                std::fs::remove_file(path)?;
                continue;
            }
            let transfer: OperationTransfer = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|e| invalid_packet_data(&e.to_string()))?;
            if transfer.version != 2
                || transfer.destination.as_ref() != Some(&transfer.receiver)
                || path != transfer_path(&terminal_dir, &transfer.sender, transfer.transfer_id)
                || std::fs::read(transfer_path(
                    &catalog_dir,
                    &transfer.sender,
                    transfer.transfer_id,
                ))? != serde_json::to_vec(&transfer)
                    .map_err(|e| invalid_packet_data(&e.to_string()))?
            {
                return Err(invalid_packet_data("invalid terminal intake record").into());
            }
        }
        crate::store::backend::fsync_dir(&terminal_dir)?;
        for entry in std::fs::read_dir(&signing_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".packet-"))
            {
                std::fs::remove_file(path)?;
                continue;
            }
            let transfer: OperationTransfer = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|e| invalid_packet_data(&e.to_string()))?;
            if transfer.version != 2
                || transfer.destination.as_ref() != Some(&transfer.receiver)
                || transfer.packet.signed.bound.declaration.mode != CertificationMode::Enabled
                || path != transfer_path(&signing_dir, &transfer.sender, transfer.transfer_id)
                || std::fs::read(transfer_path(
                    &catalog_dir,
                    &transfer.sender,
                    transfer.transfer_id,
                ))? != serde_json::to_vec(&transfer)
                    .map_err(|e| invalid_packet_data(&e.to_string()))?
            {
                return Err(invalid_packet_data("invalid Authority signing intake record").into());
            }
        }
        crate::store::backend::fsync_dir(&signing_dir)?;
        for entry in std::fs::read_dir(&report_ack_dir)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".packet-"))
            {
                std::fs::remove_file(path)?;
                continue;
            }
            let receipt: OperationCompletionReceipt =
                serde_json::from_slice(&std::fs::read(&path)?)
                    .map_err(|e| invalid_packet_data(&e.to_string()))?;
            let inbound_path = transfer_path(
                &catalog_dir,
                &receipt.transfer_id.sender,
                receipt.transfer_id.sequence,
            );
            let transfer: OperationTransfer = serde_json::from_slice(&std::fs::read(inbound_path)?)
                .map_err(|e| invalid_packet_data(&e.to_string()))?;
            if path
                != transfer_path(
                    &report_ack_dir,
                    &receipt.transfer_id.sender,
                    receipt.transfer_id.sequence,
                )
                || receipt.version != 1
                || receipt.commitment != transfer.packet.commitment()
            {
                return Err(invalid_packet_data("invalid completion receipt record").into());
            }
        }
        crate::store::backend::fsync_dir(&report_ack_dir)?;
        let mut inbox = Self {
            _lock: lock,
            ledger,
            packet_dir,
            custody_dir,
            direct_custody_dir,
            custody_receipt_dir,
            transfer_dir,
            catalog_dir,
            fence_dir,
            root_dir,
            root_tombstone_dir,
            known_policy_dir,
            policy_witness_dir,
            terminal_dir,
            signing_dir,
            report_ack_dir,
            packets,
            compacted_packets,
            custody_receipts,
            known_policy,
            policy_witnesses,
            compacted_policy_witnesses,
            root_tombstones,
            verified_local_issuance: BTreeMap::new(),
            admission,
            fences,
            policy_publishing: None,
            poisoned: false,
        };
        for packet in inbox
            .packets
            .values()
            .chain(inbox.policy_witnesses.values())
        {
            if inbox.creation_commitment_conflicts(packet) {
                return Err(
                    invalid_packet_data("conflicting Create commitment on recovery").into(),
                );
            }
        }
        for witness in inbox.compacted_policy_witnesses.values() {
            if inbox.compacted_policy_witnesses.values().any(|other| {
                witness.identity != other.identity
                    && same_creation_identity(
                        &witness.identity.declaration,
                        &other.identity.declaration,
                    )
                    && witness.creation.create_commitment != other.creation.create_commitment
            }) || inbox.known_policy.iter().any(|(subject, commitment)| {
                commitment != &witness.creation.create_commitment
                    && inbox
                        .ledger
                        .receipt(subject)
                        .or_else(|| inbox.custody_receipts.get(subject))
                        .is_some_and(|receipt| {
                            same_creation_identity(
                                &receipt.declaration,
                                &witness.identity.declaration,
                            )
                        })
            }) {
                return Err(
                    invalid_packet_data("conflicting compact Create policy on recovery").into(),
                );
            }
        }
        for (subject, commitment) in &inbox.known_policy {
            let receipt = inbox
                .ledger
                .receipt(subject)
                .or_else(|| inbox.custody_receipts.get(subject))
                .expect("known policy marker has a validated receipt");
            if inbox
                .known_policy
                .iter()
                .any(|(other_subject, other_commitment)| {
                    other_subject != subject
                        && other_commitment != commitment
                        && inbox
                            .ledger
                            .receipt(other_subject)
                            .or_else(|| inbox.custody_receipts.get(other_subject))
                            .is_some_and(|other| {
                                same_creation_identity(&receipt.declaration, &other.declaration)
                            })
                })
            {
                return Err(invalid_packet_data("conflicting known Create policies").into());
            }
        }
        for sender in inbox.fences.keys().cloned().collect::<Vec<_>>() {
            inbox.reclaim_retired_receiver_detail(&NodeId(sender))?;
        }
        inbox.reclaim_retired_payloads()?;
        Ok(inbox)
    }

    pub fn receive(&mut self, packet: OperationPacket) -> Result<OperationReceipt, ReceiveError> {
        if self
            .routed_pin_present()
            .map_err(PersistOperationError::Io)?
        {
            return Err(ReceiveError::Rejected(
                "routed operation inbox requires relay admission".into(),
            ));
        }
        self.receive_with_role(packet, true)
    }

    /// A non-owner direct receiver keeps a signed forwarding copy outside
    /// the application ledger. The marker distinguishes an ACKed direct
    /// custody copy from an unacknowledged partial transfer on recovery.
    fn receive_direct_custody(
        &mut self,
        packet: OperationPacket,
    ) -> Result<OperationReceipt, ReceiveError> {
        let receipt = self.receive_with_role(packet.clone(), false)?;
        let subject = &packet.signed.bound.operation.subject;
        if self.packets.contains_key(subject) {
            self.poisoned = true;
            write_exact_file(
                &self.direct_custody_dir,
                &packet_path(&self.direct_custody_dir, subject),
                &packet.commitment(),
            )
            .map_err(PersistOperationError::Io)?;
            self.poisoned = false;
        }
        Ok(receipt)
    }

    /// Learn an authenticated Create policy without placing its value in the
    /// application ledger. The complete signed Create is the durable witness.
    pub fn receive_policy_witness(
        &mut self,
        packet: OperationPacket,
    ) -> Result<OperationReceipt, ReceiveError> {
        self.recover_if_poisoned().map_err(ReceiveError::Persist)?;
        self.admission
            .admit(&packet.signed)
            .map_err(ReceiveError::Rejected)?;
        if self.creation_commitment_conflicts(&packet) {
            return Err(ReceiveError::Rejected(
                "conflicting Create commitment".into(),
            ));
        }
        let bound = &packet.signed.bound;
        if !matches!(bound.operation.action, CandidateKvAction::Create { .. }) {
            return Err(ReceiveError::Rejected(
                "policy witness must contain the complete signed Create".into(),
            ));
        }
        check_local_create_conflict(&self.ledger, bound)
            .map_err(|error| ReceiveError::Rejected(error.to_string()))?;
        self.check_custody_create_conflict(bound)?;
        if self
            .ledger
            .iter()
            .any(|held| !policy_witness_matches_bound(&packet, held))
            || self
                .packets
                .values()
                .any(|held| !policy_witness_matches_packet(&packet, held))
            || self
                .ledger
                .receipts()
                .chain(self.custody_receipts.values())
                .any(|receipt| {
                    receipt.subject.namespace_id == bound.operation.subject.namespace_id
                        && receipt.declaration.key == bound.declaration.key
                        && (receipt.declaration != bound.declaration
                            || (receipt.action_kind == crate::operation::ReceiptActionKind::Create
                                && receipt.commitment != bound.commitment()))
                })
        {
            return Err(ReceiveError::Rejected(
                "policy witness conflicts with retained operation".into(),
            ));
        }
        let key = policy_witness_key(&packet);
        if let Some(existing) = self.compacted_policy_witnesses.get(&key) {
            if existing.identity != PolicyWitnessIdentity::from(&packet) {
                return Err(ReceiveError::Rejected(
                    "conflicting compact policy witness".into(),
                ));
            }
            return Ok(OperationReceipt {
                commitment: packet.commitment(),
                already_present: true,
            });
        }
        if let Some(existing) = self.policy_witnesses.get(&key) {
            if existing != &packet {
                return Err(ReceiveError::Rejected("conflicting policy witness".into()));
            }
            return Ok(OperationReceipt {
                commitment: packet.commitment(),
                already_present: true,
            });
        }
        self.poisoned = true;
        write_packet_file(&self.policy_witness_dir, &bound.operation.subject, &packet)
            .map_err(PersistOperationError::Io)?;
        let root = self.policy_witness_dir.parent().ok_or_else(|| {
            ReceiveError::Persist(PersistOperationError::Io(invalid_packet_data(
                "invalid inbox path",
            )))
        })?;
        let mut manifest = read_policy_witness_manifest(root).map_err(PersistOperationError::Io)?;
        manifest.insert(key.clone(), PolicyWitnessIdentity::from(&packet));
        write_policy_witness_manifest(root, &manifest).map_err(PersistOperationError::Io)?;
        let commitment = packet.commitment();
        self.policy_witnesses.insert(key, packet);
        self.poisoned = false;
        Ok(OperationReceipt {
            commitment,
            already_present: false,
        })
    }

    /// Enabled by the policy relay before it begins accepting ordinary
    /// operations. A complete Create must then retain an independent signed
    /// policy witness before its normal receive ACK can be returned. This
    /// closes the window in which ordinary custody could be reclaimed before
    /// the policy delivery worker discovers the Create.
    pub fn enable_policy_publishing(&mut self, required: Arc<OperationRequiredDestinations>) {
        self.policy_publishing = Some(required);
    }

    fn retain_create_policy_before_ack(
        &mut self,
        packet: &OperationPacket,
    ) -> Result<(), ReceiveError> {
        let bound = &packet.signed.bound;
        if !matches!(bound.operation.action, CandidateKvAction::Create { .. }) {
            return Ok(());
        }
        let Some(required) = &self.policy_publishing else {
            return Ok(());
        };
        if required
            .destinations_for(
                Some(&bound.operation.subject.namespace_id),
                bound.operation.action.key(),
            )
            .is_none()
        {
            return Ok(());
        }
        let key = policy_witness_key(packet);
        if let Some(existing) = self.compacted_policy_witnesses.get(&key) {
            if existing.identity == PolicyWitnessIdentity::from(packet) {
                return Ok(());
            }
            return Err(ReceiveError::Rejected(
                "operation conflicts with compact policy witness".into(),
            ));
        }
        if let Some(existing) = self.policy_witnesses.get(&key) {
            if policy_witness_matches_packet(existing, packet) {
                return Ok(());
            }
            return Err(ReceiveError::Rejected(
                "operation conflicts with authenticated policy witness".into(),
            ));
        }
        self.receive_policy_witness(packet.clone())?;
        Ok(())
    }

    fn receive_with_role(
        &mut self,
        packet: OperationPacket,
        apply_to_ledger: bool,
    ) -> Result<OperationReceipt, ReceiveError> {
        self.recover_if_poisoned().map_err(ReceiveError::Persist)?;
        if self.poisoned {
            return Err(ReceiveError::Persist(PersistOperationError::Io(
                invalid_packet_data("operation inbox must be reopened after uncertain write"),
            )));
        }
        self.admission
            .admit(&packet.signed)
            .map_err(ReceiveError::Rejected)?;
        let subject = packet.signed.bound.operation.subject.clone();
        if self.creation_commitment_conflicts(&packet) {
            if self.packets.contains_key(&subject) || self.compacted_packets.contains_key(&subject)
            {
                return Err(ReceiveError::Persist(PersistOperationError::Operation(
                    crate::operation::OperationError::ConflictingContent(subject),
                )));
            }
            return Err(ReceiveError::Rejected(
                "conflicting Create commitment".into(),
            ));
        }
        self.check_policy_witness_consistency(&packet)?;
        if let Some((signed, bound)) = self.compacted_packets.get(&subject) {
            let expected_receipt = crate::operation::OperationReceipt::from(&packet.signed.bound);
            if *signed != packet.commitment()
                || *bound != packet.signed.bound.commitment()
                || self
                    .ledger
                    .receipt(&subject)
                    .or_else(|| self.custody_receipts.get(&subject))
                    != Some(&expected_receipt)
            {
                return Err(ReceiveError::Persist(PersistOperationError::Operation(
                    crate::operation::OperationError::ConflictingContent(subject),
                )));
            }
            if !apply_to_ledger || self.ledger.receipt(&subject).is_some() {
                let commitment = *signed;
                self.restore_known_policy_from_retry(&packet)?;
                self.retain_create_policy_before_ack(&packet)?;
                // Exact retries do not resurrect collected application data.
                return Ok(OperationReceipt {
                    commitment,
                    already_present: true,
                });
            }
        }
        if let Some(existing) = self.packets.get(&subject)
            && existing != &packet
        {
            return Err(ReceiveError::Persist(PersistOperationError::Operation(
                crate::operation::OperationError::ConflictingContent(subject),
            )));
        }
        if self
            .ledger
            .get(&subject)
            .is_some_and(|existing| existing != &packet.signed.bound)
        {
            return Err(ReceiveError::Persist(PersistOperationError::Operation(
                crate::operation::OperationError::ConflictingContent(subject),
            )));
        }
        if self.packets.contains_key(&subject)
            && (self.ledger.get(&subject) == Some(&packet.signed.bound)
                || self.ledger.receipt(&subject)
                    == Some(&crate::operation::OperationReceipt::from(
                        &packet.signed.bound,
                    ))
                || (!apply_to_ledger && packet_path(&self.custody_dir, &subject).exists()))
        {
            // Both recovery copies are already durable. Lost transport ACKs
            // can replay this packet indefinitely; they do not need another
            // ledger WAL frame or a segment rotation.
            self.retain_create_policy_before_ack(&packet)?;
            return Ok(OperationReceipt {
                commitment: packet.commitment(),
                already_present: true,
            });
        }
        check_local_create_conflict(&self.ledger, &packet.signed.bound)
            .map_err(|e| ReceiveError::Rejected(e.to_string()))?;
        self.check_custody_create_conflict(&packet.signed.bound)?;
        let commitment = packet.commitment();
        // The complete signed packet is fsynced first. A crash before the
        // ledger append leaves a recoverable packet; open replays it. A
        // ledger-first append could leave an unauthenticated body with no
        // forwarding copy after restart.
        self.poisoned = true;
        if !apply_to_ledger
            && self.ledger.get(&subject).is_none()
            && self.ledger.receipt(&subject).is_none()
        {
            // The marker must precede the packet so a crash cannot promote a
            // relay custody copy into application history during recovery.
            write_exact_file(
                &self.custody_dir,
                &packet_path(&self.custody_dir, &subject),
                &commitment,
            )
            .map_err(PersistOperationError::Io)?;
        }
        if !self.packets.contains_key(&subject) {
            write_packet_file(&self.packet_dir, &subject, &packet)
                .map_err(PersistOperationError::Io)?;
        }
        if !apply_to_ledger {
            let receipt = crate::operation::OperationReceipt::from(&packet.signed.bound);
            write_exact_file(
                &self.custody_receipt_dir,
                &packet_path(&self.custody_receipt_dir, &subject),
                &serde_json::to_vec(&receipt).map_err(|error| {
                    PersistOperationError::Io(invalid_packet_data(&error.to_string()))
                })?,
            )
            .map_err(PersistOperationError::Io)?;
            self.custody_receipts.insert(subject.clone(), receipt);
        }
        let outcome = if apply_to_ledger {
            Some(self.ledger.record_and_sync(packet.signed.bound.clone())?)
        } else {
            None
        };
        self.packets.insert(subject.clone(), packet.clone());
        if apply_to_ledger {
            let marker = packet_path(&self.custody_dir, &subject);
            if marker.exists() {
                std::fs::remove_file(marker).map_err(PersistOperationError::Io)?;
                crate::store::backend::fsync_dir(&self.custody_dir)
                    .map_err(PersistOperationError::Io)?;
            }
        }
        self.poisoned = false;
        self.retain_create_policy_before_ack(&packet)?;
        Ok(OperationReceipt {
            commitment,
            already_present: outcome == Some(RecordOutcome::Retry),
        })
    }

    fn restore_known_policy_from_retry(
        &mut self,
        packet: &OperationPacket,
    ) -> Result<(), ReceiveError> {
        let subject = &packet.signed.bound.operation.subject;
        if self.known_policy.contains_key(subject) || !self.creation_policy_known_for_packet(packet)
        {
            return Ok(());
        }
        let marker = KnownCreationPolicy {
            version: 2,
            subject: subject.clone(),
            bound_commitment: packet.signed.bound.commitment(),
            creation_commitment: creation_commitment(packet),
        };
        let bytes = serde_json::to_vec(&marker)
            .map_err(|error| PersistOperationError::Io(invalid_packet_data(&error.to_string())))?;
        self.poisoned = true;
        write_exact_file(
            &self.known_policy_dir,
            &packet_path(&self.known_policy_dir, subject),
            &bytes,
        )
        .map_err(PersistOperationError::Io)?;
        self.known_policy
            .insert(subject.clone(), creation_commitment(packet));
        self.poisoned = false;
        Ok(())
    }

    fn creation_commitment_conflicts(&self, packet: &OperationPacket) -> bool {
        let declaration = &packet.signed.bound.declaration;
        let commitment = creation_commitment(packet);
        self.packets
            .values()
            .chain(self.policy_witnesses.values())
            .any(|held| {
                same_creation_identity(declaration, &held.signed.bound.declaration)
                    && creation_commitment(held) != commitment
            })
            || self.compacted_policy_witnesses.values().any(|held| {
                same_creation_identity(declaration, &held.identity.declaration)
                    && held.creation.create_commitment != commitment
            })
            || self.known_policy.iter().any(|(subject, held_commitment)| {
                held_commitment != &commitment
                    && self
                        .ledger
                        .receipt(subject)
                        .or_else(|| self.custody_receipts.get(subject))
                        .is_some_and(|receipt| {
                            same_creation_identity(declaration, &receipt.declaration)
                        })
            })
    }

    fn check_policy_witness_consistency(
        &self,
        packet: &OperationPacket,
    ) -> Result<(), ReceiveError> {
        if self
            .policy_witnesses
            .get(&policy_witness_key(packet))
            .is_some_and(|witness| !policy_witness_matches_packet(witness, packet))
        {
            return Err(ReceiveError::Rejected(
                "operation conflicts with authenticated policy witness".into(),
            ));
        }
        if self
            .compacted_policy_witnesses
            .get(&policy_witness_key(packet))
            .is_some_and(|witness| {
                !compacted_policy_matches_bound(witness, &packet.signed.bound)
                    || (matches!(
                        packet.signed.bound.operation.action,
                        CandidateKvAction::Create { .. }
                    ) && witness.identity != PolicyWitnessIdentity::from(packet))
            })
        {
            return Err(ReceiveError::Rejected(
                "operation conflicts with compact policy witness".into(),
            ));
        }
        Ok(())
    }

    fn check_custody_create_conflict(&self, bound: &BoundOperation) -> Result<(), ReceiveError> {
        for receipt in self.custody_receipts.values() {
            if receipt.subject.namespace_id != bound.operation.subject.namespace_id
                || receipt.declaration.key != bound.operation.action.key()
            {
                continue;
            }
            if receipt.declaration != bound.declaration
                || (matches!(bound.operation.action, CandidateKvAction::Create { .. })
                    && receipt.action_kind == crate::operation::ReceiptActionKind::Create
                    && receipt.subject != bound.operation.subject)
            {
                return Err(ReceiveError::Rejected(
                    "competing creation in retained custody".into(),
                ));
            }
        }
        Ok(())
    }

    /// Local creation/update is closed after this node has retained a delete.
    /// An exact retry keeps its original ID and remains idempotent. Remote
    /// ingest uses `receive` directly and can still accept delayed history.
    fn key_deleted_for_new_local(&self, packet: &OperationPacket) -> bool {
        let bound = &packet.signed.bound;
        let subject = &bound.operation.subject;
        if let Some(existing) = self.packets.get(subject)
            && existing == packet
        {
            return false;
        }
        if let Some((signed, compacted_bound)) = self.compacted_packets.get(subject)
            && *signed == packet.commitment()
            && *compacted_bound == bound.commitment()
            && self.ledger.receipt(subject)
                == Some(&crate::operation::OperationReceipt::from(bound))
        {
            return false;
        }
        if let Some(tombstone) = self.root_tombstones.get(subject)
            && tombstone.packet_commitment == packet.commitment()
            && tombstone.bound_commitment == bound.commitment()
            && tombstone.key == bound.operation.action.key()
            && tombstone.mode == bound.declaration.mode
            && self.ledger.receipt(subject)
                == Some(&crate::operation::OperationReceipt::from(bound))
        {
            return false;
        }
        if !self.packets.contains_key(subject)
            && !self.compacted_packets.contains_key(subject)
            && !self.root_tombstones.contains_key(subject)
            && (self.ledger.get(subject) == Some(bound)
                || self.ledger.receipt(subject)
                    == Some(&crate::operation::OperationReceipt::from(bound)))
        {
            return false;
        }
        let key = bound.operation.action.key();
        self.ledger.iter().any(|held| {
            held.operation.subject.namespace_id == subject.namespace_id
                && held.operation.action.key() == key
                && matches!(held.operation.action, CandidateKvAction::Delete { .. })
        }) || self.ledger.receipts().any(|receipt| {
            receipt.subject.namespace_id == subject.namespace_id
                && receipt.declaration.key == key
                && receipt.action_kind == crate::operation::ReceiptActionKind::Delete
        })
    }

    pub fn receive_transfer(
        &mut self,
        transfer: OperationTransfer,
        self_id: &NodeId,
    ) -> Result<OperationTransferReceipt, ReceiveError> {
        if self
            .routed_pin_present()
            .map_err(PersistOperationError::Io)?
        {
            return Err(ReceiveError::Rejected(
                "routed operation inbox requires relay admission".into(),
            ));
        }
        self.receive_transfer_with_role(transfer, self_id, true)
    }

    fn receive_transfer_with_role(
        &mut self,
        transfer: OperationTransfer,
        self_id: &NodeId,
        apply_to_ledger: bool,
    ) -> Result<OperationTransferReceipt, ReceiveError> {
        self.recover_if_poisoned().map_err(ReceiveError::Persist)?;
        if !matches!(transfer.version, 1 | 2)
            || (transfer.version == 1
                && (transfer.destination.is_some()
                    || transfer.parent.is_some()
                    || transfer.depth != 0))
            || (transfer.version == 2
                && (transfer.destination.is_none()
                    || transfer.parent.is_none() != (transfer.depth == 0)))
            || transfer.receiver != *self_id
            || transfer.sender == transfer.receiver
        {
            return Err(ReceiveError::Rejected(
                "wrong transfer receiver or sender".into(),
            ));
        }
        let path = transfer_path(&self.transfer_dir, &transfer.sender, transfer.transfer_id);
        let catalog_path = transfer_path(&self.catalog_dir, &transfer.sender, transfer.transfer_id);
        let bytes = serde_json::to_vec(&transfer).map_err(|e| {
            ReceiveError::Persist(PersistOperationError::Io(invalid_packet_data(
                &e.to_string(),
            )))
        })?;
        // Reject sender-ID reuse before admitting a new packet into the inbox.
        if (path.exists() && std::fs::read(&path).map_err(PersistOperationError::Io)? != bytes)
            || (catalog_path.exists()
                && std::fs::read(&catalog_path).map_err(PersistOperationError::Io)? != bytes)
        {
            return Err(ReceiveError::Rejected(
                "transfer ID rebound to different packet or receiver".into(),
            ));
        }
        if transfer.transfer_id < *self.fences.get(&transfer.sender.0).unwrap_or(&0) {
            return Err(ReceiveError::Rejected(
                "retired transfer cannot receive a fresh ACK".into(),
            ));
        }
        let receipt = self.receive_with_role(transfer.packet.clone(), apply_to_ledger)?;
        let subject = &transfer.packet.signed.bound.operation.subject;
        if !self.packets.contains_key(subject) {
            // A different sender may deliver the same operation after older
            // inbound transfers have retired. This new transfer is live, so
            // restore its signed forwarding copy before acknowledging it.
            self.poisoned = true;
            if !apply_to_ledger
                && self.ledger.get(subject).is_none()
                && self.ledger.receipt(subject).is_none()
            {
                write_exact_file(
                    &self.custody_dir,
                    &packet_path(&self.custody_dir, subject),
                    &transfer.packet.commitment(),
                )
                .map_err(PersistOperationError::Io)?;
            }
            write_packet_file(&self.packet_dir, subject, &transfer.packet)
                .map_err(PersistOperationError::Io)?;
            self.packets
                .insert(subject.clone(), transfer.packet.clone());
            self.poisoned = false;
        }
        // A sender-scoped ID cannot be rebound to another receiver or packet.
        // Either file may have been published before its directory sync
        // failed. Do not let the live inbox use that uncertain transfer as
        // completed custody until recovery has checked both copies.
        self.poisoned = true;
        write_exact_file(&self.catalog_dir, &catalog_path, &bytes)
            .map_err(|e| ReceiveError::Persist(PersistOperationError::Io(e)))?;
        write_exact_file(&self.transfer_dir, &path, &bytes)
            .map_err(|e| ReceiveError::Persist(PersistOperationError::Io(e)))?;
        self.poisoned = false;
        Ok(OperationTransferReceipt {
            version: transfer.version,
            sender: transfer.sender,
            transfer_id: transfer.transfer_id,
            receiver: transfer.receiver,
            commitment: receipt.commitment,
            already_present: receipt.already_present,
        })
    }

    /// Recover the exact stored body for local replay or forwarding.
    pub fn get(&self, subject: &CrdtSubject) -> Option<BoundOperation> {
        if self.poisoned {
            return None;
        }
        self.ledger.get(subject).cloned()
    }

    /// Durable ID metadata for a body reclaimed from the local ledger.
    pub fn receipt(&self, subject: &CrdtSubject) -> Option<crate::operation::OperationReceipt> {
        if self.poisoned {
            return None;
        }
        self.ledger.receipt(subject).cloned()
    }

    /// A follow-up carries only a signed hash reference to Create.
    /// Its policy becomes known once the matching authenticated Create (or
    /// its durable receipt) reaches this inbox.
    pub fn creation_policy_known(&self, subject: &CrdtSubject) -> bool {
        if self.poisoned {
            return false;
        }
        let Some(packet) = self.packets.get(subject) else {
            // The marker is fsynced before signed custody is removed. Open
            // rejects a reclaimed packet if that marker is missing.
            return (self.ledger.receipt(subject).is_some()
                || self.custody_receipts.contains_key(subject))
                && self.compacted_packets.contains_key(subject)
                && self.known_policy.contains_key(subject);
        };
        self.creation_policy_known_for_packet(packet)
    }

    fn creation_policy_known_for_packet(&self, packet: &OperationPacket) -> bool {
        let proof = &packet.signed.creation;
        {
            check_compact_creation_witness(&self.ledger, &packet.signed).is_ok()
                || self
                    .custody_receipts
                    .get(&proof.declaration.creation_id)
                    .is_some_and(|receipt| {
                        receipt.action_kind == crate::operation::ReceiptActionKind::Create
                            && receipt.declaration == proof.declaration
                            && receipt.commitment == proof.create_commitment
                    })
                || self
                    .policy_witnesses
                    .get(&(
                        proof.declaration.creation_id.namespace_id.clone(),
                        proof.declaration.key.clone(),
                    ))
                    .is_some_and(|witness| {
                        witness.signed.bound.operation.subject == proof.declaration.creation_id
                            && witness.signed.bound.declaration == proof.declaration
                            && witness.signed.bound.commitment() == proof.create_commitment
                    })
                || self
                    .compacted_policy_witnesses
                    .get(&(
                        proof.declaration.creation_id.namespace_id.clone(),
                        proof.declaration.key.clone(),
                    ))
                    .is_some_and(|witness| {
                        witness.identity.subject == proof.declaration.creation_id
                            && witness.identity.declaration == proof.declaration
                            && witness.identity.bound_commitment == proof.create_commitment
                    })
        }
    }

    /// Durable value-free operation IDs eligible for independently guarded
    /// evidence compaction. The caller must check certificate delivery needs.
    pub fn receipts(&self) -> Vec<crate::operation::OperationReceipt> {
        if self.poisoned {
            return Vec::new();
        }
        self.ledger.receipts().cloned().collect()
    }

    /// The exact signed packet remains available for forwarding after a
    /// receiver restart, including the authenticated creation evidence.
    pub fn get_packet(&self, subject: &CrdtSubject) -> Option<OperationPacket> {
        if self.poisoned {
            return None;
        }
        self.packets.get(subject).cloned()
    }

    /// Complete authenticated Create retained outside the application ledger.
    pub fn policy_witness_packet(&self, namespace_id: &str, key: &str) -> Option<OperationPacket> {
        if self.poisoned {
            return None;
        }
        self.policy_witnesses
            .get(&(namespace_id.to_owned(), key.to_owned()))
            .cloned()
    }

    /// Stable identity used to check a compacted policy outbox record against
    /// the authenticated inbox witness.
    pub fn policy_witness_identity(
        &self,
        namespace_id: &str,
        key: &str,
    ) -> Option<PolicyWitnessIdentity> {
        if self.poisoned {
            return None;
        }
        let key = (namespace_id.to_owned(), key.to_owned());
        self.policy_witnesses
            .get(&key)
            .map(PolicyWitnessIdentity::from)
            .or_else(|| {
                self.compacted_policy_witnesses
                    .get(&key)
                    .map(|witness| witness.identity.clone())
            })
    }

    /// A value-free creation witness for Authority admission. The returned
    /// type cannot be constructed from unauthenticated wire metadata.
    pub fn verified_compact_policy_witness(
        &self,
        namespace_id: &str,
        key: &str,
    ) -> Option<VerifiedCompactPolicyWitness> {
        if self.poisoned {
            return None;
        }
        self.compacted_policy_witnesses
            .get(&(namespace_id.to_owned(), key.to_owned()))
            .map(|record| VerifiedCompactPolicyWitness {
                identity: record.identity.clone(),
                creation: record.creation.clone(),
            })
    }

    /// Reclaim the initial value after the policy relay has durably recorded
    /// a receipt for every required destination. A final destination with no
    /// onward obligations may call this after its first full validation.
    pub(crate) fn compact_policy_witness(
        &mut self,
        namespace_id: &str,
        key: &str,
    ) -> io::Result<bool> {
        self.recover_if_poisoned()
            .map_err(|error| invalid_packet_data(&format!("{error:?}")))?;
        let key_id = (namespace_id.to_owned(), key.to_owned());
        let Some(packet) = self.policy_witnesses.get(&key_id).cloned() else {
            return Ok(false);
        };
        let creation = &packet.signed.creation;
        let writer_trust_digest = self
            .admission
            .compact_creation_trust(creation)
            .map_err(|error| invalid_packet_data(&error))?;
        let record = CompactedPolicyWitness {
            version: 1,
            identity: PolicyWitnessIdentity::from(&packet),
            creation: creation.clone(),
            writer_trust_digest,
        };
        let path = packet_path(&self.policy_witness_dir, &record.identity.subject);
        if std::fs::read(&path)?
            != serde_json::to_vec(&packet)
                .map_err(|error| invalid_packet_data(&error.to_string()))?
        {
            return Err(invalid_packet_data(
                "policy witness changed before compaction",
            ));
        }
        let bytes =
            serde_json::to_vec(&record).map_err(|error| invalid_packet_data(&error.to_string()))?;
        self.poisoned = true;
        replace_file(&self.policy_witness_dir, &path, &bytes)?;
        self.compacted_policy_witnesses
            .insert(key_id.clone(), record);
        self.policy_witnesses.remove(&key_id);
        self.poisoned = false;
        Ok(true)
    }

    /// Authenticated policy witnesses held independently of application data.
    /// A routed sender can reconstruct missing durable send obligations from
    /// this snapshot after a crash between witness and outbox fsync.
    pub fn policy_witness_packets(&self) -> Vec<OperationPacket> {
        if self.poisoned {
            return Vec::new();
        }
        self.policy_witnesses.values().cloned().collect()
    }

    /// Snapshot complete custody packets for startup delivery reconciliation.
    pub fn packets(&self) -> Vec<OperationPacket> {
        if self.poisoned {
            return Vec::new();
        }
        self.packets.values().cloned().collect()
    }

    pub fn record_root(&mut self, packet: &OperationPacket) -> io::Result<()> {
        let subject = &packet.signed.bound.operation.subject;
        if self.packets.get(subject) != Some(packet)
            || self.root_tombstones.contains_key(subject)
            || self.poisoned
        {
            return Err(invalid_packet_data("root packet has not been received"));
        }
        let path = packet_path(&self.root_dir, subject);
        let bytes = serde_json::to_vec(packet).map_err(|e| invalid_packet_data(&e.to_string()))?;
        write_exact_file(&self.root_dir, &path, &bytes)
    }

    /// Persist local origin before packet/ledger intake. The root file is a
    /// complete signed recovery copy, so startup can finish a submit stopped
    /// at any subsequent write before rebuilding routed send obligations.
    fn receive_local_root(
        &mut self,
        packet: OperationPacket,
    ) -> Result<OperationReceipt, ReceiveError> {
        self.recover_if_poisoned().map_err(ReceiveError::Persist)?;
        if self.poisoned {
            return Err(ReceiveError::Persist(PersistOperationError::Io(
                invalid_packet_data("operation inbox must be reopened after uncertain write"),
            )));
        }
        self.admission
            .admit(&packet.signed)
            .map_err(ReceiveError::Rejected)?;
        let subject = &packet.signed.bound.operation.subject;
        if self.creation_commitment_conflicts(&packet) {
            if self.packets.contains_key(subject)
                || self.compacted_packets.contains_key(subject)
                || self.root_tombstones.contains_key(subject)
            {
                return Err(ReceiveError::Persist(PersistOperationError::Operation(
                    crate::operation::OperationError::ConflictingContent(subject.clone()),
                )));
            }
            return Err(ReceiveError::Rejected(
                "conflicting Create commitment".into(),
            ));
        }
        if let Some(tombstone) = self.root_tombstones.get(subject) {
            if tombstone.packet_commitment != packet.commitment()
                || tombstone.bound_commitment != packet.signed.bound.commitment()
                || tombstone.key != packet.signed.bound.operation.action.key()
                || tombstone.mode != packet.signed.bound.declaration.mode
            {
                return Err(ReceiveError::Persist(PersistOperationError::Operation(
                    crate::operation::OperationError::ConflictingContent(subject.clone()),
                )));
            }
            if self.ledger.receipt(subject).is_none() {
                return Err(ReceiveError::Persist(PersistOperationError::Io(
                    invalid_packet_data("reclaimed root has no ledger receipt"),
                )));
            }
            let commitment = tombstone.packet_commitment;
            self.restore_known_policy_from_retry(&packet)?;
            return Ok(OperationReceipt {
                commitment,
                already_present: true,
            });
        }
        if self
            .packets
            .get(subject)
            .is_some_and(|existing| existing != &packet)
            || self.compacted_packets.contains_key(subject)
            || self
                .ledger
                .get(subject)
                .is_some_and(|existing| existing != &packet.signed.bound)
            || self.ledger.receipt(subject).is_some_and(|receipt| {
                receipt != &crate::operation::OperationReceipt::from(&packet.signed.bound)
            })
        {
            return Err(ReceiveError::Persist(PersistOperationError::Operation(
                crate::operation::OperationError::ConflictingContent(subject.clone()),
            )));
        }
        check_local_create_conflict(&self.ledger, &packet.signed.bound)
            .map_err(|e| ReceiveError::Rejected(e.to_string()))?;
        self.check_policy_witness_consistency(&packet)?;
        self.check_custody_create_conflict(&packet.signed.bound)?;
        self.poisoned = true;
        let path = packet_path(&self.root_dir, subject);
        let bytes = serde_json::to_vec(&packet).map_err(|e| {
            ReceiveError::Persist(PersistOperationError::Io(invalid_packet_data(
                &e.to_string(),
            )))
        })?;
        write_exact_file(&self.root_dir, &path, &bytes)
            .map_err(|e| ReceiveError::Persist(PersistOperationError::Io(e)))?;
        self.poisoned = false;
        self.receive_with_role(packet, true)
    }

    pub fn root_packets(&self) -> io::Result<Vec<OperationPacket>> {
        std::fs::read_dir(&self.root_dir)?
            .map(|entry| {
                let bytes = std::fs::read(entry?.path())?;
                let packet: OperationPacket = serde_json::from_slice(&bytes)
                    .map_err(|e| invalid_packet_data(&e.to_string()))?;
                Ok(packet)
            })
            .filter(|packet| match packet {
                Err(_) => true,
                Ok(packet) => !self
                    .root_tombstones
                    .contains_key(&packet.signed.bound.operation.subject),
            })
            .collect()
    }

    /// A routing pin may be changed only before this inbox has participated
    /// in any routed custody handoff. Local roots are safe to re-enroll under
    /// a repaired route; received transfers, terminal intake, retirement
    /// fences and reclaimed roots are tied to the original topology.
    fn routing_repin_is_pristine(&self) -> io::Result<bool> {
        if self.poisoned
            || !self.root_tombstones.is_empty()
            || !self.compacted_packets.is_empty()
            || !self.custody_receipts.is_empty()
            || !self.fences.is_empty()
        {
            return Ok(false);
        }
        for dir in [
            &self.custody_dir,
            &self.direct_custody_dir,
            &self.custody_receipt_dir,
            &self.transfer_dir,
            &self.catalog_dir,
            &self.fence_dir,
            &self.root_tombstone_dir,
            &self.terminal_dir,
            &self.signing_dir,
            &self.report_ack_dir,
        ] {
            if std::fs::read_dir(dir)?.next().is_some() {
                return Ok(false);
            }
        }
        let roots = self.root_packets()?;
        Ok(self.packets.values().all(|packet| roots.contains(packet)))
    }

    fn root_tombstones(&self) -> impl Iterator<Item = &RootTombstone> {
        self.root_tombstones.values()
    }

    pub(crate) fn note_verified_local_issuance(
        &mut self,
        subject: CrdtSubject,
        commitment: [u8; 32],
    ) -> io::Result<()> {
        if self.poisoned {
            return Err(invalid_packet_data(
                "operation inbox must be reopened after uncertain write",
            ));
        }
        if self
            .verified_local_issuance
            .get(&subject)
            .is_some_and(|previous| *previous != commitment)
        {
            return Err(invalid_packet_data(
                "local Authority issued conflicting operation commitment",
            ));
        }
        self.verified_local_issuance.insert(subject, commitment);
        Ok(())
    }

    fn has_verified_local_issuance(&self, origin: &RootTombstone) -> bool {
        self.verified_local_issuance
            .get(&origin.subject)
            .is_some_and(|commitment| *commitment == origin.bound_commitment)
    }

    fn reclaim_root(&mut self, packet: &OperationPacket) -> io::Result<()> {
        if self.poisoned {
            return Err(invalid_packet_data(
                "operation inbox must be reopened after uncertain write",
            ));
        }
        let subject = &packet.signed.bound.operation.subject;
        if self.packets.get(subject) != Some(packet)
            || self.ledger.receipt(subject).is_none_or(|receipt| {
                receipt != &crate::operation::OperationReceipt::from(&packet.signed.bound)
            })
            || !packet_path(&self.root_dir, subject).exists()
        {
            return Err(invalid_packet_data(
                "root is not ready for payload collection",
            ));
        }
        let tombstone = RootTombstone {
            version: 1,
            subject: subject.clone(),
            key: packet.signed.bound.operation.action.key().to_owned(),
            mode: packet.signed.bound.declaration.mode,
            packet_commitment: packet.commitment(),
            bound_commitment: packet.signed.bound.commitment(),
        };
        self.poisoned = true;
        write_exact_file(
            &self.root_tombstone_dir,
            &packet_path(&self.root_tombstone_dir, subject),
            &serde_json::to_vec(&tombstone).map_err(|e| invalid_packet_data(&e.to_string()))?,
        )?;
        self.root_tombstones.insert(subject.clone(), tombstone);
        self.finish_reclaimed_root(subject)?;
        self.poisoned = false;
        Ok(())
    }

    fn finish_reclaimed_root(&mut self, subject: &CrdtSubject) -> io::Result<()> {
        if !self.root_tombstones.contains_key(subject) {
            return Err(invalid_packet_data(
                "root has no durable reclaimed identity",
            ));
        }
        let path = packet_path(&self.root_dir, subject);
        if path.exists() {
            std::fs::remove_file(path)?;
            crate::store::backend::fsync_dir(&self.root_dir)?;
        }
        Ok(())
    }

    pub fn received_transfers(&self) -> io::Result<Vec<OperationTransfer>> {
        self.require_certain()?;
        std::fs::read_dir(&self.transfer_dir)?
            .map(|entry| {
                let bytes = std::fs::read(entry?.path())?;
                serde_json::from_slice(&bytes).map_err(|e| invalid_packet_data(&e.to_string()))
            })
            .collect()
    }

    /// Full descriptors are needed while a ledger body is still eligible for
    /// compaction. Already compacted inbound identities have no payload.
    pub fn catalog_transfers(&self) -> io::Result<Vec<OperationTransfer>> {
        self.require_certain()?;
        let mut transfers = Vec::new();
        for entry in std::fs::read_dir(&self.catalog_dir)? {
            let record: InboundCatalogRecord =
                serde_json::from_slice(&std::fs::read(entry?.path())?)
                    .map_err(|e| invalid_packet_data(&e.to_string()))?;
            if let InboundCatalogRecord::Full(transfer) = record {
                transfers.push(transfer);
            }
        }
        Ok(transfers)
    }

    /// Save a monotone sender prefix before removing any receiver detail.
    /// The immutable catalog and signed packet stay available to reject ID
    /// reuse. Terminal and signing intake are independent durable obligations.
    pub fn save_retirement_fence(
        &mut self,
        notice: &OperationRetirementNotice,
        self_id: &NodeId,
    ) -> io::Result<()> {
        if self.poisoned
            || notice.version != 1
            || notice.receiver != *self_id
            || notice.sender == notice.receiver
        {
            return Err(invalid_packet_data("invalid retirement fence receiver"));
        }
        let saved = *self.fences.get(&notice.sender.0).unwrap_or(&0);
        if notice.closed_before > saved {
            self.poisoned = true;
            let path = retirement_path(&self.fence_dir, &notice.sender);
            replace_file(
                &self.fence_dir,
                &path,
                &serde_json::to_vec(notice).map_err(|e| invalid_packet_data(&e.to_string()))?,
            )?;
            self.fences
                .insert(notice.sender.0.clone(), notice.closed_before);
            self.poisoned = false;
        }
        self.reclaim_retired_receiver_detail(&notice.sender)?;
        self.reclaim_retired_payloads()?;
        Ok(())
    }

    pub fn reclaim_retired_receiver_detail(&mut self, sender: &NodeId) -> io::Result<()> {
        if self.poisoned {
            return Err(invalid_packet_data(
                "operation inbox must be reopened after uncertain write",
            ));
        }
        let cut = *self.fences.get(&sender.0).unwrap_or(&0);
        if cut == 0 {
            return Ok(());
        }
        self.poisoned = true;
        for entry in std::fs::read_dir(&self.transfer_dir)? {
            let path = entry?.path();
            let bytes = std::fs::read(&path)?;
            let transfer: OperationTransfer =
                serde_json::from_slice(&bytes).map_err(|e| invalid_packet_data(&e.to_string()))?;
            if transfer.sender != *sender || transfer.transfer_id >= cut {
                continue;
            }
            if std::fs::read(transfer_path(
                &self.catalog_dir,
                sender,
                transfer.transfer_id,
            ))? != bytes
            {
                return Err(invalid_packet_data(
                    "retired transfer lacks exact catalog record",
                ));
            }
            std::fs::remove_file(path)?;
        }
        crate::store::backend::fsync_dir(&self.transfer_dir)?;
        for entry in std::fs::read_dir(&self.report_ack_dir)? {
            let path = entry?.path();
            let receipt: OperationCompletionReceipt =
                serde_json::from_slice(&std::fs::read(&path)?)
                    .map_err(|e| invalid_packet_data(&e.to_string()))?;
            if receipt.transfer_id.sender == *sender && receipt.transfer_id.sequence < cut {
                if !transfer_path(&self.catalog_dir, sender, receipt.transfer_id.sequence).exists()
                {
                    return Err(invalid_packet_data(
                        "retired report ACK lacks exact catalog record",
                    ));
                }
                std::fs::remove_file(path)?;
            }
        }
        crate::store::backend::fsync_dir(&self.report_ack_dir)?;
        // A retired transfer cannot produce another completion report. Its
        // terminal marker is redundant once the sender fence is durable;
        // Authority signing intake remains a separate obligation.
        for entry in std::fs::read_dir(&self.terminal_dir)? {
            let path = entry?.path();
            let bytes = std::fs::read(&path)?;
            let transfer: OperationTransfer =
                serde_json::from_slice(&bytes).map_err(|e| invalid_packet_data(&e.to_string()))?;
            if transfer.sender != *sender || transfer.transfer_id >= cut {
                continue;
            }
            if std::fs::read(transfer_path(
                &self.catalog_dir,
                sender,
                transfer.transfer_id,
            ))? != bytes
            {
                return Err(invalid_packet_data(
                    "retired terminal intake lacks exact catalog record",
                ));
            }
            std::fs::remove_file(path)?;
        }
        crate::store::backend::fsync_dir(&self.terminal_dir)?;
        self.poisoned = false;
        Ok(())
    }

    /// Replace every retired inbound copy of a compacted operation with an
    /// immutable identity descriptor, then remove the shared signed packet.
    /// A nonretired transfer, local root, Authority signing intake, or live
    /// ledger body still needs the exact packet and blocks collection.
    pub fn reclaim_retired_payloads(&mut self) -> io::Result<usize> {
        if self.poisoned {
            return Err(invalid_packet_data(
                "operation inbox must be reopened after uncertain write",
            ));
        }
        let mut by_subject: BTreeMap<CrdtSubject, Vec<(PathBuf, InboundCatalogRecord)>> =
            BTreeMap::new();
        for entry in std::fs::read_dir(&self.catalog_dir)? {
            let path = entry?.path();
            let record: InboundCatalogRecord = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|e| invalid_packet_data(&e.to_string()))?;
            by_subject
                .entry(record.identity().subject)
                .or_default()
                .push((path, record));
        }
        for subject in self.root_tombstones.keys() {
            by_subject.entry(subject.clone()).or_default();
        }
        let mut signing_subjects = BTreeSet::new();
        for transfer in self.transfer_records(&self.signing_dir)? {
            signing_subjects.insert(transfer.packet.signed.bound.operation.subject);
        }
        let mut collected = 0;
        for (subject, records) in by_subject {
            let Some(packet) = self.packets.get(&subject).cloned() else {
                continue;
            };
            // A direct gossip ACK has no receiver retirement boundary. Keep
            // its forwarding copy until a separate retention rule exists.
            if packet_path(&self.direct_custody_dir, &subject).exists() {
                continue;
            }
            if !self.creation_policy_known(&subject) {
                continue;
            }
            if self.ledger.get(&subject).is_some() {
                continue;
            }
            let Some(receipt) = self
                .ledger
                .receipt(&subject)
                .or_else(|| self.custody_receipts.get(&subject))
            else {
                continue;
            };
            if crate::operation::OperationReceipt::from(&packet.signed.bound) != *receipt
                || packet_path(&self.root_dir, &subject).exists()
                || signing_subjects.contains(&subject)
            {
                continue;
            }
            let signed = packet.commitment();
            let bound = receipt.commitment;
            let mut eligible = true;
            for (_, record) in &records {
                let descriptor = record.identity();
                if descriptor.packet_commitment != signed
                    || descriptor.bound_commitment != bound
                    || descriptor.transfer_id
                        >= *self.fences.get(&descriptor.sender.0).unwrap_or(&0)
                    || [&self.transfer_dir, &self.terminal_dir, &self.report_ack_dir]
                        .iter()
                        .any(|dir| {
                            transfer_path(dir, &descriptor.sender, descriptor.transfer_id).exists()
                        })
                {
                    eligible = false;
                    break;
                }
            }
            if !eligible {
                continue;
            }
            self.poisoned = true;
            for (path, record) in records {
                if let InboundCatalogRecord::Full(transfer) = record {
                    let descriptor = CompactInboundTransfer::from(&transfer);
                    replace_file(
                        &self.catalog_dir,
                        &path,
                        &serde_json::to_vec(&descriptor)
                            .map_err(|e| invalid_packet_data(&e.to_string()))?,
                    )?;
                }
            }
            let marker = KnownCreationPolicy {
                version: 2,
                subject: subject.clone(),
                bound_commitment: bound,
                creation_commitment: creation_commitment(&packet),
            };
            write_exact_file(
                &self.known_policy_dir,
                &packet_path(&self.known_policy_dir, &subject),
                &serde_json::to_vec(&marker)
                    .map_err(|error| invalid_packet_data(&error.to_string()))?,
            )?;
            self.known_policy
                .insert(subject.clone(), creation_commitment(&packet));
            // All identity witnesses are fsynced before unlinking the final
            // signed custody copy. A crash between these steps is recoverable.
            std::fs::remove_file(packet_path(&self.packet_dir, &subject))?;
            crate::store::backend::fsync_dir(&self.packet_dir)?;
            let custody_marker = packet_path(&self.custody_dir, &subject);
            if custody_marker.exists() {
                std::fs::remove_file(custody_marker)?;
                crate::store::backend::fsync_dir(&self.custody_dir)?;
            }
            self.packets.remove(&subject);
            self.compacted_packets.insert(subject, (signed, bound));
            self.poisoned = false;
            collected += 1;
        }
        Ok(collected)
    }

    /// Application bodies must have a local owner, an origin, or a durable
    /// Authority intake. Transit custody never enters this ledger.
    fn verify_application_bodies(&self, owned: &OperationOwnedPrefixes) -> Result<(), OutboxError> {
        let signing_subjects: BTreeSet<_> = self
            .signing_transfers()?
            .into_iter()
            .map(|transfer| transfer.packet.signed.bound.operation.subject)
            .collect();
        for bound in self.ledger.iter() {
            let subject = &bound.operation.subject;
            if !owned.owns_in_namespace(&subject.namespace_id, bound.operation.action.key())
                && !packet_path(&self.root_dir, subject).exists()
                && !self.root_tombstones.contains_key(subject)
                && !signing_subjects.contains(subject)
            {
                return Err(route_error("routed application body lacks a local role"));
            }
        }
        Ok(())
    }

    fn is_retired(&self, transfer: &OperationTransfer) -> bool {
        transfer.transfer_id < *self.fences.get(&transfer.sender.0).unwrap_or(&0)
    }

    pub fn mark_terminal(&mut self, transfer: &OperationTransfer) -> io::Result<()> {
        if self.poisoned
            || transfer.version != 2
            || transfer.destination.as_ref() != Some(&transfer.receiver)
        {
            return Err(invalid_packet_data("transfer is not terminal"));
        }
        let path = transfer_path(&self.transfer_dir, &transfer.sender, transfer.transfer_id);
        let bytes =
            serde_json::to_vec(transfer).map_err(|e| invalid_packet_data(&e.to_string()))?;
        if std::fs::read(path)? != bytes
            || self
                .packets
                .get(&transfer.packet.signed.bound.operation.subject)
                != Some(&transfer.packet)
        {
            return Err(invalid_packet_data(
                "terminal transfer was not durably received",
            ));
        }
        let path = transfer_path(&self.terminal_dir, &transfer.sender, transfer.transfer_id);
        self.poisoned = true;
        write_exact_file(&self.terminal_dir, &path, &bytes)?;
        self.poisoned = false;
        Ok(())
    }

    pub fn terminal_transfers(&self) -> io::Result<Vec<OperationTransfer>> {
        self.require_certain()?;
        let mut ready = self.transfer_records(&self.terminal_dir)?;
        ready.extend(self.transfer_records(&self.signing_dir)?);
        let mut seen = BTreeSet::new();
        ready.retain(|transfer| {
            !self.is_retired(transfer)
                && seen.insert((transfer.sender.clone(), transfer.transfer_id))
        });
        Ok(ready)
    }

    fn transfer_records(&self, dir: &Path) -> io::Result<Vec<OperationTransfer>> {
        self.require_certain()?;
        std::fs::read_dir(dir)?
            .map(|entry| {
                serde_json::from_slice(&std::fs::read(entry?.path())?)
                    .map_err(|e| invalid_packet_data(&e.to_string()))
            })
            .collect()
    }

    /// Authority processing custody is distinct from application placement.
    /// The exact inbound transfer and signed packet must already be durable.
    pub fn mark_signing_intake(&mut self, transfer: &OperationTransfer) -> io::Result<()> {
        if transfer.packet.signed.bound.declaration.mode != CertificationMode::Enabled {
            return Err(invalid_packet_data(
                "disabled operation cannot enter signing inbox",
            ));
        }
        if self.poisoned
            || transfer.version != 2
            || transfer.destination.as_ref() != Some(&transfer.receiver)
        {
            return Err(invalid_packet_data("transfer is not terminal"));
        }
        let bytes =
            serde_json::to_vec(transfer).map_err(|e| invalid_packet_data(&e.to_string()))?;
        if std::fs::read(transfer_path(
            &self.transfer_dir,
            &transfer.sender,
            transfer.transfer_id,
        ))? != bytes
            || self
                .packets
                .get(&transfer.packet.signed.bound.operation.subject)
                != Some(&transfer.packet)
        {
            return Err(invalid_packet_data(
                "signing intake lacks durable inbound transfer",
            ));
        }
        let path = transfer_path(&self.signing_dir, &transfer.sender, transfer.transfer_id);
        self.poisoned = true;
        write_exact_file(&self.signing_dir, &path, &bytes)?;
        self.poisoned = false;
        Ok(())
    }

    pub fn signing_packets(&self) -> io::Result<Vec<OperationPacket>> {
        Ok(self
            .transfer_records(&self.signing_dir)?
            .into_iter()
            .map(|transfer| transfer.packet)
            .collect())
    }

    pub fn signing_transfers(&self) -> io::Result<Vec<OperationTransfer>> {
        self.transfer_records(&self.signing_dir)
    }

    /// Drop a completed Authority intake only after the sender has retired
    /// this transfer and a certified ledger receipt has replaced its body.
    /// The caller must first establish that its own durable Authority root
    /// admitted this exact bound operation. The immutable transfer catalog
    /// remains to reject reuse of the sender-scoped ID after a restart.
    pub(crate) fn retire_issued_signing_intake(
        &mut self,
        transfer: &OperationTransfer,
    ) -> io::Result<bool> {
        if self.poisoned {
            return Err(invalid_packet_data(
                "operation inbox must be reopened after uncertain write",
            ));
        }
        let subject = &transfer.packet.signed.bound.operation.subject;
        if !self.is_retired(transfer)
            || self.ledger.receipt(subject)
                != Some(&crate::operation::OperationReceipt::from(
                    &transfer.packet.signed.bound,
                ))
        {
            return Ok(false);
        }
        let path = transfer_path(&self.signing_dir, &transfer.sender, transfer.transfer_id);
        if !path.exists() {
            return Ok(false);
        }
        let bytes = serde_json::to_vec(transfer)
            .map_err(|error| invalid_packet_data(&error.to_string()))?;
        if std::fs::read(&path)? != bytes
            || std::fs::read(transfer_path(
                &self.catalog_dir,
                &transfer.sender,
                transfer.transfer_id,
            ))? != bytes
        {
            return Err(invalid_packet_data(
                "issued signing intake lacks exact transfer catalog",
            ));
        }
        self.poisoned = true;
        std::fs::remove_file(path)?;
        crate::store::backend::fsync_dir(&self.signing_dir)?;
        self.poisoned = false;
        Ok(true)
    }

    pub fn completion_report_acked(&self, transfer: &OperationTransfer) -> bool {
        !self.poisoned
            && (self.is_retired(transfer)
                || transfer_path(&self.report_ack_dir, &transfer.sender, transfer.transfer_id)
                    .exists())
    }

    pub fn record_completion_receipt(
        &mut self,
        transfer: &OperationTransfer,
        receipt: OperationCompletionReceipt,
    ) -> io::Result<()> {
        let expected = OperationCompletionReceipt {
            version: 1,
            transfer_id: TransferId {
                sender: transfer.sender.clone(),
                sequence: transfer.transfer_id,
            },
            commitment: transfer.packet.commitment(),
        };
        if receipt != expected || self.poisoned {
            return Err(invalid_packet_data(
                "completion receipt does not match transfer",
            ));
        }
        let path = transfer_path(&self.transfer_dir, &transfer.sender, transfer.transfer_id);
        if std::fs::read(path)?
            != serde_json::to_vec(transfer).map_err(|e| invalid_packet_data(&e.to_string()))?
        {
            return Err(invalid_packet_data(
                "completion receipt lacks inbound transfer",
            ));
        }
        let path = transfer_path(&self.report_ack_dir, &transfer.sender, transfer.transfer_id);
        self.poisoned = true;
        write_exact_file(
            &self.report_ack_dir,
            &path,
            &serde_json::to_vec(&receipt).map_err(|e| invalid_packet_data(&e.to_string()))?,
        )?;
        self.poisoned = false;
        Ok(())
    }

    /// Local read includes every durably received operation body.
    pub fn ordinary_read(&self, namespace_id: &str, key: &str) -> OperationRead {
        if self.poisoned {
            return OperationRead::Unknown;
        }
        ordinary_read(self.ledger.history(), namespace_id, key)
    }

    /// Certified read uses only independently verified operation proofs that
    /// match exact bodies in this inbox. The returned basis retains those
    /// proofs so a caller can return them alongside the projected value.
    pub fn certified_read<'a>(
        &self,
        verified: &'a [VerifiedOperation],
        namespace_id: &str,
        key: &str,
    ) -> CertifiedOperationRead<'a> {
        if self.poisoned {
            return CertifiedOperationRead {
                result: CertifiedReadResult::Unknown,
                basis: Vec::new(),
                local_body_seen: false,
            };
        }
        let known_mode = self
            .policy_witnesses
            .get(&(namespace_id.to_owned(), key.to_owned()))
            .map(|packet| packet.signed.bound.declaration.mode)
            .or_else(|| {
                self.compacted_policy_witnesses
                    .get(&(namespace_id.to_owned(), key.to_owned()))
                    .map(|witness| witness.identity.declaration.mode)
            });
        let mut read = certified_read_with_known_mode(
            self.ledger.history(),
            verified,
            namespace_id,
            key,
            |bound| self.creation_policy_known(&bound.operation.subject),
            known_mode,
        );
        // A compacted operation with an authenticated creation witness can
        // establish Disabled even when no application body remains for this
        // key. The receipt carries the immutable declaration, not a value.
        if read.result == CertifiedReadResult::Unknown
            && self.ledger.receipts().any(|receipt| {
                receipt.subject.namespace_id == namespace_id
                    && receipt.declaration.key == key
                    && receipt.declaration.mode == CertificationMode::Disabled
                    && self.creation_policy_known(&receipt.subject)
            })
        {
            read.result = CertifiedReadResult::NotTargeted;
        }
        read
    }
}

async fn receive_http(
    State(inbox): State<Arc<Mutex<OperationInbox>>>,
    Json(packet): Json<OperationPacket>,
) -> Result<Json<OperationReceipt>, StatusCode> {
    // Ledger I/O is synchronous. Keep its serialization lock off the async
    // executor while preserving a single atomic ID choice per receiver.
    let result = tokio::task::spawn_blocking(move || {
        let mut guard = inbox.blocking_lock();
        guard.receive(packet)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match result {
        Ok(receipt) => Ok(Json(receipt)),
        Err(ReceiveError::Persist(PersistOperationError::Operation(
            crate::operation::OperationError::ConflictingContent(_),
        ))) => Err(StatusCode::CONFLICT),
        Err(ReceiveError::Persist(PersistOperationError::Operation(_))) => {
            Err(StatusCode::BAD_REQUEST)
        }
        Err(ReceiveError::Rejected(_)) => Err(StatusCode::FORBIDDEN),
        Err(ReceiveError::Persist(PersistOperationError::Io(_))) => {
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn read_http(
    State(inbox): State<Arc<Mutex<OperationInbox>>>,
    RoutePath((namespace_id, key)): RoutePath<(String, String)>,
) -> Result<Json<OperationRead>, StatusCode> {
    tokio::task::spawn_blocking(move || {
        let mut guard = inbox.blocking_lock();
        guard
            .recover_if_poisoned()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if guard
            .routed_pin_present()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        {
            return Err(StatusCode::FORBIDDEN);
        }
        Ok(Json(guard.ordinary_read(&namespace_id, &key)))
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}

/// Build a token-protected operation receiver route.
pub fn receiver_router(inbox: Arc<Mutex<OperationInbox>>, token: String) -> Router {
    Router::new()
        .route(
            OPERATION_RECEIVE_PATH,
            post(receive_http).layer(DefaultBodyLimit::max(DIRECT_OPERATION_BODY_LIMIT)),
        )
        .route(OPERATION_READ_PATH, get(read_http))
        .layer(middleware::from_fn(move |request, next| {
            let token = token.clone();
            require_bearer_token(request, next, token)
        }))
        .with_state(inbox)
}

/// Test/standalone receiver with a fixed node identity for transfer ACKs.
pub fn receiver_router_for_node(
    inbox: Arc<Mutex<OperationInbox>>,
    token: String,
    node_id: NodeId,
) -> Router {
    let transfer_inbox = inbox.clone();
    let transfer_node = node_id.clone();
    let transfer_token = token.clone();
    Router::new()
        .route(
            OPERATION_TRANSFER_PATH,
            post(move |Json(transfer): Json<OperationTransfer>| {
                let inbox = transfer_inbox.clone();
                let node = transfer_node.clone();
                async move {
                    tokio::task::spawn_blocking(move || {
                        inbox.blocking_lock().receive_transfer(transfer, &node)
                    })
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                    .map(Json)
                    .map_err(|error| relay_error_status(error.into()))
                }
            })
            .layer(DefaultBodyLimit::max(DIRECT_OPERATION_BODY_LIMIT)),
        )
        .layer(middleware::from_fn(move |request, next| {
            let token = transfer_token.clone();
            require_bearer_token(request, next, token)
        }))
        .merge(receiver_router(inbox, token))
}

/// A fixed adjacency reconciles every custody packet against durable send
/// obligations before serving traffic. The caller must provide a trusted,
/// stable list of adjacent destinations; gossip membership is insufficient.
#[derive(Clone)]
pub struct OperationRelay {
    inbox: Arc<Mutex<OperationInbox>>,
    outbox: Arc<Mutex<OperationOutbox>>,
    neighbors: Arc<OperationNeighborManifest>,
    owned: Arc<OperationOwnedPrefixes>,
    node_id: NodeId,
    routed: Option<Arc<RoutedCustody>>,
}

#[derive(Clone)]
struct RoutedCustody {
    routes: OperationRouteManifest,
    required: OperationRequiredDestinations,
    authorities: Vec<AuthorityTerminalRole>,
    submit_authority_policy: Option<Arc<SystemNamespace>>,
    live_authority_policy: Option<Arc<std::sync::RwLock<SystemNamespace>>>,
}

impl RoutedCustody {
    fn authority_manifest_digest(&self, node: &NodeId) -> io::Result<[u8; 32]> {
        routed_authority_manifest_digest(
            node,
            &self.authorities,
            self.submit_authority_policy.as_deref(),
        )
    }

    fn accepts_signing_intake(&self, node: &NodeId, packet: &OperationPacket) -> bool {
        packet.signed.bound.declaration.mode == CertificationMode::Enabled
            && self
                .authorities
                .iter()
                .any(|role| role.accepts_terminal(node, packet))
    }

    fn accepts_disabled_terminal(&self, node: &NodeId, packet: &OperationPacket) -> bool {
        // A fixed Authority destination still completes custody for an
        // Eventual operation, but never feeds that operation to the signer.
        let bound = &packet.signed.bound;
        bound.declaration.mode == CertificationMode::Disabled
            && self
                .required
                .destinations_for(
                    Some(&bound.operation.subject.namespace_id),
                    bound.operation.action.key(),
                )
                .is_some_and(|destinations| destinations.contains(node))
            && self
                .authorities
                .iter()
                .any(|role| role.accepts_terminal(node, packet))
    }

    fn may_need_local_signing(&self, node: &NodeId, origin: &RootTombstone) -> bool {
        origin.mode == CertificationMode::Enabled
            && self.authorities.iter().any(|role| {
                role.scope.namespace_id == origin.subject.namespace_id
                    && origin.key.starts_with(&role.scope.key_prefix)
                    && role
                        .namespace
                        .operation_authorities_for_key(&origin.subject.namespace_id, &origin.key)
                        .is_some_and(|definition| definition.authority_nodes.contains(node))
            })
    }
}

/// Bind the immutable mounted policy and local terminal roles. The mounted
/// snapshot is reloaded on restart; only new roots compare their own effective
/// policy with the live namespace.
pub fn routed_authority_manifest_digest(
    node: &NodeId,
    authorities: &[AuthorityTerminalRole],
    submit_policy: Option<&SystemNamespace>,
) -> io::Result<[u8; 32]> {
    let terminal_digest = authority_terminal_manifest_digest(node, authorities)?;
    let Some(policy) = submit_policy else {
        return Ok(terminal_digest);
    };
    let policy_bytes = mounted_authority_policy_bytes(policy)?;
    let mut hash = Sha256::new();
    hash.update(b"asteroidb/operation-routed-authority-policy/v2");
    hash.update(terminal_digest);
    hash.update((policy_bytes.len() as u64).to_be_bytes());
    hash.update(policy_bytes);
    Ok(hash.finalize().into())
}

/// The same Authority assignment pin used by routed relay startup.
pub fn authority_terminal_manifest_digest(
    node: &NodeId,
    authorities: &[AuthorityTerminalRole],
) -> io::Result<[u8; 32]> {
    let mut roles = Vec::new();
    for role in authorities {
        roles.push((
            role.scope.clone(),
            mounted_authority_policy_bytes(&role.namespace)?,
        ));
    }
    roles.sort_by(|a, b| {
        (&a.0.namespace_id, &a.0.key_prefix).cmp(&(&b.0.namespace_id, &b.0.key_prefix))
    });
    let mut hash = Sha256::new();
    hash.update(b"asteroidb/operation-authority-terminals/v2");
    hash.update((node.0.len() as u64).to_be_bytes());
    hash.update(node.0.as_bytes());
    hash.update(
        serde_json::to_vec(&roles).map_err(|error| invalid_packet_data(&error.to_string()))?,
    );
    Ok(hash.finalize().into())
}

fn completed_root_sends(
    config: &RoutedCustody,
    node: &NodeId,
    origin: &RootTombstone,
    entries: &[&OutboxEntry],
) -> bool {
    let Some(destinations) = config
        .required
        .destinations_for(Some(&origin.subject.namespace_id), &origin.key)
    else {
        return false;
    };
    if !root_destinations_cover_authorities(
        config,
        &origin.subject.namespace_id,
        &origin.key,
        origin.mode,
        destinations,
    ) {
        return false;
    }
    destinations
        .iter()
        .filter(|destination| *destination != node)
        .all(|destination| {
            entries.iter().any(|entry| {
                entry.sender == *node
                    && entry.parent.is_none()
                    && entry.destination.as_ref() == Some(destination)
                    && entry.subject == origin.subject
                    && entry.commitment == origin.packet_commitment
                    && (entry.closed || (entry.receipt.is_some() && entry.completion.is_some()))
            })
        })
}

/// A root with an Enabled declaration retains its signed recovery copy until
/// the fixed delivery set includes every signing Authority. A certificate
/// may arrive by another path, but it cannot erase still missing custody work.
fn root_destinations_cover_authorities(
    config: &RoutedCustody,
    namespace_id: &str,
    key: &str,
    mode: CertificationMode,
    destinations: &[NodeId],
) -> bool {
    if mode == CertificationMode::Disabled {
        return true;
    }
    let Some(policy) = &config.submit_authority_policy else {
        return false;
    };
    let Some(authority) = policy.operation_authorities_for_key(namespace_id, key) else {
        return false;
    };
    let Some(placement) = policy.operation_placement_for_key(namespace_id, key) else {
        return false;
    };
    placement.certified
        && placement.key_range.prefix == authority.key_range.prefix
        && !authority.authority_nodes.is_empty()
        && authority
            .authority_nodes
            .iter()
            .all(|node| destinations.contains(node))
}

/// Trusted, immutable Authority role for a routed terminal destination.
/// The namespace is checked again for every packet to respect shadowed ranges.
#[derive(Clone)]
pub struct AuthorityTerminalRole {
    pub namespace: SystemNamespace,
    pub scope: OperationScope,
}

impl AuthorityTerminalRole {
    fn accepts_terminal(&self, node: &NodeId, packet: &OperationPacket) -> bool {
        let bound = &packet.signed.bound;
        let key = bound.operation.action.key();
        self.scope.contains(bound)
            && self
                .namespace
                .operation_authorities_for_key(&self.scope.namespace_id, key)
                .is_some_and(|definition| {
                    definition.key_range.prefix == self.scope.key_prefix
                        && definition.authority_nodes.contains(node)
                })
            && self
                .namespace
                .operation_placement_for_key(&self.scope.namespace_id, key)
                .is_some_and(|policy| {
                    policy.certified && policy.key_range.prefix == self.scope.key_prefix
                })
    }
}

/// Check the local static routed configuration without opening or pinning
/// operation state. Startup can run this before writing immutable manifests.
pub fn validate_routed_relay_configuration(
    neighbors: &OperationNeighborManifest,
    routes: &OperationRouteManifest,
    authorities: &[AuthorityTerminalRole],
) -> Result<(), OutboxError> {
    for (index, role) in authorities.iter().enumerate() {
        if authorities[..index].iter().any(|other| {
            other.scope.namespace_id == role.scope.namespace_id
                && other.scope.key_prefix == role.scope.key_prefix
        }) {
            return Err(route_error("Authority terminal scope is duplicated"));
        }
        let definition = role
            .namespace
            .operation_authority_definition(&role.scope.namespace_id, &role.scope.key_prefix);
        let policy = role
            .namespace
            .operation_placement_policy(&role.scope.namespace_id, &role.scope.key_prefix);
        if role.scope.namespace_id.is_empty()
            || definition.is_none_or(|def| !def.authority_nodes.contains(neighbors.self_id()))
            || policy.is_none_or(|policy| !policy.certified)
        {
            return Err(route_error(
                "node lacks static Authority role for signing intake",
            ));
        }
    }
    for route in routes
        .rows()
        .iter()
        .filter(|row| row.source == *neighbors.self_id())
    {
        let neighbor = neighbors.get(&route.next_hop).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "route next hop has no local address",
            )
        })?;
        let permitted = match route.namespace_id.as_deref() {
            Some(namespace_id) => neighbor.permits(namespace_id, &route.key_prefix),
            None => neighbor
                .key_prefixes
                .iter()
                .any(|prefix| route.key_prefix.starts_with(prefix)),
        };
        if !permitted {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "route exceeds local neighbor permission",
            )
            .into());
        }
    }
    Ok(())
}

impl OperationRelay {
    /// Check legacy prefixes that apply to every namespace. Use
    /// `owns_namespace_key` for an operation with a known namespace.
    pub fn owns_key(&self, key: &str) -> bool {
        self.owned.owns(key)
    }

    pub fn owns_namespace_key(&self, namespace_id: &str, key: &str) -> bool {
        self.owned.owns_in_namespace(namespace_id, key)
    }

    pub fn inbox(&self) -> Arc<Mutex<OperationInbox>> {
        Arc::clone(&self.inbox)
    }

    /// Replace certified, projection-redundant ledger bodies with durable ID
    /// receipts. The signed inbox packet and every send descriptor remain.
    /// Transport enrollment is checked against the pinned routed manifest at
    /// the same time as compaction, under the inbox and outbox locks.
    pub(super) fn compact_certified_ledger_bodies(
        &self,
        verified: &[VerifiedOperation],
    ) -> Result<usize, PersistOperationError> {
        let Some(config) = &self.routed else {
            return Ok(0);
        };
        let mut inbox = self.inbox.blocking_lock();
        inbox
            .recover_if_poisoned()
            .map_err(|error| io::Error::other(format!("{error:?}")))?;
        let outbox = self.outbox.blocking_lock();
        let Some(entries) = outbox.gc_entries() else {
            return Ok(0);
        };
        let entries: Vec<_> = entries.collect();
        let roots = inbox.root_packets().map_err(PersistOperationError::Io)?;
        let transfers = inbox
            .catalog_transfers()
            .map_err(PersistOperationError::Io)?;
        let signing = inbox
            .signing_transfers()
            .map_err(PersistOperationError::Io)?;
        let mut accepted = BTreeSet::new();
        for proof in verified {
            let bound = proof.bound();
            if inbox.ledger.get(&bound.operation.subject) == Some(bound)
                && inbox.creation_policy_known(&bound.operation.subject)
            {
                accepted.insert(bound.operation.subject.clone());
            }
        }
        let mut released = BTreeSet::new();
        for bound in inbox.ledger.iter() {
            let id = &bound.operation.subject;
            if !accepted.contains(id) {
                continue;
            }
            let Some(packet) = inbox.packets.get(id) else {
                continue;
            };
            if packet.signed.bound != *bound {
                continue;
            }
            let related: Vec<_> = entries
                .iter()
                .filter(|entry| entry.subject == *id)
                .collect();
            if related
                .iter()
                .any(|entry| entry.commitment != packet.commitment())
            {
                continue;
            }
            let key = bound.operation.action.key();
            let root = roots.iter().any(|root| root == packet);
            let inbound: Vec<_> = transfers
                .iter()
                .filter(|transfer| transfer.packet == *packet)
                .collect();
            if !root && inbound.is_empty() {
                continue;
            }
            if root {
                let Some(destinations) = config
                    .required
                    .destinations_for(Some(&id.namespace_id), key)
                else {
                    continue;
                };
                if !root_destinations_cover_authorities(
                    config,
                    &id.namespace_id,
                    key,
                    bound.declaration.mode,
                    destinations,
                ) {
                    continue;
                }
                if destinations.iter().any(|destination| {
                    destination != &self.node_id
                        && !related.iter().any(|entry| {
                            entry.sender == self.node_id
                                && entry.parent.is_none()
                                && entry.destination.as_ref() == Some(destination)
                        })
                }) {
                    continue;
                }
            }
            if inbound.iter().any(|transfer| {
                let Some(destination) = transfer.destination.as_ref() else {
                    return true;
                };
                if destination == &self.node_id {
                    !self.owned.owns_in_namespace(&id.namespace_id, key)
                        && !signing.iter().any(|item| item == *transfer)
                } else {
                    let parent = TransferId {
                        sender: transfer.sender.clone(),
                        sequence: transfer.transfer_id,
                    };
                    !related.iter().any(|entry| {
                        entry.parent.as_ref() == Some(&parent)
                            && entry.destination.as_ref() == Some(destination)
                    })
                }
            }) {
                continue;
            }
            // A matching verified certificate discharges local signing intake.
            released.insert(id.clone());
        }
        let history = inbox.ledger.history();
        let mut candidates = BTreeSet::new();
        let keys: BTreeSet<_> = history
            .iter()
            .map(|bound| {
                (
                    bound.operation.subject.namespace_id.clone(),
                    bound.operation.action.key().to_string(),
                )
            })
            .collect();
        for (namespace, key) in keys {
            candidates.extend(deleted_value_candidates(
                history, &namespace, &key, &accepted, &released,
            ));
        }
        // Choose one retained winner per key. Equal HLC/value writes dominate
        // each other in the predicate; applying it to every accepted write
        // would reclaim all tied values.
        for (namespace, key) in history
            .iter()
            .map(|bound| {
                (
                    bound.operation.subject.namespace_id.clone(),
                    bound.operation.action.key().to_string(),
                )
            })
            .collect::<BTreeSet<_>>()
        {
            if history.iter().any(|bound| {
                bound.operation.subject.namespace_id == namespace
                    && bound.operation.action.key() == key
                    && accepted.contains(&bound.operation.subject)
                    && matches!(
                        bound.operation.action,
                        crate::operation::CandidateKvAction::Delete { .. }
                    )
            }) {
                continue;
            }
            let winner = history
                .iter()
                .filter(|bound| {
                    bound.operation.subject.namespace_id == namespace
                        && bound.operation.action.key() == key
                        && accepted.contains(&bound.operation.subject)
                })
                .filter_map(|bound| match &bound.operation.action {
                    crate::operation::CandidateKvAction::Create {
                        timestamp,
                        value_bytes,
                        ..
                    }
                    | crate::operation::CandidateKvAction::Add {
                        timestamp,
                        value_bytes,
                        ..
                    } => Some((timestamp, value_bytes, &bound.operation.subject)),
                    crate::operation::CandidateKvAction::Delete { .. } => None,
                })
                .max();
            if let Some((_, _, winner_id)) = winner {
                candidates.extend(dominated_value_candidates(
                    history, winner_id, &accepted, &released,
                ));
                candidates.remove(winner_id);
            }
        }
        let mut count = 0;
        if !candidates.is_empty() {
            inbox.poisoned = true;
            for id in candidates {
                if inbox.ledger.compact_and_sync(&id)? {
                    count += 1;
                }
            }
        }
        if inbox.ledger.checkpoint_pending() {
            inbox.poisoned = true;
            inbox.ledger.checkpoint_and_sync()?;
            inbox.poisoned = false;
        }
        // A crash may have happened after checkpoint publication but before
        // the inbox removed a retired signed packet or its catalog payload.
        inbox
            .reclaim_retired_payloads()
            .map_err(PersistOperationError::Io)?;
        drop(outbox);
        drop(inbox);
        self.reclaim_completed_roots()
            .map_err(PersistOperationError::Io)?;
        Ok(count)
    }

    /// Collect a certified origin after every required destination has
    /// durably completed. The exact ID/commitment tombstone survives retries
    /// and is checked against the outbox before startup accepts traffic.
    pub fn reclaim_completed_roots(&self) -> io::Result<usize> {
        let Some(config) = &self.routed else {
            return Ok(0);
        };
        let mut inbox = self.inbox.blocking_lock();
        inbox
            .recover_if_poisoned()
            .map_err(|error| io::Error::other(format!("{error:?}")))?;
        let outbox = self.outbox.blocking_lock();
        let entries = outbox
            .gc_entries()
            .ok_or_else(|| invalid_packet_data("uncertain outbox cannot release roots"))?
            .collect::<Vec<_>>();
        let mut collected = 0;
        for packet in inbox.root_packets()? {
            let bound = &packet.signed.bound;
            let origin = RootTombstone {
                version: 1,
                subject: bound.operation.subject.clone(),
                key: bound.operation.action.key().to_owned(),
                mode: bound.declaration.mode,
                packet_commitment: packet.commitment(),
                bound_commitment: bound.commitment(),
            };
            if (config.may_need_local_signing(&self.node_id, &origin)
                && !inbox.has_verified_local_issuance(&origin))
                || !completed_root_sends(config, &self.node_id, &origin, &entries)
                || inbox.ledger.receipt(&origin.subject).is_none_or(|receipt| {
                    receipt != &crate::operation::OperationReceipt::from(bound)
                })
            {
                continue;
            }
            inbox.reclaim_root(&packet)?;
            collected += 1;
        }
        if collected > 0 {
            inbox.reclaim_retired_payloads()?;
        }
        Ok(collected)
    }

    pub async fn new(
        inbox: Arc<Mutex<OperationInbox>>,
        outbox: Arc<Mutex<OperationOutbox>>,
        neighbors: OperationNeighborManifest,
        owned: OperationOwnedPrefixes,
    ) -> Result<Self, OutboxError> {
        Self::new_with_mode(inbox, outbox, neighbors, owned, None).await
    }

    pub async fn new_routed(
        inbox: Arc<Mutex<OperationInbox>>,
        outbox: Arc<Mutex<OperationOutbox>>,
        neighbors: OperationNeighborManifest,
        owned: OperationOwnedPrefixes,
        routes: OperationRouteManifest,
        required: OperationRequiredDestinations,
    ) -> Result<Self, OutboxError> {
        Self::new_routed_with_authority(inbox, outbox, neighbors, owned, routes, required, None)
            .await
    }

    pub async fn new_routed_with_authority(
        inbox: Arc<Mutex<OperationInbox>>,
        outbox: Arc<Mutex<OperationOutbox>>,
        neighbors: OperationNeighborManifest,
        owned: OperationOwnedPrefixes,
        routes: OperationRouteManifest,
        required: OperationRequiredDestinations,
        authority: Option<AuthorityTerminalRole>,
    ) -> Result<Self, OutboxError> {
        Self::new_routed_with_authorities(
            inbox,
            outbox,
            neighbors,
            owned,
            routes,
            required,
            authority.into_iter().collect(),
        )
        .await
    }

    /// Mount independently assigned Authority terminal ranges.
    /// All roles are checked before any routed custody state is recovered.
    pub async fn new_routed_with_authorities(
        inbox: Arc<Mutex<OperationInbox>>,
        outbox: Arc<Mutex<OperationOutbox>>,
        neighbors: OperationNeighborManifest,
        owned: OperationOwnedPrefixes,
        routes: OperationRouteManifest,
        required: OperationRequiredDestinations,
        authorities: Vec<AuthorityTerminalRole>,
    ) -> Result<Self, OutboxError> {
        Self::new_routed_with_policy(
            inbox,
            outbox,
            neighbors,
            owned,
            routes,
            required,
            authorities,
            None,
            None,
        )
        .await
    }

    /// Mount routed custody with the live Authority assignment used to check
    /// that an Enabled local submit reaches every required signing Authority.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_routed_with_authorities_and_policy(
        inbox: Arc<Mutex<OperationInbox>>,
        outbox: Arc<Mutex<OperationOutbox>>,
        neighbors: OperationNeighborManifest,
        owned: OperationOwnedPrefixes,
        routes: OperationRouteManifest,
        required: OperationRequiredDestinations,
        authorities: Vec<AuthorityTerminalRole>,
        mounted_namespace: SystemNamespace,
        namespace: Arc<std::sync::RwLock<SystemNamespace>>,
    ) -> Result<Self, OutboxError> {
        Self::new_routed_with_policy(
            inbox,
            outbox,
            neighbors,
            owned,
            routes,
            required,
            authorities,
            Some(Arc::new(mounted_namespace)),
            Some(namespace),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn new_routed_with_policy(
        inbox: Arc<Mutex<OperationInbox>>,
        outbox: Arc<Mutex<OperationOutbox>>,
        neighbors: OperationNeighborManifest,
        owned: OperationOwnedPrefixes,
        routes: OperationRouteManifest,
        required: OperationRequiredDestinations,
        authorities: Vec<AuthorityTerminalRole>,
        submit_authority_policy: Option<Arc<SystemNamespace>>,
        live_authority_policy: Option<Arc<std::sync::RwLock<SystemNamespace>>>,
    ) -> Result<Self, OutboxError> {
        validate_routed_relay_configuration(&neighbors, &routes, &authorities)?;
        Self::new_with_mode(
            inbox,
            outbox,
            neighbors,
            owned,
            Some(RoutedCustody {
                routes,
                required,
                authorities,
                submit_authority_policy,
                live_authority_policy,
            }),
        )
        .await
    }

    async fn new_with_mode(
        inbox: Arc<Mutex<OperationInbox>>,
        outbox: Arc<Mutex<OperationOutbox>>,
        neighbors: OperationNeighborManifest,
        owned: OperationOwnedPrefixes,
        routed: Option<RoutedCustody>,
    ) -> Result<Self, OutboxError> {
        let (packets, roots, transfers) = {
            let guard = inbox.lock().await;
            guard.verify_owned_manifest(owned.digest())?;
            guard.verify_application_bodies(&owned)?;
            if let Some(config) = &routed {
                guard.verify_authority_manifest(
                    config.authority_manifest_digest(neighbors.self_id())?,
                )?;
            }
            guard.verify_transfer_receiver(neighbors.self_id())?;
            if routed.is_some() {
                for transfer in guard.signing_transfers()? {
                    if !routed.as_ref().is_some_and(|config| {
                        config.accepts_signing_intake(neighbors.self_id(), &transfer.packet)
                    }) {
                        return Err(route_error(
                            "recovered signing intake lacks static Authority role",
                        ));
                    }
                }
            }
            (
                if routed.is_none() {
                    guard.packets()
                } else {
                    Vec::new()
                },
                if routed.is_some() {
                    guard.root_packets()?
                } else {
                    Vec::new()
                },
                guard.received_transfers()?,
            )
        };
        if routed.is_none() && transfers.iter().any(|transfer| transfer.version != 1) {
            return Err(route_error(
                "routed transfer records cannot be opened in legacy mode",
            ));
        }
        {
            // Match runtime's inbox -> outbox lock order even when a caller
            // constructs another relay around already live shared handles.
            let mut guard = inbox.lock().await;
            let mut outbox_guard = outbox.lock().await;
            if outbox_guard.is_routed() != routed.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "outbox custody mode differs from relay mode",
                )
                .into());
            }
            if let Some(config) = &routed {
                if let Some(policy) = &config.submit_authority_policy {
                    guard.ensure_mounted_authority_policy(policy)?;
                }
                outbox_guard.ensure_routed_manifest(routed_custody_manifest_digest(
                    &config.routes,
                    &config.required,
                    &neighbors,
                )?)?;
                guard.ensure_owned_manifest(owned.digest())?;
                guard.ensure_authority_manifest(
                    config.authority_manifest_digest(neighbors.self_id())?,
                )?;
                let origins = guard.root_tombstones().cloned().collect::<Vec<_>>();
                let entries = outbox_guard
                    .gc_entries()
                    .ok_or_else(|| route_error("uncertain outbox cannot validate reclaimed roots"))?
                    .collect::<Vec<_>>();
                for origin in &origins {
                    if (config.may_need_local_signing(neighbors.self_id(), origin)
                        && !guard.has_verified_local_issuance(origin))
                        || !completed_root_sends(config, neighbors.self_id(), origin, &entries)
                    {
                        return Err(route_error(
                            "reclaimed root lacks completed sends or local signing safety",
                        ));
                    }
                }
                for origin in origins {
                    guard.finish_reclaimed_root(&origin.subject)?;
                }
                guard.reclaim_retired_payloads()?;
            } else {
                guard.ensure_owned_manifest(owned.digest())?;
            }
            for packet in packets {
                ensure_local_delivery_capacity(&packet, &neighbors, None).map_err(|_| {
                    route_error("saved operation exceeds direct delivery body limit")
                })?;
                for neighbor in neighbors.permitted_for(
                    &packet.signed.bound.operation.subject.namespace_id,
                    packet.signed.bound.operation.action.key(),
                ) {
                    outbox_guard.ensure_enqueued_to(
                        neighbor.node_id.clone(),
                        neighbor.addr.clone(),
                        packet.clone(),
                    )?;
                }
            }
            if let Some(config) = &routed {
                for packet in roots {
                    ensure_local_delivery_capacity(&packet, &neighbors, Some(config)).map_err(
                        |_| route_error("saved root exceeds routed delivery body limit"),
                    )?;
                    enroll_roots(&mut outbox_guard, &neighbors, config, &packet)?;
                }
                for transfer in transfers {
                    validate_routed_transfer(&transfer, neighbors.self_id(), config)?;
                    if neighbors.get(&transfer.sender).is_none() {
                        return Err(route_error("routed inbound sender has no return address"));
                    }
                    ensure_inbound_transfer_capacity(
                        &transfer,
                        neighbors.self_id(),
                        &neighbors,
                        Some(config),
                    )
                    .map_err(|_| {
                        route_error("saved transfer exceeds routed delivery body limit")
                    })?;
                    if transfer.destination.as_ref() == Some(neighbors.self_id()) {
                        let owner = owned.owns_in_namespace(
                            &transfer.packet.signed.bound.operation.subject.namespace_id,
                            transfer.packet.signed.bound.operation.action.key(),
                        );
                        let authority =
                            config.accepts_signing_intake(neighbors.self_id(), &transfer.packet);
                        let disabled_terminal =
                            config.accepts_disabled_terminal(neighbors.self_id(), &transfer.packet);
                        if owner || disabled_terminal {
                            guard.mark_terminal(&transfer)?;
                        }
                        if authority {
                            guard.mark_signing_intake(&transfer)?;
                        }
                        if !owner && !authority && !disabled_terminal {
                            return Err(route_error(
                                "terminal transfer has no local application or Authority intake",
                            ));
                        }
                    }
                    enroll_child(&mut outbox_guard, &neighbors, config, &transfer)?;
                }
            }
        }
        Ok(Self {
            node_id: neighbors.self_id().clone(),
            inbox,
            outbox,
            neighbors: Arc::new(neighbors),
            owned: Arc::new(owned),
            routed: routed.map(Arc::new),
        })
    }

    fn receive(&self, packet: OperationPacket) -> Result<OperationReceipt, RelayReceiveError> {
        if self.routed.is_some() {
            return Err(RelayReceiveError::NotOwned);
        }
        ensure_local_delivery_capacity(&packet, &self.neighbors, None)?;
        let bound = &packet.signed.bound;
        let apply_to_ledger = self.owned.owns_in_namespace(
            &bound.operation.subject.namespace_id,
            bound.operation.action.key(),
        );
        let receipt = if apply_to_ledger {
            self.inbox.blocking_lock().receive(packet.clone())?
        } else {
            self.inbox
                .blocking_lock()
                .receive_direct_custody(packet.clone())?
        };
        let mut outbox = self.outbox.blocking_lock();
        for neighbor in self.neighbors.permitted_for(
            &packet.signed.bound.operation.subject.namespace_id,
            packet.signed.bound.operation.action.key(),
        ) {
            if let Err(error) = outbox.ensure_enqueued_to(
                neighbor.node_id.clone(),
                neighbor.addr.clone(),
                packet.clone(),
            ) {
                tracing::warn!(error = %error, "durable received operation awaits neighbor enrollment retry");
            }
        }
        Ok(receipt)
    }

    fn receive_transfer(
        &self,
        transfer: OperationTransfer,
    ) -> Result<OperationTransferReceipt, RelayReceiveError> {
        if transfer.receiver != self.node_id {
            return Err(RelayReceiveError::NotOwned);
        }
        let mounted_accepts_signing_intake = self.routed.as_ref().is_some_and(|config| {
            transfer.destination.as_ref() == Some(&self.node_id)
                && config.accepts_signing_intake(&self.node_id, &transfer.packet)
        });
        let live_policy_guard = self
            .routed
            .as_ref()
            .filter(|_| mounted_accepts_signing_intake)
            .and_then(|config| config.live_authority_policy.as_ref())
            .map(|policy| {
                policy
                    .read()
                    .map_err(|_| route_error("operation Authority policy is poisoned"))
            })
            .transpose()?;
        let accepts_signing_intake = mounted_accepts_signing_intake
            && match (&self.routed, &live_policy_guard) {
                (Some(config), Some(live)) => {
                    let mounted = config.submit_authority_policy.as_ref().ok_or_else(|| {
                        route_error("enabled terminal requires trusted Authority routing policy")
                    })?;
                    let bound = &transfer.packet.signed.bound;
                    same_effective_authority_policy(
                        mounted,
                        live,
                        &bound.operation.subject.namespace_id,
                        &bound.declaration.key,
                    )
                    .map_err(OutboxError::Io)?
                }
                _ => true,
            };
        if let Some(config) = &self.routed {
            validate_routed_transfer(&transfer, &self.node_id, config)?;
            if self.neighbors.get(&transfer.sender).is_none() {
                return Err(RelayReceiveError::NotOwned);
            }
            if transfer.destination.as_ref() == Some(&self.node_id)
                && !self.owned.owns_in_namespace(
                    &transfer.packet.signed.bound.operation.subject.namespace_id,
                    transfer.packet.signed.bound.operation.action.key(),
                )
                && !accepts_signing_intake
                && !config.accepts_disabled_terminal(&self.node_id, &transfer.packet)
            {
                return Err(RelayReceiveError::NotOwned);
            }
        } else if transfer.version != 1 {
            return Err(RelayReceiveError::NotOwned);
        }
        ensure_inbound_transfer_capacity(
            &transfer,
            &self.node_id,
            &self.neighbors,
            self.routed.as_deref(),
        )?;
        let receipt = self.inbox.blocking_lock().receive_transfer_with_role(
            transfer.clone(),
            &self.node_id,
            (self.routed.is_some()
                && transfer.destination.as_ref() == Some(&self.node_id)
                && accepts_signing_intake)
                || self.owned.owns_in_namespace(
                    &transfer.packet.signed.bound.operation.subject.namespace_id,
                    transfer.packet.signed.bound.operation.action.key(),
                ),
        )?;
        if self.routed.is_some() && transfer.destination.as_ref() == Some(&self.node_id) {
            let mut inbox = self.inbox.blocking_lock();
            let key = transfer.packet.signed.bound.operation.action.key();
            if self.owned.owns_in_namespace(
                &transfer.packet.signed.bound.operation.subject.namespace_id,
                key,
            ) || self.routed.as_ref().is_some_and(|config| {
                config.accepts_disabled_terminal(&self.node_id, &transfer.packet)
            }) {
                inbox
                    .mark_terminal(&transfer)
                    .map_err(|e| RelayReceiveError::Outbox(OutboxError::Io(e)))?;
            }
            if accepts_signing_intake {
                inbox
                    .mark_signing_intake(&transfer)
                    .map_err(|e| RelayReceiveError::Outbox(OutboxError::Io(e)))?;
            }
        }
        let mut outbox = self.outbox.blocking_lock();
        if let Some(config) = &self.routed {
            if let Err(error) = enroll_child(&mut outbox, &self.neighbors, config, &transfer) {
                tracing::warn!(error = %error, "durable received transfer awaits routed enrollment retry");
            }
            return Ok(receipt);
        }
        for neighbor in self.neighbors.permitted_for(
            &transfer.packet.signed.bound.operation.subject.namespace_id,
            transfer.packet.signed.bound.operation.action.key(),
        ) {
            if let Err(error) = outbox.ensure_enqueued_to(
                neighbor.node_id.clone(),
                neighbor.addr.clone(),
                transfer.packet.clone(),
            ) {
                tracing::warn!(error = %error, "durable received transfer awaits neighbor enrollment retry");
            }
        }
        Ok(receipt)
    }

    fn submit(&self, packet: OperationPacket) -> Result<OperationReceipt, RelayReceiveError> {
        if !self.owned.owns_in_namespace(
            &packet.signed.bound.operation.subject.namespace_id,
            packet.signed.bound.operation.action.key(),
        ) {
            return Err(RelayReceiveError::NotOwned);
        }
        if let Some(config) = &self.routed {
            // Hold the live namespace read lock through root persistence and
            // enrollment. Control-plane changes cannot split those two steps.
            let live_policy_guard = config
                .live_authority_policy
                .as_ref()
                .map(|policy| {
                    policy
                        .read()
                        .map_err(|_| route_error("operation Authority policy is poisoned"))
                })
                .transpose()?;
            if packet.signed.bound.declaration.mode == CertificationMode::Enabled
                && let Some(live) = &live_policy_guard
            {
                let mounted = config.submit_authority_policy.as_ref().ok_or_else(|| {
                    route_error("enabled operation requires a trusted Authority routing policy")
                })?;
                let bound = &packet.signed.bound;
                if !same_effective_authority_policy(
                    mounted,
                    live,
                    &bound.operation.subject.namespace_id,
                    &bound.declaration.key,
                )
                .map_err(OutboxError::Io)?
                {
                    return Err(route_error("operation Authority assignment changed").into());
                }
            }
            validate_root_policy(config, &packet)?;
            ensure_local_delivery_capacity(&packet, &self.neighbors, Some(config))?;
            let mut inbox = self.inbox.blocking_lock();
            inbox.recover_if_poisoned().map_err(ReceiveError::Persist)?;
            if inbox.key_deleted_for_new_local(&packet) {
                return Err(RelayReceiveError::KeyDeleted);
            }
            let receipt = inbox.receive_local_root(packet.clone())?;
            drop(inbox);
            let enrollment = enroll_roots(
                &mut self.outbox.blocking_lock(),
                &self.neighbors,
                config,
                &packet,
            );
            drop(live_policy_guard);
            if let Err(error) = enrollment {
                tracing::warn!(error = %error, "durable operation awaits routed enrollment retry");
            }
            Ok(receipt)
        } else {
            ensure_local_delivery_capacity(&packet, &self.neighbors, None)?;
            let mut inbox = self.inbox.blocking_lock();
            inbox.recover_if_poisoned().map_err(ReceiveError::Persist)?;
            if inbox.key_deleted_for_new_local(&packet) {
                return Err(RelayReceiveError::KeyDeleted);
            }
            let receipt = inbox.receive(packet.clone())?;
            drop(inbox);
            let mut outbox = self.outbox.blocking_lock();
            for neighbor in self.neighbors.permitted_for(
                &packet.signed.bound.operation.subject.namespace_id,
                packet.signed.bound.operation.action.key(),
            ) {
                if let Err(error) = outbox.ensure_enqueued_to(
                    neighbor.node_id.clone(),
                    neighbor.addr.clone(),
                    packet.clone(),
                ) {
                    tracing::warn!(error = %error, "durable operation awaits neighbor enrollment retry");
                }
            }
            Ok(receipt)
        }
    }

    pub async fn submit_local(
        &self,
        packet: OperationPacket,
    ) -> Result<OperationReceipt, StatusCode> {
        let relay = self.clone();
        tokio::task::spawn_blocking(move || relay.submit_local_blocking(packet))
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    }

    pub(crate) fn submit_local_blocking(
        &self,
        packet: OperationPacket,
    ) -> Result<OperationReceipt, StatusCode> {
        self.submit(packet).map_err(relay_error_status)
    }

    pub(crate) fn is_routed(&self) -> bool {
        self.routed.is_some()
    }

    fn receive_completion(
        &self,
        report: OperationCompletionReport,
    ) -> Result<OperationCompletionReceipt, RelayReceiveError> {
        if self.routed.is_none() || report.version != 1 || report.transfer.sender != self.node_id {
            return Err(RelayReceiveError::NotOwned);
        }
        // The outbox matches every descriptor field, including the packet,
        // destination, parent and next hop, before persisting this report.
        self.outbox
            .blocking_lock()
            .record_completion(report.clone())?;
        Ok(OperationCompletionReceipt {
            version: 1,
            transfer_id: TransferId {
                sender: report.transfer.sender,
                sequence: report.transfer.transfer_id,
            },
            commitment: report.transfer.packet.commitment(),
        })
    }

    fn receive_retirement(
        &self,
        notice: OperationRetirementNotice,
    ) -> Result<OperationRetirementNotice, RelayReceiveError> {
        if self.routed.is_none()
            || notice.receiver != self.node_id
            || self.neighbors.get(&notice.sender).is_none()
        {
            return Err(RelayReceiveError::NotOwned);
        }
        let mut inbox = self.inbox.blocking_lock();
        inbox.recover_if_poisoned().map_err(|error| {
            RelayReceiveError::Outbox(OutboxError::Io(io::Error::other(format!("{error:?}"))))
        })?;
        inbox
            .save_retirement_fence(&notice, &self.node_id)
            .map_err(|error| RelayReceiveError::Outbox(OutboxError::Io(error)))?;
        Ok(notice)
    }

    fn pending_completion_reports(&self) -> io::Result<Vec<(String, OperationTransfer)>> {
        if self.routed.is_none() {
            return Ok(Vec::new());
        }
        let mut inbox = self.inbox.blocking_lock();
        inbox
            .recover_if_poisoned()
            .map_err(|error| io::Error::other(format!("{error:?}")))?;
        let mut outbox = self.outbox.blocking_lock();
        outbox.recover_if_poisoned().map_err(io::Error::other)?;
        let mut ready = inbox.terminal_transfers()?;
        for parent in inbox.received_transfers()? {
            if parent.destination.as_ref() == Some(&self.node_id) {
                continue;
            }
            if let Some(destination) = &parent.destination {
                let parent_id = TransferId {
                    sender: parent.sender.clone(),
                    sequence: parent.transfer_id,
                };
                if outbox.completed_child(&parent_id, destination, &parent.packet) {
                    ready.push(parent);
                }
            }
        }
        Ok(ready
            .into_iter()
            .filter(|transfer| !inbox.completion_report_acked(transfer))
            .filter_map(|transfer| {
                self.neighbors
                    .get(&transfer.sender)
                    .map(|neighbor| (neighbor.addr.clone(), transfer))
            })
            .collect())
    }

    /// An inbox write and the corresponding send enrollment are separate
    /// durable steps. Retry the latter while this process remains online when
    /// the first enrollment failed after the inbox accepted the packet.
    fn reconcile_durable_enrollments(&self) -> Result<(), OutboxError> {
        let mut inbox = self.inbox.blocking_lock();
        inbox
            .recover_if_poisoned()
            .map_err(|error| io::Error::other(format!("{error:?}")))?;
        let mut outbox = self.outbox.blocking_lock();
        outbox.recover_if_poisoned()?;
        if let Some(config) = &self.routed {
            for packet in inbox.root_packets()? {
                enroll_roots(&mut outbox, &self.neighbors, config, &packet)?;
            }
            for transfer in inbox.received_transfers()? {
                validate_routed_transfer(&transfer, &self.node_id, config)?;
                if transfer.destination.as_ref() == Some(&self.node_id) {
                    let key = transfer.packet.signed.bound.operation.action.key();
                    let namespace = &transfer.packet.signed.bound.operation.subject.namespace_id;
                    let owner = self.owned.owns_in_namespace(namespace, key);
                    let authority = config.accepts_signing_intake(&self.node_id, &transfer.packet);
                    let disabled_terminal =
                        config.accepts_disabled_terminal(&self.node_id, &transfer.packet);
                    if !owner && !authority && !disabled_terminal {
                        return Err(route_error(
                            "terminal transfer has no local application or Authority intake",
                        ));
                    }
                    if owner || disabled_terminal {
                        inbox.mark_terminal(&transfer)?;
                    }
                    if authority {
                        inbox.mark_signing_intake(&transfer)?;
                    }
                }
                enroll_child(&mut outbox, &self.neighbors, config, &transfer)?;
            }
        } else {
            for packet in inbox.packets() {
                for neighbor in self.neighbors.permitted_for(
                    &packet.signed.bound.operation.subject.namespace_id,
                    packet.signed.bound.operation.action.key(),
                ) {
                    outbox.ensure_enqueued_to(
                        neighbor.node_id.clone(),
                        neighbor.addr.clone(),
                        packet.clone(),
                    )?;
                }
            }
            for transfer in inbox.received_transfers()? {
                for neighbor in self.neighbors.permitted_for(
                    &transfer.packet.signed.bound.operation.subject.namespace_id,
                    transfer.packet.signed.bound.operation.action.key(),
                ) {
                    outbox.ensure_enqueued_to(
                        neighbor.node_id.clone(),
                        neighbor.addr.clone(),
                        transfer.packet.clone(),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn record_completion_receipt(
        &self,
        transfer: &OperationTransfer,
        receipt: OperationCompletionReceipt,
    ) -> io::Result<()> {
        self.inbox
            .blocking_lock()
            .record_completion_receipt(transfer, receipt)
    }
}

fn route_error(message: &'static str) -> OutboxError {
    OutboxError::Io(io::Error::new(io::ErrorKind::InvalidInput, message))
}

/// The exact routed topology pin used by the relay and startup preflight.
pub fn routed_custody_manifest_digest(
    routes: &OperationRouteManifest,
    required: &OperationRequiredDestinations,
    neighbors: &OperationNeighborManifest,
) -> io::Result<[u8; 32]> {
    let mut hash = Sha256::new();
    hash.update(b"asteroidb/operation-routed-custody-manifest/v1");
    let manifest = serde_json::to_vec(&(
        routes.rows(),
        required.rows(),
        neighbors.neighbors().collect::<Vec<_>>(),
    ))
    .map_err(|error| invalid_packet_data(&error.to_string()))?;
    hash.update(manifest);
    Ok(hash.finalize().into())
}

/// The only public route-pin repair path. A pristine outbox alone is not
/// enough: the matching inbox may still hold transfers or reclaimed roots
/// whose identities were bound to the old route. Match the storage pair by
/// canonical path before checking either durable history.
pub fn repair_routed_manifest_if_pristine(
    inbox: &OperationInbox,
    outbox: &mut OperationOutbox,
    digest: [u8; 32],
) -> Result<(), OutboxError> {
    let inbox_dir = inbox
        .packet_dir
        .parent()
        .ok_or_else(|| invalid_packet_data("invalid operation inbox path"))?
        .canonicalize()?;
    let name = inbox_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid_packet_data("invalid operation inbox directory name"))?;
    let suffix = name
        .strip_prefix("operation_inbox")
        .map(|suffix| ("operation_outbox", suffix))
        .or_else(|| name.strip_prefix("inbox").map(|suffix| ("outbox", suffix)))
        .filter(|(_, suffix)| suffix.is_empty() || suffix.starts_with('-'))
        .ok_or_else(|| invalid_packet_data("operation inbox has no paired outbox name"))?;
    let paired_dir = inbox_dir
        .parent()
        .ok_or_else(|| invalid_packet_data("invalid operation inbox parent"))?
        .join(format!("{}{}", suffix.0, suffix.1))
        .canonicalize()?;
    if paired_dir != outbox.storage_directory().canonicalize()? {
        return Err(
            invalid_packet_data("operation inbox and outbox are not a storage pair").into(),
        );
    }
    match outbox.verify_routed_manifest(digest) {
        Ok(()) => Ok(()),
        Err(error) => {
            if !inbox.routing_repin_is_pristine()? {
                return Err(error);
            }
            outbox.replace_routed_manifest_if_pristine(digest)
        }
    }
}

fn validate_routed_transfer(
    transfer: &OperationTransfer,
    receiver: &NodeId,
    config: &RoutedCustody,
) -> Result<(), OutboxError> {
    if transfer.version != 2
        || &transfer.receiver != receiver
        || transfer.sender == transfer.receiver
        || transfer.destination.is_none()
        || transfer.parent.is_none() != (transfer.depth == 0)
        || transfer.parent.as_ref().is_some_and(|parent| {
            parent.sender.0.is_empty() || parent.sender.0.trim() != parent.sender.0
        })
    {
        return Err(route_error("invalid routed transfer descriptor"));
    }
    let destination = transfer.destination.as_ref().unwrap();
    let key = transfer.packet.signed.bound.operation.action.key();
    let namespace_id = &transfer.packet.signed.bound.operation.subject.namespace_id;
    let route = config
        .routes
        .next_hop_for(&transfer.sender, destination, Some(namespace_id), key)
        .ok_or_else(|| route_error("no route for inbound transfer"))?;
    if &route.next_hop != receiver {
        return Err(route_error(
            "inbound transfer is not its declared route hop",
        ));
    }
    if receiver != destination {
        if transfer.depth == u64::MAX {
            return Err(route_error("transfer depth exhausted"));
        }
        let next = config
            .routes
            .next_hop_for(receiver, destination, Some(namespace_id), key)
            .ok_or_else(|| route_error("inbound route has no continuation"))?;
        if next.rank >= route.rank {
            return Err(route_error("inbound route rank does not decrease"));
        }
    }
    Ok(())
}

fn enroll_child(
    outbox: &mut OperationOutbox,
    neighbors: &OperationNeighborManifest,
    config: &RoutedCustody,
    transfer: &OperationTransfer,
) -> Result<(), OutboxError> {
    let destination = transfer
        .destination
        .as_ref()
        .ok_or_else(|| route_error("missing destination"))?;
    if destination == neighbors.self_id() {
        return Ok(());
    }
    let key = transfer.packet.signed.bound.operation.action.key();
    let namespace_id = &transfer.packet.signed.bound.operation.subject.namespace_id;
    let route = config
        .routes
        .next_hop_for(neighbors.self_id(), destination, Some(namespace_id), key)
        .ok_or_else(|| route_error("no route for child transfer"))?;
    let neighbor = neighbors
        .get(&route.next_hop)
        .ok_or_else(|| route_error("child route next hop has no local address"))?;
    if !neighbor.permits(namespace_id, key) {
        return Err(route_error("child next hop does not permit key"));
    }
    let depth = transfer
        .depth
        .checked_add(1)
        .ok_or_else(|| route_error("transfer depth exhausted"))?;
    outbox.ensure_routed(
        route.next_hop.clone(),
        neighbor.addr.clone(),
        transfer.packet.clone(),
        destination.clone(),
        Some(TransferId {
            sender: transfer.sender.clone(),
            sequence: transfer.transfer_id,
        }),
        depth,
    )?;
    Ok(())
}

fn enroll_roots(
    outbox: &mut OperationOutbox,
    neighbors: &OperationNeighborManifest,
    config: &RoutedCustody,
    packet: &OperationPacket,
) -> Result<(), OutboxError> {
    validate_root_policy(config, packet)?;
    let key = packet.signed.bound.operation.action.key();
    let namespace_id = &packet.signed.bound.operation.subject.namespace_id;
    let Some(destinations) = config.required.destinations_for(Some(namespace_id), key) else {
        return Ok(());
    };
    for destination in destinations {
        if destination == neighbors.self_id() {
            continue;
        }
        let Some((next_hop, peer_addr)) =
            available_root_hop(neighbors, config, packet, destination)
        else {
            continue;
        };
        outbox.ensure_routed(
            next_hop,
            peer_addr,
            packet.clone(),
            destination.clone(),
            None,
            0,
        )?;
    }
    Ok(())
}

fn validate_root_policy(
    config: &RoutedCustody,
    packet: &OperationPacket,
) -> Result<(), OutboxError> {
    let key = packet.signed.bound.operation.action.key();
    let namespace_id = &packet.signed.bound.operation.subject.namespace_id;
    if packet.signed.bound.declaration.mode == CertificationMode::Enabled
        && let Some(policy) = &config.submit_authority_policy
    {
        let authority = policy.operation_authorities_for_key(namespace_id, key);
        let placement = policy.operation_placement_for_key(namespace_id, key);
        if authority.is_some_and(|definition| definition.authority_nodes.is_empty())
            || placement.is_some_and(|definition| !definition.certified)
            || matches!((authority, placement), (Some(authority), Some(placement))
                if authority.key_range.prefix != placement.key_range.prefix)
        {
            return Err(route_error(
                "enabled operation conflicts with Authority policy",
            ));
        }
    }
    Ok(())
}

fn same_effective_authority_policy(
    mounted: &SystemNamespace,
    live: &SystemNamespace,
    namespace_id: &str,
    key: &str,
) -> io::Result<bool> {
    let authority_identity =
        |definition: Option<&crate::control_plane::system_namespace::AuthorityDefinition>| {
            definition
                .map(|definition| {
                    let mut members = definition
                        .authority_nodes
                        .iter()
                        .map(|node| node.0.clone())
                        .collect::<Vec<_>>();
                    members.sort();
                    if members.windows(2).any(|pair| pair[0] == pair[1]) {
                        return Err(invalid_packet_data("duplicate operation Authority member"));
                    }
                    Ok((definition.key_range.prefix.clone(), members))
                })
                .transpose()
        };
    let mounted_authority =
        authority_identity(mounted.operation_authorities_for_key(namespace_id, key))?;
    let live_authority = authority_identity(live.operation_authorities_for_key(namespace_id, key))?;
    let placement_identity = |namespace: &SystemNamespace| {
        namespace
            .operation_placement_for_key(namespace_id, key)
            .map(|policy| (policy.key_range.prefix.clone(), policy.certified))
    };
    Ok(mounted_authority == live_authority
        && placement_identity(mounted) == placement_identity(live))
}

/// A missing local hop leaves this durable root pending. Route manifests
/// already prove that any selected hop has a decreasing path to its target.
fn available_root_hop(
    neighbors: &OperationNeighborManifest,
    config: &RoutedCustody,
    packet: &OperationPacket,
    destination: &NodeId,
) -> Option<(NodeId, String)> {
    let key = packet.signed.bound.operation.action.key();
    let namespace_id = &packet.signed.bound.operation.subject.namespace_id;
    let route =
        config
            .routes
            .next_hop_for(neighbors.self_id(), destination, Some(namespace_id), key)?;
    let neighbor = neighbors.get(&route.next_hop)?;
    neighbor
        .permits(namespace_id, key)
        .then(|| (route.next_hop.clone(), neighbor.addr.clone()))
}

/// A durable local ACK must not create an outbox job whose HTTP body is too
/// large for its receiver. Use the largest transfer ID because its decimal
/// encoding can grow after this submit; routed completion carries the same
/// full transfer in another JSON envelope.
fn ensure_local_delivery_capacity(
    packet: &OperationPacket,
    neighbors: &OperationNeighborManifest,
    routed: Option<&RoutedCustody>,
) -> Result<(), RelayReceiveError> {
    let namespace_id = &packet.signed.bound.operation.subject.namespace_id;
    let key = packet.signed.bound.operation.action.key();
    if let Some(config) = routed {
        if let Some(destinations) = config.required.destinations_for(Some(namespace_id), key) {
            for destination in destinations {
                if destination != neighbors.self_id()
                    && available_root_hop(neighbors, config, packet, destination).is_some()
                {
                    ensure_routed_path_capacity(
                        packet,
                        config,
                        neighbors.self_id(),
                        destination,
                        None,
                        0,
                    )?;
                }
            }
        }
    } else {
        for neighbor in neighbors.permitted_for(namespace_id, key) {
            let transfer = OperationTransfer {
                version: 1,
                sender: neighbors.self_id().clone(),
                transfer_id: u64::MAX,
                receiver: neighbor.node_id.clone(),
                destination: None,
                parent: None,
                depth: 0,
                packet: packet.clone(),
            };
            if serde_json::to_vec(&transfer)
                .map_err(OutboxError::Json)?
                .len()
                > DIRECT_OPERATION_BODY_LIMIT
            {
                return Err(RelayReceiveError::PacketTooLarge);
            }
        }
    }
    Ok(())
}

fn ensure_routed_path_capacity(
    packet: &OperationPacket,
    config: &RoutedCustody,
    source: &NodeId,
    destination: &NodeId,
    parent_sender: Option<&NodeId>,
    depth: u64,
) -> Result<(), RelayReceiveError> {
    let namespace_id = &packet.signed.bound.operation.subject.namespace_id;
    let key = packet.signed.bound.operation.action.key();
    let mut sender = source.clone();
    let mut parent_sender = parent_sender.cloned();
    let mut depth = depth;
    while sender != *destination {
        let route = config
            .routes
            .next_hop_for(&sender, destination, Some(namespace_id), key)
            .ok_or_else(|| route_error("no route for required destination"))?;
        let transfer = OperationTransfer {
            version: 2,
            sender: sender.clone(),
            transfer_id: u64::MAX,
            receiver: route.next_hop.clone(),
            destination: Some(destination.clone()),
            parent: parent_sender.as_ref().map(|prior| TransferId {
                sender: prior.clone(),
                sequence: u64::MAX,
            }),
            depth,
            packet: packet.clone(),
        };
        if serde_json::to_vec(&OperationCompletionReport {
            version: 1,
            transfer,
        })
        .map_err(OutboxError::Json)?
        .len()
            > INTERNAL_OPERATION_BODY_LIMIT
        {
            return Err(RelayReceiveError::PacketTooLarge);
        }
        parent_sender = Some(sender);
        sender = route.next_hop.clone();
        depth = depth
            .checked_add(1)
            .ok_or_else(|| route_error("transfer depth exhausted"))?;
    }
    Ok(())
}

fn ensure_inbound_transfer_capacity(
    transfer: &OperationTransfer,
    node: &NodeId,
    neighbors: &OperationNeighborManifest,
    routed: Option<&RoutedCustody>,
) -> Result<(), RelayReceiveError> {
    let limit = if routed.is_some() {
        INTERNAL_OPERATION_BODY_LIMIT
    } else {
        DIRECT_OPERATION_BODY_LIMIT
    };
    if serde_json::to_vec(transfer)
        .map_err(OutboxError::Json)?
        .len()
        > limit
    {
        return Err(RelayReceiveError::PacketTooLarge);
    }
    if let Some(config) = routed {
        if serde_json::to_vec(&OperationCompletionReport {
            version: 1,
            transfer: transfer.clone(),
        })
        .map_err(OutboxError::Json)?
        .len()
            > INTERNAL_OPERATION_BODY_LIMIT
        {
            return Err(RelayReceiveError::PacketTooLarge);
        }
        let destination = transfer
            .destination
            .as_ref()
            .ok_or(RelayReceiveError::NotOwned)?;
        ensure_routed_path_capacity(
            &transfer.packet,
            config,
            node,
            destination,
            Some(&transfer.sender),
            transfer
                .depth
                .checked_add(1)
                .ok_or_else(|| route_error("transfer depth exhausted"))?,
        )
    } else {
        ensure_local_delivery_capacity(&transfer.packet, neighbors, None)
    }
}

#[derive(Debug)]
enum RelayReceiveError {
    NotOwned,
    KeyDeleted,
    PacketTooLarge,
    Receive(ReceiveError),
    Outbox(OutboxError),
}

fn relay_error_status(error: RelayReceiveError) -> StatusCode {
    match error {
        RelayReceiveError::KeyDeleted => StatusCode::CONFLICT,
        RelayReceiveError::PacketTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        RelayReceiveError::NotOwned | RelayReceiveError::Receive(ReceiveError::Rejected(_)) => {
            StatusCode::FORBIDDEN
        }
        RelayReceiveError::Receive(ReceiveError::Persist(PersistOperationError::Operation(
            crate::operation::OperationError::ConflictingContent(_),
        ))) => StatusCode::CONFLICT,
        RelayReceiveError::Receive(ReceiveError::Persist(PersistOperationError::Operation(_))) => {
            StatusCode::BAD_REQUEST
        }
        RelayReceiveError::Receive(ReceiveError::Persist(PersistOperationError::Io(_))) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
        RelayReceiveError::Outbox(error) => {
            tracing::warn!(error = %error, "operation neighbor enrollment failed");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

impl From<ReceiveError> for RelayReceiveError {
    fn from(error: ReceiveError) -> Self {
        Self::Receive(error)
    }
}

impl From<OutboxError> for RelayReceiveError {
    fn from(error: OutboxError) -> Self {
        Self::Outbox(error)
    }
}

async fn relay_receive_http(
    State(relay): State<OperationRelay>,
    Json(packet): Json<OperationPacket>,
) -> Result<Json<OperationReceipt>, StatusCode> {
    tokio::task::spawn_blocking(move || relay.receive(packet))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
        .map_err(relay_error_status)
}

async fn relay_transfer_http(
    State(relay): State<OperationRelay>,
    Json(transfer): Json<OperationTransfer>,
) -> Result<Json<OperationTransferReceipt>, StatusCode> {
    tokio::task::spawn_blocking(move || relay.receive_transfer(transfer))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
        .map_err(relay_error_status)
}

async fn relay_completion_http(
    State(relay): State<OperationRelay>,
    Json(report): Json<OperationCompletionReport>,
) -> Result<Json<OperationCompletionReceipt>, StatusCode> {
    tokio::task::spawn_blocking(move || relay.receive_completion(report))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
        .map_err(relay_error_status)
}

async fn relay_retirement_http(
    State(relay): State<OperationRelay>,
    Json(notice): Json<OperationRetirementNotice>,
) -> Result<Json<OperationRetirementNotice>, StatusCode> {
    tokio::task::spawn_blocking(move || relay.receive_retirement(notice))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
        .map_err(relay_error_status)
}

async fn submit_http(
    State(relay): State<OperationRelay>,
    Json(packet): Json<OperationPacket>,
) -> Result<Json<OperationReceipt>, StatusCode> {
    relay.submit_local(packet).await.map(Json)
}

/// Public local submission authenticates through the packet's writer and
/// creation signatures and the trusted writer manifest. It does not use the
/// cluster's internal bearer token.
pub fn submit_router(relay: OperationRelay) -> Router {
    Router::new()
        .route(
            OPERATION_SUBMIT_PATH,
            post(submit_http).layer(DefaultBodyLimit::max(DIRECT_OPERATION_BODY_LIMIT)),
        )
        .route(PUBLIC_OPERATION_READ_PATH, get(relay_read_http))
        .with_state(relay)
}

/// One operation read surface with an explicit choice of evidence level.
/// The response keeps the distinct local and certified result types intact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "view", rename_all = "snake_case")]
pub enum OperationViewResponse {
    Local(OperationRead),
    Certified(CertifiedOperationResponse),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OperationViewKind {
    Local,
    Certified,
}

#[derive(Debug, Deserialize)]
struct OperationViewQuery {
    kind: OperationViewKind,
}

#[derive(Clone)]
struct OperationViewState {
    relay: OperationRelay,
    certificates: Option<MultiOperationCertificateState>,
}

/// Additive public read endpoint for operation-mode keys. Without an evidence
/// verifier, a certified read can still report the locally known creation
/// mode and that no certificate has been verified here.
pub fn unified_operation_view_router(
    relay: OperationRelay,
    certificates: Option<OperationCertificateState>,
) -> Router {
    unified_operation_view_router_multi(
        relay,
        certificates.map(|state| {
            MultiOperationCertificateState::new(vec![state])
                .expect("a single operation certificate scope is valid")
        }),
    )
}

pub fn unified_operation_view_router_multi(
    relay: OperationRelay,
    certificates: Option<MultiOperationCertificateState>,
) -> Router {
    Router::new()
        .route(PUBLIC_OPERATION_VIEW_PATH, get(operation_view_http))
        .with_state(OperationViewState {
            relay,
            certificates,
        })
}

async fn operation_view_http(
    State(state): State<OperationViewState>,
    RoutePath((namespace_id, key)): RoutePath<(String, String)>,
    Query(query): Query<OperationViewQuery>,
) -> Result<Json<OperationViewResponse>, StatusCode> {
    tokio::task::spawn_blocking(move || match query.kind {
        OperationViewKind::Local => {
            let result = if state.relay.owned.owns_in_namespace(&namespace_id, &key) {
                let mut inbox = state.relay.inbox.blocking_lock();
                inbox
                    .recover_if_poisoned()
                    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
                inbox.ordinary_read(&namespace_id, &key)
            } else {
                OperationRead::Unknown
            };
            Ok(OperationViewResponse::Local(result))
        }
        OperationViewKind::Certified => {
            let read = match state.certificates {
                Some(certificates) => certificates.read_for_key_blocking(&namespace_id, &key)?,
                None if !state.relay.owned.owns_in_namespace(&namespace_id, &key) => {
                    let mut inbox = state.relay.inbox.blocking_lock();
                    non_owner_certified_result(&mut inbox, &namespace_id, &key)?
                }
                None => {
                    let mut inbox = state.relay.inbox.blocking_lock();
                    inbox
                        .recover_if_poisoned()
                        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
                    let read = inbox.certified_read(&[], &namespace_id, &key);
                    CertifiedOperationResponse {
                        result: read.result,
                        basis: Vec::new(),
                        local_body_seen: read.local_body_seen,
                    }
                }
            };
            Ok(OperationViewResponse::Certified(read))
        }
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map(Json)
}

async fn relay_read_http(
    State(relay): State<OperationRelay>,
    RoutePath((namespace_id, key)): RoutePath<(String, String)>,
) -> Result<Json<OperationRead>, StatusCode> {
    tokio::task::spawn_blocking(move || {
        if !relay.owned.owns_in_namespace(&namespace_id, &key) {
            return Ok(Json(OperationRead::Unknown));
        }
        let mut inbox = relay.inbox.blocking_lock();
        inbox
            .recover_if_poisoned()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        Ok(Json(inbox.ordinary_read(&namespace_id, &key)))
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}

pub fn relay_router(relay: OperationRelay, token: String) -> Router {
    assert!(
        relay.routed.is_none(),
        "routed operation relay requires a link-authenticated router"
    );
    Router::new()
        .route(
            OPERATION_RECEIVE_PATH,
            post(relay_receive_http).layer(DefaultBodyLimit::max(DIRECT_OPERATION_BODY_LIMIT)),
        )
        .route(
            OPERATION_TRANSFER_PATH,
            post(relay_transfer_http).layer(DefaultBodyLimit::max(DIRECT_OPERATION_BODY_LIMIT)),
        )
        .route(OPERATION_COMPLETION_PATH, post(relay_completion_http))
        .route(OPERATION_RETIREMENT_PATH, post(relay_retirement_http))
        .route(OPERATION_READ_PATH, get(relay_read_http))
        .layer(middleware::from_fn(move |request, next| {
            let token = token.clone();
            require_bearer_token(request, next, token)
        }))
        .with_state(relay)
}

/// Routed custody listener. Every state-changing request is authenticated as
/// the adjacent peer named in its body before the handler can persist it.
pub fn relay_router_authenticated(
    relay: OperationRelay,
    token: String,
    link: OperationLinkAuth,
) -> Router {
    Router::new()
        .route(
            OPERATION_TRANSFER_PATH,
            post(relay_transfer_http).layer(DefaultBodyLimit::max(INTERNAL_OPERATION_BODY_LIMIT)),
        )
        .route(
            OPERATION_COMPLETION_PATH,
            post(relay_completion_http).layer(DefaultBodyLimit::max(INTERNAL_OPERATION_BODY_LIMIT)),
        )
        .route(OPERATION_RETIREMENT_PATH, post(relay_retirement_http))
        .route(OPERATION_READ_PATH, get(relay_read_http))
        .layer(middleware::from_fn(move |request, next| {
            let link = link.clone();
            async move { verify_link_request(request, next, link).await }
        }))
        .layer(middleware::from_fn(move |request, next| {
            let token = token.clone();
            require_bearer_token(request, next, token)
        }))
        .with_state(relay)
}

pub(crate) async fn verify_link_request(
    request: axum::extract::Request,
    next: axum::middleware::Next,
    link: OperationLinkAuth,
) -> axum::response::Response {
    use axum::body::{Body, to_bytes};
    use axum::http::HeaderValue;
    let method = request.method().as_str().to_owned();
    let path = request.uri().path().to_owned();
    if method != "POST" {
        return next.run(request).await;
    }
    let sender = request
        .headers()
        .get(LINK_SENDER)
        .and_then(|v| v.to_str().ok())
        .map(|v| NodeId(v.to_owned()));
    let receiver = request
        .headers()
        .get(LINK_RECEIVER)
        .and_then(|v| v.to_str().ok())
        .map(|v| NodeId(v.to_owned()));
    let signature = request
        .headers()
        .get(LINK_SIGNATURE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let (Some(sender), Some(receiver), Some(signature)) = (sender, receiver, signature) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let (parts, body) = request.into_parts();
    let Ok(body) = to_bytes(body, INTERNAL_OPERATION_BODY_LIMIT).await else {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    let Some(request_digest) =
        link.verify_request(&sender, &receiver, &method, &path, &body, &signature)
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let claims_match = match path.as_str() {
        // Link authentication binds both versions to the signed peer IDs.
        // The relay checks v1 for direct gossip and v2 for routed custody.
        OPERATION_TRANSFER_PATH => {
            serde_json::from_slice::<OperationTransfer>(&body).is_ok_and(|v| {
                v.sender == sender && v.receiver == receiver && matches!(v.version, 1 | 2)
            })
        }
        OPERATION_COMPLETION_PATH => serde_json::from_slice::<OperationCompletionReport>(&body)
            .is_ok_and(|v| v.transfer.receiver == sender && v.transfer.sender == receiver),
        OPERATION_RETIREMENT_PATH => serde_json::from_slice::<OperationRetirementNotice>(&body)
            .is_ok_and(|v| v.sender == sender && v.receiver == receiver),
        POLICY_WITNESS_PATH => serde_json::from_slice::<PolicyWitnessTransfer>(&body)
            .is_ok_and(|v| v.sender == sender && v.receiver == receiver && v.version == 1),
        _ => false,
    };
    if !claims_match {
        return StatusCode::FORBIDDEN.into_response();
    }
    let request = axum::http::Request::from_parts(parts, Body::from(body));
    let response = next.run(request).await;
    let (mut parts, body) = response.into_parts();
    let Ok(body) = to_bytes(body, INTERNAL_OPERATION_BODY_LIMIT).await else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let response_signature = link.sign_response(
        &sender,
        &method,
        &path,
        &request_digest,
        parts.status.as_u16(),
        &body,
    );
    let (Ok(local_header), Ok(peer_header), Ok(signature_header), Ok(request_header)) = (
        HeaderValue::from_str(&link.self_id().0),
        HeaderValue::from_str(&sender.0),
        HeaderValue::from_str(&response_signature),
        HeaderValue::from_str(&hex::encode(request_digest)),
    ) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    parts.headers.insert(LINK_SENDER, local_header);
    parts.headers.insert(LINK_RECEIVER, peer_header);
    parts.headers.insert(LINK_SIGNATURE, signature_header);
    parts.headers.insert(LINK_REQUEST_DIGEST, request_header);
    axum::http::Response::from_parts(parts, Body::from(body))
}

/// Sender for one packet at a time. A caller must retain an outbox copy until
/// it has durably recorded the matching receipt.
pub struct OperationTransportClient {
    client: reqwest::Client,
    token: String,
    link: Option<OperationLinkAuth>,
}

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("receiver rejected operation with HTTP {0}")]
    Rejected(StatusCode),
    #[error("receiver acknowledged a different operation")]
    WrongCommitment,
    #[error("receiver acknowledged a different transfer identity")]
    WrongTransfer,
    #[error("receiver acknowledged a different completion report")]
    WrongCompletion,
    #[error("receiver acknowledged a different retirement fence")]
    WrongRetirement,
    #[error("adjacent peer identity or signed response did not match")]
    WrongPeer,
    #[error("operation request serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

impl OperationTransportClient {
    pub fn new(token: String) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("operation transport HTTP client"),
            token,
            link: None,
        }
    }

    pub fn authenticated(token: String, link: OperationLinkAuth) -> Self {
        let mut client = Self::new(token);
        client.link = Some(link);
        client
    }

    async fn post<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        peer_addr: &str,
        peer: &NodeId,
        path: &str,
        value: &T,
    ) -> Result<R, SendError> {
        let body = serde_json::to_vec(value)?;
        let mut request = self
            .client
            .post(format!("http://{peer_addr}{path}"))
            .bearer_auth(&self.token);
        let mut digest = None;
        if let Some(link) = &self.link {
            if link.peer_for_addr(peer_addr) != Some(peer) {
                return Err(SendError::WrongPeer);
            }
            let (request_digest, signature) = link.sign_request(peer, "POST", path, &body);
            digest = Some(request_digest);
            request = request
                .header(LINK_SENDER, &link.self_id().0)
                .header(LINK_RECEIVER, &peer.0)
                .header(LINK_SIGNATURE, signature);
        }
        let response = request
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await?;
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
        if let (Some(link), Some(digest)) = (&self.link, digest)
            && (sender.as_deref() != Some(&peer.0)
                || receiver.as_deref() != Some(&link.self_id().0)
                || bound_request.as_deref() != Some(&hex::encode(digest))
                || !signature.as_deref().is_some_and(|signature| {
                    link.verify_response(
                        peer,
                        link.self_id(),
                        "POST",
                        path,
                        &digest,
                        status.as_u16(),
                        &bytes,
                        signature,
                    )
                }))
        {
            return Err(SendError::WrongPeer);
        }
        if !status.is_success() {
            return Err(SendError::Rejected(status));
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub async fn send(
        &self,
        peer_addr: &str,
        packet: &OperationPacket,
    ) -> Result<OperationReceipt, SendError> {
        if self.link.is_some() {
            return Err(SendError::WrongPeer);
        }
        let response = self
            .client
            .post(format!("http://{peer_addr}{OPERATION_RECEIVE_PATH}"))
            .bearer_auth(&self.token)
            .json(packet)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(SendError::Rejected(response.status()));
        }
        let receipt: OperationReceipt = response.json().await?;
        if receipt.commitment != packet.commitment() {
            return Err(SendError::WrongCommitment);
        }
        Ok(receipt)
    }

    pub async fn send_transfer(
        &self,
        peer_addr: &str,
        transfer: &OperationTransfer,
    ) -> Result<OperationTransferReceipt, SendError> {
        let receipt: OperationTransferReceipt = self
            .post(
                peer_addr,
                &transfer.receiver,
                OPERATION_TRANSFER_PATH,
                transfer,
            )
            .await?;
        if receipt.version != transfer.version
            || receipt.sender != transfer.sender
            || receipt.receiver != transfer.receiver
            || receipt.transfer_id != transfer.transfer_id
        {
            return Err(SendError::WrongTransfer);
        }
        if receipt.commitment != transfer.packet.commitment() {
            return Err(SendError::WrongCommitment);
        }
        Ok(receipt)
    }

    pub async fn send_policy_witness(
        &self,
        peer_addr: &str,
        transfer: &PolicyWitnessTransfer,
    ) -> Result<OperationReceipt, SendError> {
        if self.link.is_none() {
            return Err(SendError::WrongPeer);
        }
        let receipt: OperationReceipt = self
            .post(peer_addr, &transfer.receiver, POLICY_WITNESS_PATH, transfer)
            .await?;
        if receipt.commitment != transfer.packet.commitment() {
            return Err(SendError::WrongCommitment);
        }
        Ok(receipt)
    }

    pub async fn send_completion(
        &self,
        peer_addr: &str,
        report: &OperationCompletionReport,
    ) -> Result<OperationCompletionReceipt, SendError> {
        let receipt: OperationCompletionReceipt = self
            .post(
                peer_addr,
                &report.transfer.sender,
                OPERATION_COMPLETION_PATH,
                report,
            )
            .await?;
        if receipt.version != 1
            || receipt.transfer_id
                != (TransferId {
                    sender: report.transfer.sender.clone(),
                    sequence: report.transfer.transfer_id,
                })
            || receipt.commitment != report.transfer.packet.commitment()
        {
            return Err(SendError::WrongCompletion);
        }
        Ok(receipt)
    }

    pub async fn send_retirement(
        &self,
        peer_addr: &str,
        notice: &OperationRetirementNotice,
    ) -> Result<(), SendError> {
        let received: OperationRetirementNotice = self
            .post(
                peer_addr,
                &notice.receiver,
                OPERATION_RETIREMENT_PATH,
                notice,
            )
            .await?;
        if &received != notice {
            return Err(SendError::WrongRetirement);
        }
        Ok(())
    }
}

/// Retry terminal and collapsed completion reports from durable local state.
/// A report is retired only after its upstream neighbor has persisted it and
/// the matching response is itself durable locally.
pub fn spawn_operation_completion_worker(
    relay: OperationRelay,
    token: String,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    spawn_operation_completion_worker_with_auth(relay, token, None, shutdown)
}

pub fn spawn_operation_completion_worker_with_auth(
    relay: OperationRelay,
    token: String,
    link: Option<OperationLinkAuth>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = match link {
            Some(link) => OperationTransportClient::authenticated(token, link),
            None => OperationTransportClient::new(token),
        };
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut cursor = crate::network::retry_cursor::RetryCursor::default();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                _ = ticker.tick() => {
                    if *shutdown.borrow() { break; }
                    let source = relay.clone();
                    match tokio::task::spawn_blocking(move || source.reconcile_durable_enrollments()).await {
                        Ok(Ok(())) => {},
                        Ok(Err(error)) => {
                            // An unrelated enrollment can fail after a completion
                            // report was made durable. Pending discovery separately
                            // recovers both stores before using their state.
                            tracing::warn!(error = %error, "operation send enrollment discovery failed");
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, "operation send enrollment task failed");
                            continue;
                        }
                    }
                    // The sender ACK and completion may arrive in either
                    // order. Recheck roots after both are durable, including
                    // after a restart with no new completion message.
                    let collector = relay.clone();
                    match tokio::task::spawn_blocking(move || collector.reclaim_completed_roots()).await {
                        Ok(Ok(_)) => {},
                        other => tracing::warn!(error = ?other, "completed root collection failed"),
                    }
                    let source = relay.clone();
                    let mut pending = match tokio::task::spawn_blocking(move || source.pending_completion_reports()).await {
                        Ok(Ok(pending)) => pending,
                        other => { tracing::warn!(error = ?other, "completion report discovery failed"); continue; }
                    };
                    pending.sort_by_key(|(_, transfer)| (transfer.sender.0.clone(), transfer.transfer_id));
                    let selected = cursor.next(pending.iter().map(|(_, transfer)|
                        (transfer.sender.0.clone(), transfer.transfer_id)));
                    if let Some((addr, transfer)) = selected.and_then(|key| pending.iter()
                        .find(|(_, transfer)| (transfer.sender.0.clone(), transfer.transfer_id) == key)
                        .cloned()) {
                        let report = OperationCompletionReport { version: 1, transfer: transfer.clone() };
                        match client.send_completion(&addr, &report).await {
                            Ok(receipt) => {
                                let relay = relay.clone();
                                match tokio::task::spawn_blocking(move || relay.record_completion_receipt(&transfer, receipt)).await {
                                    Ok(Ok(())) => {},
                                    Ok(Err(error)) => tracing::warn!(error = %error, "completion receipt persistence failed"),
                                    Err(error) => tracing::warn!(error = %error, "completion receipt task failed"),
                                }
                            }
                            Err(error) => tracing::debug!(error = %error, "completion report retry remains pending"),
                        }
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::writer_auth::SignedCreation;
    use crate::hlc::HlcTimestamp;
    use crate::network::operation_routes::{OperationRouteEdge, OperationRouteRow};
    use crate::operation::{
        CandidateKvAction, CertificationMode, CrdtSubject, Declaration, EventDot, OperationLeaf,
    };
    use crate::types::NodeId;
    use axum::body::Body;
    use axum::body::to_bytes;
    use axum::http::Request;
    use ed25519_dalek::SigningKey;
    use std::collections::BTreeSet;
    use tower::ServiceExt;

    fn inbox_ledger_wal_bytes(dir: &Path) -> u64 {
        crate::store::wal::list_segments(dir)
            .unwrap()
            .iter()
            .map(|(_, path)| std::fs::metadata(path).unwrap().len())
            .sum()
    }

    #[test]
    fn inbox_lock_precedes_ledger_torn_tail_recovery() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let mut first = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        first.receive(packet(b"held")).unwrap();
        let wal_path = crate::store::wal::list_segments(dir.path())
            .unwrap()
            .last()
            .unwrap()
            .1
            .clone();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        file.write_all(&[0u8; 5]).unwrap();
        file.sync_all().unwrap();
        let torn_len = std::fs::metadata(&wal_path).unwrap().len();

        assert!(OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).is_err());
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), torn_len);

        drop(first);
        let recovered = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(
            recovered
                .get_packet(&packet(b"held").signed.bound.operation.subject)
                .is_some()
        );
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), torn_len - 5);
    }

    fn test_link(id: &str, seed: u8, neighbors: &[(&str, &str, u8)]) -> OperationLinkAuth {
        let manifest = serde_json::Value::Array(neighbors.iter().map(|(peer, addr, _)|
            serde_json::json!({"node_id":peer,"addr":addr,"key_prefixes":[""]})
        ).collect());
        let keys = serde_json::Value::Array(neighbors.iter().map(|(peer, _, key_seed)|
            serde_json::json!({"node_id":peer,"public_key":hex::encode(SigningKey::from_bytes(&[*key_seed;32]).verifying_key().as_bytes())})
        ).collect());
        OperationLinkAuth::from_manifest(
            NodeId(id.into()),
            &hex::encode([seed; 32]),
            &keys.to_string(),
            &OperationNeighborManifest::from_json(&manifest.to_string(), &NodeId(id.into()))
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn inbox_link_manifest_requires_empty_prior_custody_state() {
        let root = tempfile::tempdir().unwrap();
        let inbox = OperationInbox::open(root.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.verify_link_manifest([1; 32]).unwrap();
        assert!(!root.path().join(".link-manifest").exists());
        inbox.ensure_link_manifest([1; 32]).unwrap();
        inbox.verify_link_manifest([1; 32]).unwrap();
        assert!(inbox.verify_link_manifest([2; 32]).is_err());
        assert!(inbox.ensure_link_manifest([2; 32]).is_err());
        drop(inbox);
        OperationInbox::open(root.path(), Arc::new(PermitTestOperations))
            .unwrap()
            .ensure_link_manifest([1; 32])
            .unwrap();

        let old = tempfile::tempdir().unwrap();
        let inbox = OperationInbox::open(old.path(), Arc::new(PermitTestOperations)).unwrap();
        std::fs::write(old.path().join("transfers").join("prior"), b"untrusted").unwrap();
        assert!(inbox.verify_link_manifest([1; 32]).is_err());
        assert!(!old.path().join(".link-manifest").exists());
        assert!(inbox.ensure_link_manifest([1; 32]).is_err());
    }

    #[tokio::test]
    async fn signed_direct_http_accepts_v1_and_rejects_mismatched_sender() {
        let dir = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let b_addr = listener.local_addr().unwrap().to_string();
        let a_id = NodeId("a".into());
        let b_id = NodeId("b".into());
        let a = OperationRelay::new(
            Arc::new(Mutex::new(
                OperationInbox::open(dir.path().join("a-inbox"), Arc::new(PermitTestOperations))
                    .unwrap(),
            )),
            Arc::new(Mutex::new(
                OperationOutbox::open_for_node(dir.path().join("a-outbox"), a_id.clone()).unwrap(),
            )),
            OperationNeighborManifest::from_json(
                &serde_json::json!([{"node_id":"b","addr":b_addr.clone(),"key_prefixes":[""]}])
                    .to_string(),
                &a_id,
            )
            .unwrap(),
            OperationOwnedPrefixes::from_json(r#"[""]"#).unwrap(),
        )
        .await
        .unwrap();
        let b = OperationRelay::new(
            Arc::new(Mutex::new(
                OperationInbox::open(dir.path().join("b-inbox"), Arc::new(PermitTestOperations))
                    .unwrap(),
            )),
            Arc::new(Mutex::new(
                OperationOutbox::open_for_node(dir.path().join("b-outbox"), b_id.clone()).unwrap(),
            )),
            OperationNeighborManifest::from_json(
                r#"[{"node_id":"a","addr":"127.0.0.1:1","key_prefixes":[""]}]"#,
                &b_id,
            )
            .unwrap(),
            OperationOwnedPrefixes::from_json(r#"[""]"#).unwrap(),
        )
        .await
        .unwrap();
        let a_link = test_link("a", 1, &[("b", &b_addr, 2)]);
        let b_link = test_link("b", 2, &[("a", "127.0.0.1:1", 1)]);
        for (relay, link) in [(&a, &a_link), (&b, &b_link)] {
            relay
                .inbox
                .lock()
                .await
                .ensure_link_manifest(link.manifest_digest())
                .unwrap();
            relay
                .outbox
                .lock()
                .await
                .ensure_link_manifest(link.manifest_digest())
                .unwrap();
        }
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                relay_router_authenticated(b.clone(), "secret".into(), b_link),
            )
            .await
            .unwrap();
        });
        a.submit_local(packet(b"direct-signed-link")).await.unwrap();
        let entry = a.outbox.lock().await.pending().next().unwrap().clone();
        let transfer = entry_transfer_for_test(&entry);
        assert_eq!(transfer.version, 1);
        let client = OperationTransportClient::authenticated("secret".into(), a_link.clone());
        let receipt = client.send_transfer(&b_addr, &transfer).await.unwrap();
        assert_eq!(receipt.transfer_id, transfer.transfer_id);

        let mut forged = transfer.clone();
        forged.sender = NodeId("other".into());
        let body = serde_json::to_vec(&forged).unwrap();
        let (_, signature) = a_link.sign_request(&b_id, "POST", OPERATION_TRANSFER_PATH, &body);
        let response = reqwest::Client::new()
            .post(format!("http://{b_addr}{OPERATION_TRANSFER_PATH}"))
            .bearer_auth("secret")
            .header(LINK_SENDER, "a")
            .header(LINK_RECEIVER, "b")
            .header(LINK_SIGNATURE, signature)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        server.abort();
    }

    #[tokio::test]
    async fn signed_routed_http_rejects_forged_sender_and_replays_exact_transfer() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let a_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let c_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a_addr = a_listener.local_addr().unwrap().to_string();
        let b_addr = listener.local_addr().unwrap().to_string();
        let c_addr = c_listener.local_addr().unwrap().to_string();
        let a = routed_relay(
            dir.path(),
            "a",
            &serde_json::json!([
                {"node_id":"b","addr":b_addr,"key_prefixes":[""]}
            ])
            .to_string(),
            &config,
        )
        .await;
        let b = routed_relay(
            dir.path(),
            "b",
            &serde_json::json!([
                {"node_id":"a","addr":a_addr,"key_prefixes":[""]},
                {"node_id":"c","addr":c_addr,"key_prefixes":[""]}
            ])
            .to_string(),
            &config,
        )
        .await;
        let c = routed_relay(
            dir.path(),
            "c",
            &serde_json::json!([
                {"node_id":"b","addr":b_addr,"key_prefixes":[""]}
            ])
            .to_string(),
            &config,
        )
        .await;
        let a_link = test_link("a", 1, &[("b", &b_addr, 2)]);
        let b_link = test_link("b", 2, &[("a", &a_addr, 1), ("c", &c_addr, 3)]);
        let c_link = test_link("c", 3, &[("b", &b_addr, 2)]);
        for (relay, link) in [(&a, &a_link), (&b, &b_link), (&c, &c_link)] {
            relay
                .inbox
                .lock()
                .await
                .ensure_link_manifest(link.manifest_digest())
                .unwrap();
            relay
                .outbox
                .lock()
                .await
                .ensure_link_manifest(link.manifest_digest())
                .unwrap();
        }
        let a_app = relay_router_authenticated(a.clone(), "secret".into(), a_link.clone());
        let app = relay_router_authenticated(b.clone(), "secret".into(), b_link.clone());
        let c_app = relay_router_authenticated(c.clone(), "secret".into(), c_link.clone());
        let a_server = tokio::spawn(async move {
            axum::serve(a_listener, a_app).await.unwrap();
        });
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let c_server = tokio::spawn(async move {
            axum::serve(c_listener, c_app).await.unwrap();
        });
        let initial = packet(&vec![0; 1_000_000]);
        let initial_len = serde_json::to_vec(&initial).unwrap().len();
        let target_len = 2 * 1024 * 1024 - 64;
        assert!(initial_len < target_len);
        let body = packet(&vec![0; 1_000_000 + (target_len - initial_len) / 2]);
        let packet_len = serde_json::to_vec(&body).unwrap().len();
        assert!(packet_len < 2 * 1024 * 1024);
        a.submit_local(body).await.unwrap();
        let root = a.outbox.lock().await.pending().next().unwrap().clone();
        let transfer = entry_transfer_for_test(&root);
        let bytes = serde_json::to_vec(&transfer).unwrap();
        assert!(bytes.len() > 2 * 1024 * 1024);
        assert!(bytes.len() < INTERNAL_OPERATION_BODY_LIMIT);
        let raw = reqwest::Client::new();
        let url = format!("http://{b_addr}{OPERATION_TRANSFER_PATH}");
        let forged = raw
            .post(&url)
            .bearer_auth("secret")
            .json(&transfer)
            .send()
            .await
            .unwrap();
        assert_eq!(forged.status(), StatusCode::UNAUTHORIZED);
        let (digest, signature) =
            a_link.sign_request(&NodeId("b".into()), "POST", OPERATION_TRANSFER_PATH, &bytes);
        let mut other = transfer.clone();
        other.sender = NodeId("c".into());
        let forged_sender = raw
            .post(&url)
            .bearer_auth("secret")
            .header(LINK_SENDER, "a")
            .header(LINK_RECEIVER, "b")
            .header(LINK_SIGNATURE, &signature)
            .json(&other)
            .send()
            .await
            .unwrap();
        assert!(!forged_sender.status().is_success());
        assert!(
            b.inbox
                .lock()
                .await
                .received_transfers()
                .unwrap()
                .is_empty()
        );
        let client = OperationTransportClient::authenticated("secret".into(), a_link.clone());
        let receipt = client.send_transfer(&b_addr, &transfer).await.unwrap();
        assert_eq!(receipt.transfer_id, transfer.transfer_id);
        let repeated = client.send_transfer(&b_addr, &transfer).await.unwrap();
        assert!(repeated.already_present);
        let child = b.outbox.lock().await.pending().next().unwrap().clone();
        let child_receipt =
            OperationTransportClient::authenticated("secret".into(), b_link.clone())
                .send_transfer(&c_addr, &entry_transfer_for_test(&child))
                .await
                .unwrap();
        assert_eq!(child_receipt.receiver, NodeId("c".into()));
        assert_eq!(c.inbox.lock().await.terminal_transfers().unwrap().len(), 1);
        let c_ready = c.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || c_ready.pending_completion_reports().unwrap())
                .await
                .unwrap()
                .len(),
            1
        );
        b.outbox
            .lock()
            .await
            .record_receipt(child.id, child_receipt)
            .unwrap();
        let child_transfer = entry_transfer_for_test(&child);
        OperationTransportClient::authenticated("secret".into(), c_link)
            .send_completion(
                &b_addr,
                &OperationCompletionReport {
                    version: 1,
                    transfer: child_transfer,
                },
            )
            .await
            .unwrap();
        assert!(
            b.outbox
                .lock()
                .await
                .get(child.id)
                .unwrap()
                .completion
                .is_some()
        );
        let b_ready = b.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || b_ready.pending_completion_reports().unwrap())
                .await
                .unwrap()
                .len(),
            1
        );
        let completion = OperationCompletionReport {
            version: 1,
            transfer: transfer.clone(),
        };
        let completion_len = serde_json::to_vec(&completion).unwrap().len();
        assert!(completion_len > 2 * 1024 * 1024);
        let completion_receipt =
            OperationTransportClient::authenticated("secret".into(), b_link.clone())
                .send_completion(&a_addr, &completion)
                .await
                .unwrap();
        assert_eq!(
            completion_receipt.transfer_id.sequence,
            transfer.transfer_id
        );
        assert!(
            a.outbox
                .lock()
                .await
                .get(root.id)
                .unwrap()
                .completion
                .is_some()
        );
        let forged_fence = OperationRetirementNotice {
            version: 1,
            sender: NodeId("a".into()),
            receiver: NodeId("b".into()),
            closed_before: 999,
        };
        let fence = raw
            .post(format!("http://{b_addr}{OPERATION_RETIREMENT_PATH}"))
            .bearer_auth("secret")
            .json(&forged_fence)
            .send()
            .await
            .unwrap();
        assert_eq!(fence.status(), StatusCode::UNAUTHORIZED);
        let mut false_sender_fence = forged_fence.clone();
        false_sender_fence.sender = NodeId("c".into());
        let fence_body = serde_json::to_vec(&false_sender_fence).unwrap();
        let (_, fence_signature) = a_link.sign_request(
            &NodeId("b".into()),
            "POST",
            OPERATION_RETIREMENT_PATH,
            &fence_body,
        );
        let fence = raw
            .post(format!("http://{b_addr}{OPERATION_RETIREMENT_PATH}"))
            .bearer_auth("secret")
            .header(LINK_SENDER, "a")
            .header(LINK_RECEIVER, "b")
            .header(LINK_SIGNATURE, fence_signature)
            .body(fence_body)
            .send()
            .await
            .unwrap();
        assert_eq!(fence.status(), StatusCode::FORBIDDEN);
        let report = OperationCompletionReport {
            version: 1,
            transfer: transfer.clone(),
        };
        let forged_report = raw
            .post(format!("http://{b_addr}{OPERATION_COMPLETION_PATH}"))
            .bearer_auth("secret")
            .json(&report)
            .send()
            .await
            .unwrap();
        assert_eq!(forged_report.status(), StatusCode::UNAUTHORIZED);
        let mut false_sender_report = OperationCompletionReport {
            version: 1,
            transfer: entry_transfer_for_test(&child),
        };
        false_sender_report.transfer.receiver = NodeId("a".into());
        let report_body = serde_json::to_vec(&false_sender_report).unwrap();
        let c_signer = test_link("c", 3, &[("b", &b_addr, 2)]);
        let (_, report_signature) = c_signer.sign_request(
            &NodeId("b".into()),
            "POST",
            OPERATION_COMPLETION_PATH,
            &report_body,
        );
        let completion = raw
            .post(format!("http://{b_addr}{OPERATION_COMPLETION_PATH}"))
            .bearer_auth("secret")
            .header(LINK_SENDER, "c")
            .header(LINK_RECEIVER, "b")
            .header(LINK_SIGNATURE, report_signature)
            .body(report_body)
            .send()
            .await
            .unwrap();
        assert_eq!(completion.status(), StatusCode::FORBIDDEN);
        a_server.abort();
        assert_eq!(
            b.outbox
                .lock()
                .await
                .get(child.id)
                .unwrap()
                .completion
                .as_ref(),
            Some(&OperationCompletionReport {
                version: 1,
                transfer: entry_transfer_for_test(&child),
            })
        );
        b.inbox
            .lock()
            .await
            .ensure_link_manifest(b_link.manifest_digest())
            .unwrap();
        b.outbox
            .lock()
            .await
            .ensure_link_manifest(b_link.manifest_digest())
            .unwrap();
        let fake_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fake_addr = fake_listener.local_addr().unwrap().to_string();
        let fake_receipt = receipt.clone();
        let fake_router = Router::new().route(
            OPERATION_TRANSFER_PATH,
            post(move || {
                let receipt = fake_receipt.clone();
                async move { Json(receipt) }
            }),
        );
        let fake_server = tokio::spawn(async move {
            axum::serve(fake_listener, fake_router).await.unwrap();
        });
        let fake_client = OperationTransportClient::authenticated(
            "secret".into(),
            test_link("a", 1, &[("b", &fake_addr, 2)]),
        );
        assert!(matches!(
            fake_client.send_transfer(&fake_addr, &transfer).await,
            Err(SendError::WrongPeer)
        ));
        assert_ne!(digest, [0; 32]);
        server.abort();
        c_server.abort();
        fake_server.abort();
    }

    struct PermitTestOperations;

    impl OperationAdmission for PermitTestOperations {
        fn admit(&self, _signed: &SignedBoundOperation) -> Result<(), String> {
            Ok(())
        }
    }

    struct PermitCompactTestOperations;

    impl OperationAdmission for PermitCompactTestOperations {
        fn admit(&self, _signed: &SignedBoundOperation) -> Result<(), String> {
            Ok(())
        }

        fn compact_creation_trust(
            &self,
            _proof: &CompactSignedCreation,
        ) -> Result<[u8; 32], String> {
            Ok([0; 32])
        }
    }

    struct RejectTestOperations;

    impl OperationAdmission for RejectTestOperations {
        fn admit(&self, _signed: &SignedBoundOperation) -> Result<(), String> {
            Err("writer not authenticated".into())
        }
    }

    fn packet(value: &[u8]) -> OperationPacket {
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

    async fn relay_receive_blocking(
        relay: &OperationRelay,
        packet: OperationPacket,
    ) -> OperationReceipt {
        let relay = relay.clone();
        tokio::task::spawn_blocking(move || relay.receive(packet).unwrap())
            .await
            .unwrap()
    }

    async fn relay_receive_transfer_blocking(
        relay: &OperationRelay,
        transfer: OperationTransfer,
    ) -> OperationTransferReceipt {
        let relay = relay.clone();
        tokio::task::spawn_blocking(move || relay.receive_transfer(transfer).unwrap())
            .await
            .unwrap()
    }

    fn compact_create_packet(value: &[u8]) -> OperationPacket {
        let bound = packet(value).signed.bound;
        let key = SigningKey::from_bytes(&[7; 32]);
        let creation = SignedCreation::sign(&bound, &key).unwrap();
        OperationPacket {
            signed: SignedBoundOperation::sign(bound, &key, creation).unwrap(),
        }
    }

    #[test]
    fn disabled_create_receipt_without_body_remains_not_targeted() {
        let dir = tempfile::tempdir().unwrap();
        let mut bound = packet(b"initial").signed.bound;
        bound.declaration.mode = CertificationMode::Disabled;
        let key = SigningKey::from_bytes(&[7; 32]);
        let creation = SignedCreation::sign(&bound, &key).unwrap();
        let packet = OperationPacket {
            signed: SignedBoundOperation::sign(bound.clone(), &key, creation).unwrap(),
        };
        let later = later_packet(&packet, b"followup");
        let subject = bound.operation.subject;
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.receive(packet).unwrap();
        inbox.ledger.compact_and_sync(&subject).unwrap();
        assert!(inbox.get(&subject).is_none());
        assert_eq!(
            inbox.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::NotTargeted
        );

        // A follow-up without a Create witness cannot establish the mode.
        let followup_dir = tempfile::tempdir().unwrap();
        let later_id = later.signed.bound.operation.subject.clone();
        let mut followup =
            OperationInbox::open(followup_dir.path(), Arc::new(PermitTestOperations)).unwrap();
        followup.receive(later).unwrap();
        followup.ledger.compact_and_sync(&later_id).unwrap();
        assert_eq!(
            followup.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::Unknown
        );
        drop(followup);
        let followup =
            OperationInbox::open(followup_dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert_eq!(
            followup.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::Unknown
        );
        drop(inbox);
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert_eq!(
            inbox.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::NotTargeted
        );
    }

    #[test]
    fn policy_only_create_is_authenticated_durable_and_excludes_its_value() {
        let dir = tempfile::tempdir().unwrap();
        let signing = SigningKey::from_bytes(&[7; 32]);
        let mut registry = WriterRegistry::new();
        registry
            .register(
                NodeId("writer".into()),
                signing.verifying_key(),
                ["ns".to_owned()].into(),
            )
            .unwrap();
        let admission = Arc::new(AuthenticatedOperationAdmission::new(
            Arc::new(registry),
            "ns".into(),
        ));
        let mut bound = packet(b"private value").signed.bound;
        bound.declaration.mode = CertificationMode::Disabled;
        let creation = SignedCreation::sign(&bound, &signing).unwrap();
        let witness = OperationPacket {
            signed: SignedBoundOperation::sign(bound.clone(), &signing, creation).unwrap(),
        };
        let mut forged = witness.clone();
        forged.signed.operation_signature[0] ^= 1;
        let mut inbox = OperationInbox::open(dir.path(), admission.clone()).unwrap();
        assert!(matches!(
            inbox.receive_policy_witness(forged),
            Err(ReceiveError::Rejected(_))
        ));
        assert!(
            !inbox
                .receive_policy_witness(witness.clone())
                .unwrap()
                .already_present
        );
        assert!(
            inbox
                .receive_policy_witness(witness.clone())
                .unwrap()
                .already_present
        );
        assert!(inbox.get(&bound.operation.subject).is_none());
        assert_eq!(inbox.ordinary_read("ns", "key"), OperationRead::Unknown);
        assert_eq!(
            inbox.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::NotTargeted
        );
        assert!(inbox.receive(packet(b"competing value")).is_err());
        let mut other_create = bound.clone();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut other_create.operation.action {
            *value_bytes = b"different Create".to_vec();
        }
        let conflicting_proof = SignedCreation::sign(&other_create, &signing).unwrap();
        let mut later = later_packet(&witness, b"later value");
        later.signed =
            SignedBoundOperation::sign(later.signed.bound, &signing, conflicting_proof).unwrap();
        assert!(matches!(
            inbox.receive(later),
            Err(ReceiveError::Rejected(_))
        ));
        drop(inbox);
        let inbox = OperationInbox::open(dir.path(), admission.clone()).unwrap();
        assert!(inbox.get(&bound.operation.subject).is_none());
        assert_eq!(
            inbox.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::NotTargeted
        );
        drop(inbox);
        let mut forged = witness;
        forged.signed.operation_signature[0] ^= 1;
        std::fs::write(
            packet_path(&dir.path().join("policy-witness"), &bound.operation.subject),
            serde_json::to_vec(&forged).unwrap(),
        )
        .unwrap();
        assert!(OperationInbox::open(dir.path(), admission).is_err());
    }

    #[test]
    fn interrupted_first_inbox_open_finishes_empty_manifest() {
        for unfinished_temp in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let lock = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(dir.path().join(INBOX_LOCK))
                .unwrap();
            lock.sync_all().unwrap();
            if unfinished_temp {
                let temp_path = dir.path().join(".packet-interrupted.tmp");
                let temp = std::fs::File::create(&temp_path).unwrap();
                temp.sync_all().unwrap();
            }
            crate::store::backend::fsync_dir(dir.path()).unwrap();

            let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            assert!(policy_witness_manifest_path(dir.path()).exists());
            assert!(!dir.path().join(".packet-interrupted.tmp").exists());
            drop(inbox);
            OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        }
    }

    #[test]
    fn missing_policy_witness_after_restart_fails_closed() {
        for compact in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let signing = SigningKey::from_bytes(&[7; 32]);
            let mut registry = WriterRegistry::new();
            registry
                .register(
                    NodeId("writer".into()),
                    signing.verifying_key(),
                    ["ns".to_owned()].into(),
                )
                .unwrap();
            let admission = Arc::new(AuthenticatedOperationAdmission::new(
                Arc::new(registry),
                "ns".into(),
            ));
            let create = compact_create_packet(b"policy value");
            let subject = create.signed.bound.operation.subject.clone();
            let mut inbox = OperationInbox::open(dir.path(), admission.clone()).unwrap();
            inbox.receive_policy_witness(create).unwrap();
            if compact {
                assert!(inbox.compact_policy_witness("ns", "key").unwrap());
            }
            drop(inbox);

            let path = packet_path(&dir.path().join("policy-witness"), &subject);
            std::fs::remove_file(path).unwrap();
            assert!(OperationInbox::open(dir.path(), admission).is_err());
        }
    }

    #[test]
    fn missing_policy_witness_directory_after_restart_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let admission = Arc::new(PermitTestOperations);
        let mut inbox = OperationInbox::open(dir.path(), admission.clone()).unwrap();
        inbox
            .receive_policy_witness(compact_create_packet(b"policy value"))
            .unwrap();
        drop(inbox);

        std::fs::remove_dir_all(dir.path().join("policy-witness")).unwrap();
        assert!(OperationInbox::open(dir.path(), admission).is_err());
    }

    #[test]
    fn missing_policy_witness_manifest_after_restart_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let admission = Arc::new(PermitTestOperations);
        let mut inbox = OperationInbox::open(dir.path(), admission.clone()).unwrap();
        inbox
            .receive_policy_witness(compact_create_packet(b"policy value"))
            .unwrap();
        drop(inbox);

        std::fs::remove_file(policy_witness_manifest_path(dir.path())).unwrap();
        assert!(OperationInbox::open(dir.path(), admission).is_err());
    }

    #[test]
    fn compact_policy_witness_reopens_without_value_and_pins_writer_trust() {
        let dir = tempfile::tempdir().unwrap();
        let signing = SigningKey::from_bytes(&[7; 32]);
        let mut registry = WriterRegistry::new();
        registry
            .register(
                NodeId("writer".into()),
                signing.verifying_key(),
                ["ns".to_owned()].into(),
            )
            .unwrap();
        let admission = Arc::new(AuthenticatedOperationAdmission::new(
            Arc::new(registry.clone()),
            "ns".into(),
        ));
        let mut bound = compact_create_packet(b"SECRET_POLICY_VALUE").signed.bound;
        bound.declaration.mode = CertificationMode::Disabled;
        let proof = SignedCreation::sign(&bound, &signing).unwrap();
        let packet = OperationPacket {
            signed: SignedBoundOperation::sign(bound.clone(), &signing, proof).unwrap(),
        };
        let identity = PolicyWitnessIdentity::from(&packet);
        let path = packet_path(&dir.path().join("policy-witness"), &identity.subject);
        let mut inbox = OperationInbox::open(dir.path(), admission.clone()).unwrap();
        inbox.receive_policy_witness(packet.clone()).unwrap();
        assert!(inbox.compact_policy_witness("ns", "key").unwrap());
        assert!(inbox.policy_witness_packet("ns", "key").is_none());
        assert_eq!(inbox.policy_witness_identity("ns", "key"), Some(identity));
        assert!(
            !std::fs::read(&path)
                .unwrap()
                .windows(b"SECRET_POLICY_VALUE".len())
                .any(|window| window == b"SECRET_POLICY_VALUE")
        );
        drop(inbox);

        let mut changed_registry = registry;
        changed_registry
            .register(
                NodeId("extra".into()),
                SigningKey::from_bytes(&[8; 32]).verifying_key(),
                ["ns".to_owned()].into(),
            )
            .unwrap();
        assert!(
            OperationInbox::open(
                dir.path(),
                Arc::new(AuthenticatedOperationAdmission::new(
                    Arc::new(changed_registry),
                    "ns".into(),
                )),
            )
            .is_err()
        );
        let mut same_registry = WriterRegistry::new();
        same_registry
            .register(
                NodeId("writer".into()),
                signing.verifying_key(),
                ["ns".to_owned()].into(),
            )
            .unwrap();
        assert!(
            OperationInbox::open(
                dir.path(),
                Arc::new(AuthenticatedOperationAdmission::new_for_namespaces(
                    Arc::new(same_registry.clone()),
                    BTreeSet::from(["ns".to_owned(), "other".to_owned()]),
                )),
            )
            .is_err()
        );
        let creation_policy = CreationPolicyManifest::from_json(
            r#"{"version":1,"ranges":[{"namespace_id":"ns","key_prefix":"","default_mode":"Disabled","override_writers":[]}]}"#,
        )
        .unwrap();
        assert!(
            OperationInbox::open(
                dir.path(),
                Arc::new(AuthenticatedOperationAdmission::new_with_creation_policy(
                    Arc::new(same_registry),
                    "ns".into(),
                    Arc::new(creation_policy),
                )),
            )
            .is_err()
        );

        let mut inbox = OperationInbox::open(dir.path(), admission.clone()).unwrap();
        assert_eq!(
            inbox.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::NotTargeted
        );
        assert!(
            inbox
                .receive_policy_witness(packet.clone())
                .unwrap()
                .already_present
        );
        let mut conflicting = packet;
        if let CandidateKvAction::Create { value_bytes, .. } =
            &mut conflicting.signed.bound.operation.action
        {
            *value_bytes = b"another value".to_vec();
        }
        let other_proof = SignedCreation::sign(&conflicting.signed.bound, &signing).unwrap();
        conflicting.signed =
            SignedBoundOperation::sign(conflicting.signed.bound, &signing, other_proof).unwrap();
        assert!(inbox.receive_policy_witness(conflicting).is_err());
        drop(inbox);

        let mut marker: CompactedPolicyWitness =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        marker.creation.signature[0] ^= 1;
        std::fs::write(&path, serde_json::to_vec(&marker).unwrap()).unwrap();
        assert!(OperationInbox::open(dir.path(), admission).is_err());
    }

    #[test]
    fn late_policy_witness_updates_compacted_add_without_restoring_its_value() {
        let dir = tempfile::tempdir().unwrap();
        let signing = SigningKey::from_bytes(&[7; 32]);
        let mut registry = WriterRegistry::new();
        registry
            .register(
                NodeId("writer".into()),
                signing.verifying_key(),
                ["ns".to_owned()].into(),
            )
            .unwrap();
        let admission = Arc::new(AuthenticatedOperationAdmission::new(
            Arc::new(registry),
            "ns".into(),
        ));
        let mut create = packet(b"private create").signed.bound;
        create.declaration.mode = CertificationMode::Disabled;
        let compact_creation = SignedCreation::sign(&create, &signing).unwrap();
        let creation = SignedCreation::sign(&create, &signing).unwrap();
        let witness = OperationPacket {
            signed: SignedBoundOperation::sign(create, &signing, creation).unwrap(),
        };
        let mut add = later_packet(&witness, b"private add");
        add.signed =
            SignedBoundOperation::sign(add.signed.bound, &signing, compact_creation).unwrap();
        let add_id = add.signed.bound.operation.subject.clone();
        let mut inbox = OperationInbox::open(dir.path(), admission.clone()).unwrap();
        inbox.receive(add).unwrap();
        assert_eq!(
            inbox.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::PolicyUnknown
        );
        inbox.ledger.compact_and_sync(&add_id).unwrap();
        assert!(inbox.get(&add_id).is_none());
        assert!(!inbox.creation_policy_known(&add_id));
        inbox.receive_policy_witness(witness).unwrap();
        assert!(inbox.creation_policy_known(&add_id));
        assert_eq!(
            inbox.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::NotTargeted
        );
        assert_eq!(inbox.ordinary_read("ns", "key"), OperationRead::Unknown);
        drop(inbox);
        let inbox = OperationInbox::open(dir.path(), admission).unwrap();
        assert!(inbox.creation_policy_known(&add_id));
        assert_eq!(
            inbox.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::NotTargeted
        );
    }

    fn packet_for_key(key_name: &str, counter: u64) -> OperationPacket {
        packet_for_namespace_key("ns", key_name, counter)
    }

    fn packet_for_namespace_key(
        namespace_id: &str,
        key_name: &str,
        counter: u64,
    ) -> OperationPacket {
        let mut bound = packet(b"authority terminal").signed.bound;
        bound.operation.subject.namespace_id = namespace_id.into();
        bound.operation.subject.event_dot.counter = counter;
        bound.declaration.creation_id = bound.operation.subject.clone();
        bound.declaration.key = key_name.into();
        if let CandidateKvAction::Create { key, .. } = &mut bound.operation.action {
            *key = key_name.into();
        }
        let key = SigningKey::from_bytes(&[7; 32]);
        let creation = SignedCreation::sign(&bound, &key).unwrap();
        OperationPacket {
            signed: SignedBoundOperation::sign(bound, &key, creation).unwrap(),
        }
    }

    fn later_packet(creation: &OperationPacket, value: &[u8]) -> OperationPacket {
        let mut bound = creation.signed.bound.clone();
        bound.operation.subject.event_dot.counter = 2;
        bound.operation.action = CandidateKvAction::Add {
            key: "key".into(),
            value_bytes: value.to_vec(),
            timestamp: HlcTimestamp {
                physical: 2,
                logical: 0,
                node_id: "writer".into(),
            },
        };
        OperationPacket {
            signed: SignedBoundOperation::sign(
                bound,
                &SigningKey::from_bytes(&[7; 32]),
                creation.signed.creation.clone(),
            )
            .unwrap(),
        }
    }

    #[test]
    fn followup_packet_omits_initial_value() {
        let create = packet(&vec![137; 4096]);
        let add = later_packet(&create, b"later");
        let encoded = serde_json::to_value(&add).unwrap();
        assert_eq!(encoded["signed"]["creation"]["version"], 3);
        assert!(encoded["signed"]["creation"].get("bound").is_none());
        assert!(serde_json::to_vec(&add).unwrap().len() < 1500);
        let decoded: OperationPacket =
            serde_json::from_slice(&serde_json::to_vec(&add).unwrap()).unwrap();
        assert_eq!(decoded.commitment(), add.commitment());
    }

    fn competing_creation() -> OperationPacket {
        let mut bound = packet(b"other creation").signed.bound;
        bound.operation.subject.event_dot.counter = 3;
        bound.declaration.creation_id = bound.operation.subject.clone();
        let key = SigningKey::from_bytes(&[7; 32]);
        let creation = SignedCreation::sign(&bound, &key).unwrap();
        OperationPacket {
            signed: SignedBoundOperation::sign(bound, &key, creation).unwrap(),
        }
    }

    fn later_action(
        creation: &OperationPacket,
        counter: u64,
        action: CandidateKvAction,
    ) -> OperationPacket {
        let mut bound = creation.signed.bound.clone();
        bound.operation.subject.event_dot.counter = counter;
        bound.operation.action = action;
        OperationPacket {
            signed: SignedBoundOperation::sign(
                bound,
                &SigningKey::from_bytes(&[7; 32]),
                creation.signed.creation.clone(),
            )
            .unwrap(),
        }
    }

    fn verified_packets(packets: &[OperationPacket]) -> Vec<VerifiedOperation> {
        use crate::authority::certificate::{EpochConfig, KeysetRegistry, KeysetVersion};
        use crate::authority::operation_admission::{
            OperationAdmissionPolicy, OperationLockStore, admit_for_signing,
        };
        use crate::authority::operation_merkle::{
            OperationMerkleCertificate, OperationMerkleTree, OperationRootSupport,
            SignedOperationRoot,
        };
        use crate::operation::{OperationError, OperationHistory};
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
            type Error = OperationError;
            fn record_and_sync(
                &mut self,
                bound: BoundOperation,
            ) -> Result<RecordOutcome, Self::Error> {
                self.0.record(bound)
            }
        }
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "".into(),
        };
        let mut lock = Lock::default();
        let signable: Vec<_> = packets
            .iter()
            .map(|packet| {
                admit_for_signing(&Allow, &mut lock, packet.signed.bound.clone()).unwrap()
            })
            .collect();
        let tree = OperationMerkleTree::from_signable(scope.clone(), &signable).unwrap();
        let key = SigningKey::from_bytes(&[9; 32]);
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![(NodeId("auth".into()), key.verifying_key())],
            )
            .unwrap();
        packets
            .iter()
            .map(|packet| {
                OperationMerkleCertificate {
                    operation: packet.signed.bound.clone(),
                    supports: vec![OperationRootSupport {
                        signed_root: SignedOperationRoot::sign(
                            &tree,
                            KeysetVersion(1),
                            NodeId("auth".into()),
                            &key,
                        ),
                        proof: tree.proof(&packet.signed.bound.operation.subject).unwrap(),
                    }],
                }
                .verify(
                    &registry,
                    &scope,
                    KeysetVersion(1),
                    0,
                    &EpochConfig::default(),
                    0,
                )
                .unwrap()
            })
            .collect()
    }

    fn routed_config(
        destinations: &[&str],
    ) -> (OperationRouteManifest, OperationRequiredDestinations) {
        let mut edges = vec![OperationRouteEdge {
            source: NodeId("a".into()),
            receiver: NodeId("b".into()),
            namespace_id: None,
            key_prefixes: vec!["".into()],
        }];
        let mut rows = Vec::new();
        for destination in destinations {
            let destination = NodeId((*destination).into());
            edges.push(OperationRouteEdge {
                source: NodeId("b".into()),
                receiver: destination.clone(),
                namespace_id: None,
                key_prefixes: vec!["".into()],
            });
            rows.push(OperationRouteRow {
                source: NodeId("a".into()),
                destination: destination.clone(),
                namespace_id: None,
                key_prefix: "".into(),
                next_hop: NodeId("b".into()),
                rank: 2,
            });
            rows.push(OperationRouteRow {
                source: NodeId("b".into()),
                destination: destination.clone(),
                namespace_id: None,
                key_prefix: "".into(),
                next_hop: destination,
                rank: 1,
            });
        }
        let required = serde_json::json!([{"key_prefix":"", "destinations": destinations}]);
        (
            OperationRouteManifest::new(rows, &edges).unwrap(),
            OperationRequiredDestinations::from_json(&required.to_string()).unwrap(),
        )
    }

    #[test]
    fn routed_configuration_preflight_rejects_local_neighbor_permission_gap() {
        let (routes, _) = routed_config(&["c"]);
        let self_id = NodeId("a".into());
        let valid = OperationNeighborManifest::from_json(
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &self_id,
        )
        .unwrap();
        validate_routed_relay_configuration(&valid, &routes, &[]).unwrap();

        let invalid = OperationNeighborManifest::from_json(
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":["x"]}]"#,
            &self_id,
        )
        .unwrap();
        assert!(
            validate_routed_relay_configuration(&invalid, &routes, &[])
                .unwrap_err()
                .to_string()
                .contains("route exceeds local neighbor permission")
        );
    }

    #[tokio::test]
    async fn enabled_submit_stays_local_when_authority_has_no_custody_route() {
        let dir = tempfile::tempdir().unwrap();
        let node = NodeId("a".into());
        let neighbors = OperationNeighborManifest::from_json(
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &node,
        )
        .unwrap();
        let routes = OperationRouteManifest::new(
            vec![OperationRouteRow {
                source: node.clone(),
                destination: NodeId("b".into()),
                namespace_id: None,
                key_prefix: "".into(),
                next_hop: NodeId("b".into()),
                rank: 1,
            }],
            &[OperationRouteEdge {
                source: node.clone(),
                receiver: NodeId("b".into()),
                namespace_id: None,
                key_prefixes: vec!["".into()],
            }],
        )
        .unwrap();
        let required =
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["a"]}]"#)
                .unwrap();
        let mut namespace = SystemNamespace::new();
        namespace.set_authority_definition(
            crate::control_plane::system_namespace::AuthorityDefinition {
                key_range: crate::types::KeyRange { prefix: "".into() },
                authority_nodes: vec![NodeId("b".into())],
                auto_generated: false,
            },
        );
        namespace
            .set_placement_policy(
                crate::placement::PlacementPolicy::new(
                    crate::types::PolicyVersion(1),
                    crate::types::KeyRange { prefix: "".into() },
                    1,
                )
                .with_certified(true),
            )
            .unwrap();
        let namespace = Arc::new(std::sync::RwLock::new(namespace));
        let mounted_namespace = namespace.read().unwrap().clone();
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(dir.path().join("inbox"), Arc::new(PermitTestOperations)).unwrap(),
        ));
        let outbox = Arc::new(Mutex::new(
            OperationOutbox::open_routed_for_node(dir.path().join("outbox"), node).unwrap(),
        ));
        let relay = OperationRelay::new_routed_with_authorities_and_policy(
            Arc::clone(&inbox),
            Arc::clone(&outbox),
            neighbors,
            OperationOwnedPrefixes::from_json(r#"[""]"#).unwrap(),
            routes.clone(),
            required,
            vec![],
            mounted_namespace,
            namespace,
        )
        .await
        .unwrap();
        let body = packet(b"must reach b");
        assert_eq!(
            relay.submit_local(body.clone()).await.unwrap().commitment,
            body.commitment()
        );
        assert_eq!(
            inbox.lock().await.root_packets().unwrap(),
            vec![body.clone()]
        );
        assert!(outbox.lock().await.pending().next().is_none());
        let bound = &body.signed.bound;
        let origin = RootTombstone {
            version: 1,
            subject: bound.operation.subject.clone(),
            key: bound.operation.action.key().into(),
            mode: bound.declaration.mode,
            packet_commitment: body.commitment(),
            bound_commitment: bound.commitment(),
        };
        assert!(!completed_root_sends(
            relay.routed.as_ref().unwrap(),
            &NodeId("a".into()),
            &origin,
            &[]
        ));

        let good_required = OperationRequiredDestinations::from_json(
            r#"[{"key_prefix":"","destinations":["a","b"]}]"#,
        )
        .unwrap();
        let good = RoutedCustody {
            routes,
            required: good_required,
            authorities: vec![],
            submit_authority_policy: relay
                .routed
                .as_ref()
                .unwrap()
                .submit_authority_policy
                .clone(),
            live_authority_policy: relay.routed.as_ref().unwrap().live_authority_policy.clone(),
        };
        validate_root_policy(&good, &body).unwrap();
        let mut missing_policy = good.clone();
        missing_policy.submit_authority_policy = None;
        validate_root_policy(&missing_policy, &body).unwrap();

        let old_digest = routed_custody_manifest_digest(
            &relay.routed.as_ref().unwrap().routes,
            &relay.routed.as_ref().unwrap().required,
            &relay.neighbors,
        )
        .unwrap();
        drop(relay);
        drop(inbox);
        drop(outbox);

        // Reopen the exact trust and neighbor configuration. Only routing's
        // required destination changes; no transfer ID was ever allocated.
        let node = NodeId("a".into());
        let neighbors = OperationNeighborManifest::from_json(
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &node,
        )
        .unwrap();
        let owned = OperationOwnedPrefixes::from_json(r#"[""]"#).unwrap();
        let mounted = good.submit_authority_policy.as_ref().unwrap().as_ref();
        let authority_digest = routed_authority_manifest_digest(&node, &[], Some(mounted)).unwrap();
        let new_digest =
            routed_custody_manifest_digest(&good.routes, &good.required, &neighbors).unwrap();
        assert_ne!(old_digest, new_digest);
        let reopened_inbox = Arc::new(Mutex::new(
            OperationInbox::open(dir.path().join("inbox"), Arc::new(PermitTestOperations)).unwrap(),
        ));
        let reopened_outbox = Arc::new(Mutex::new(
            OperationOutbox::open_routed_for_node(dir.path().join("outbox"), node.clone()).unwrap(),
        ));
        {
            let guard = reopened_inbox.lock().await;
            guard.verify_owned_manifest(owned.digest()).unwrap();
            guard.verify_authority_manifest(authority_digest).unwrap();
            assert!(guard.routing_repin_is_pristine().unwrap());
        }
        {
            let inbox_guard = reopened_inbox.lock().await;
            let mut guard = reopened_outbox.lock().await;
            guard.verify_routed_manifest(old_digest).unwrap();
            assert_eq!(guard.gc_entries().unwrap().count(), 0);
            repair_routed_manifest_if_pristine(&inbox_guard, &mut guard, new_digest).unwrap();
            guard.verify_routed_manifest(new_digest).unwrap();
        }
        let repaired = OperationRelay::new_routed_with_authorities_and_policy(
            Arc::clone(&reopened_inbox),
            Arc::clone(&reopened_outbox),
            neighbors,
            owned,
            good.routes,
            good.required,
            vec![],
            mounted.clone(),
            good.live_authority_policy.unwrap(),
        )
        .await
        .unwrap();
        let sends: Vec<_> = repaired.outbox.lock().await.pending().cloned().collect();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].id, 0);
        assert_eq!(sends[0].destination, Some(NodeId("b".into())));
        assert_eq!(
            reopened_inbox.lock().await.root_packets().unwrap(),
            vec![body]
        );
    }

    #[test]
    fn route_repair_rejects_unrelated_pristine_inbox_in_same_parent() {
        let dir = tempfile::tempdir().unwrap();
        let inbox =
            OperationInbox::open(dir.path().join("inbox-a"), Arc::new(PermitTestOperations))
                .unwrap();
        let mut outbox =
            OperationOutbox::open_routed_for_node(dir.path().join("outbox-b"), NodeId("a".into()))
                .unwrap();
        outbox.ensure_routed_manifest([1; 32]).unwrap();
        assert!(repair_routed_manifest_if_pristine(&inbox, &mut outbox, [2; 32]).is_err());
        outbox.verify_routed_manifest([1; 32]).unwrap();
    }

    #[tokio::test]
    async fn routed_submit_without_destinations_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let config = (
            OperationRouteManifest::new(vec![], &[]).unwrap(),
            OperationRequiredDestinations::from_json("[]").unwrap(),
        );
        let body = packet(b"local pending without a route");
        let first = routed_relay(dir.path(), "a", "[]", &config).await;
        assert_eq!(
            first.submit_local(body.clone()).await.unwrap().commitment,
            body.commitment()
        );
        assert!(first.outbox.lock().await.pending().next().is_none());
        assert!(matches!(
            first.inbox.lock().await.ordinary_read("ns", "key"),
            OperationRead::Value { .. }
        ));
        drop(first);

        let reopened = routed_relay(dir.path(), "a", "[]", &config).await;
        assert_eq!(
            reopened.inbox.lock().await.root_packets().unwrap(),
            vec![body]
        );
        assert!(reopened.outbox.lock().await.pending().next().is_none());
    }

    #[tokio::test]
    async fn routed_submit_enrolls_available_destination_and_retains_missing_one() {
        let dir = tempfile::tempdir().unwrap();
        let (routes, _) = routed_config(&["c"]);
        let required = OperationRequiredDestinations::from_json(
            r#"[{"key_prefix":"","destinations":["c","d"]}]"#,
        )
        .unwrap();
        let config = (routes, required);
        let relay = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let body = packet(b"c reachable, d pending");
        relay.submit_local(body.clone()).await.unwrap();
        let entries: Vec<_> = relay.outbox.lock().await.pending().cloned().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].destination, Some(NodeId("c".into())));
        assert_eq!(relay.inbox.lock().await.root_packets().unwrap(), vec![body]);
    }

    fn routed_policy_for_test(
        config: &(OperationRouteManifest, OperationRequiredDestinations),
    ) -> SystemNamespace {
        let mut namespace = SystemNamespace::new();
        for row in config.1.rows() {
            let prefix = row.key_prefix.clone();
            let policy = crate::placement::PlacementPolicy::new(
                crate::types::PolicyVersion(1),
                crate::types::KeyRange {
                    prefix: prefix.clone(),
                },
                row.destinations.len(),
            )
            .with_certified(true);
            let authority = crate::control_plane::system_namespace::AuthorityDefinition {
                key_range: crate::types::KeyRange { prefix },
                authority_nodes: row.destinations.clone(),
                auto_generated: false,
            };
            if let Some(namespace_id) = &row.namespace_id {
                namespace
                    .set_operation_assignment(
                        crate::control_plane::system_namespace::OperationAssignment {
                            namespace_id: namespace_id.clone(),
                            policy,
                            authority,
                        },
                    )
                    .unwrap();
            } else {
                namespace.set_placement_policy(policy).unwrap();
                namespace.set_authority_definition(authority);
            }
        }
        namespace
    }

    async fn routed_relay(
        root: &Path,
        node: &str,
        neighbors_json: &str,
        config: &(OperationRouteManifest, OperationRequiredDestinations),
    ) -> OperationRelay {
        let namespace = routed_policy_for_test(config);
        let id = NodeId(node.into());
        let mut neighbor_rows: Vec<serde_json::Value> =
            serde_json::from_str(neighbors_json).unwrap();
        let upstream = if node == "b" {
            Some("a")
        } else if node == "c" || node == "d" {
            Some("b")
        } else {
            None
        };
        if let Some(upstream) = upstream
            && !neighbor_rows.iter().any(|row| row["node_id"] == upstream)
        {
            neighbor_rows.push(
                serde_json::json!({"node_id":upstream,"addr":"127.0.0.1:1","key_prefixes":[""]}),
            );
        }
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(
                root.join(format!("inbox-{node}")),
                Arc::new(PermitTestOperations),
            )
            .unwrap(),
        ));
        let outbox = Arc::new(Mutex::new(
            OperationOutbox::open_routed_for_node(root.join(format!("outbox-{node}")), id.clone())
                .unwrap(),
        ));
        OperationRelay::new_routed_with_authorities_and_policy(
            inbox,
            outbox,
            OperationNeighborManifest::from_json(
                &serde_json::to_string(&neighbor_rows).unwrap(),
                &id,
            )
            .unwrap(),
            OperationOwnedPrefixes::from_json(if node == "b" { "[]" } else { "[\"\"]" }).unwrap(),
            config.0.clone(),
            config.1.clone(),
            Vec::new(),
            namespace.clone(),
            Arc::new(std::sync::RwLock::new(namespace)),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn routed_submit_rejects_oversize_completion_before_root_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let destination = "c".repeat(4096);
        let config = routed_config(&[&destination]);
        let relay = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let initial = packet(&vec![0; 4_000_000]);
        let initial_len = serde_json::to_vec(&initial).unwrap().len();
        let target_len = INTERNAL_OPERATION_BODY_LIMIT - 1024;
        assert!(initial_len < target_len);
        let body = packet(&vec![0; 4_000_000 + (target_len - initial_len) / 2]);
        assert!(serde_json::to_vec(&body).unwrap().len() < INTERNAL_OPERATION_BODY_LIMIT);
        let completion = OperationCompletionReport {
            version: 1,
            transfer: OperationTransfer {
                version: 2,
                sender: NodeId("a".into()),
                transfer_id: u64::MAX,
                receiver: NodeId("b".into()),
                destination: Some(NodeId(destination)),
                parent: None,
                depth: 0,
                packet: body.clone(),
            },
        };
        assert!(serde_json::to_vec(&completion).unwrap().len() > INTERNAL_OPERATION_BODY_LIMIT);
        assert_eq!(
            relay.submit_local(body).await,
            Err(StatusCode::PAYLOAD_TOO_LARGE)
        );
        assert!(relay.inbox.lock().await.root_packets().unwrap().is_empty());
        assert!(relay.outbox.lock().await.pending().next().is_none());
    }

    async fn local_authority_origin_relay(
        root: &Path,
        config: &(OperationRouteManifest, OperationRequiredDestinations),
        issued: Option<(CrdtSubject, [u8; 32])>,
    ) -> Result<OperationRelay, OutboxError> {
        use crate::control_plane::system_namespace::AuthorityDefinition;
        use crate::placement::PlacementPolicy;
        use crate::types::{KeyRange, PolicyVersion};

        let mut namespace = SystemNamespace::new();
        namespace
            .set_placement_policy(
                PlacementPolicy::new(PolicyVersion(1), KeyRange { prefix: "".into() }, 1)
                    .with_certified(true),
            )
            .unwrap();
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange { prefix: "".into() },
            authority_nodes: vec![NodeId("a".into())],
            auto_generated: false,
        });
        let mut inbox =
            OperationInbox::open(root.join("inbox-a"), Arc::new(PermitTestOperations)).unwrap();
        if let Some((subject, commitment)) = issued {
            inbox
                .note_verified_local_issuance(subject, commitment)
                .unwrap();
        }
        let inbox = Arc::new(Mutex::new(inbox));
        let outbox = Arc::new(Mutex::new(
            OperationOutbox::open_routed_for_node(root.join("outbox-a"), NodeId("a".into()))
                .unwrap(),
        ));
        OperationRelay::new_routed_with_authorities_and_policy(
            inbox,
            outbox,
            OperationNeighborManifest::from_json(
                r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
                &NodeId("a".into()),
            )
            .unwrap(),
            OperationOwnedPrefixes::from_json(r#"[""]"#).unwrap(),
            config.0.clone(),
            config.1.clone(),
            vec![AuthorityTerminalRole {
                namespace: namespace.clone(),
                scope: OperationScope {
                    namespace_id: "ns".into(),
                    key_prefix: "".into(),
                },
            }],
            namespace.clone(),
            Arc::new(std::sync::RwLock::new(namespace)),
        )
        .await
    }

    #[tokio::test]
    async fn changed_live_authority_policy_rejects_new_root_without_changing_recovery_routes() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let relay = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let body = packet(b"old assignment");
        let routed = relay.routed.as_ref().unwrap();
        validate_root_policy(routed, &body).unwrap();
        relay
            .inbox
            .lock()
            .await
            .receive_local_root(body.clone())
            .unwrap();
        routed
            .live_authority_policy
            .as_ref()
            .unwrap()
            .write()
            .unwrap()
            .set_authority_definition(
                crate::control_plane::system_namespace::AuthorityDefinition {
                    key_range: crate::types::KeyRange { prefix: "".into() },
                    authority_nodes: vec![NodeId("d".into())],
                    auto_generated: false,
                },
            );
        assert!(relay.submit_local(packet_for_key("key", 2)).await.is_err());
        assert_eq!(relay.inbox.lock().await.root_packets().unwrap(), vec![body]);
        // A root synced before the control-plane change still enrolls under
        // the mounted assignment rather than the new live policy.
        let recovered = relay.clone();
        tokio::task::spawn_blocking(move || recovered.reconcile_durable_enrollments())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            relay
                .outbox
                .lock()
                .await
                .pending()
                .next()
                .unwrap()
                .destination,
            Some(NodeId("c".into()))
        );
    }

    #[tokio::test]
    async fn authority_origin_flag_does_not_change_effective_routed_policy() {
        let dir = tempfile::tempdir().unwrap();
        let relay = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &routed_config(&["c"]),
        )
        .await;
        let live = relay
            .routed
            .as_ref()
            .unwrap()
            .live_authority_policy
            .as_ref()
            .unwrap();
        let original = live
            .read()
            .unwrap()
            .operation_authorities_for_key("ns", "key")
            .unwrap()
            .clone();
        live.write().unwrap().set_authority_definition(
            crate::control_plane::system_namespace::AuthorityDefinition {
                auto_generated: !original.auto_generated,
                ..original.clone()
            },
        );
        let body = packet(b"same effective authority");
        assert_eq!(
            relay.submit_local(body.clone()).await.unwrap().commitment,
            body.commitment()
        );
        assert_eq!(relay.inbox.lock().await.root_packets().unwrap(), vec![body]);

        live.write().unwrap().set_authority_definition(
            crate::control_plane::system_namespace::AuthorityDefinition {
                authority_nodes: vec![NodeId("different".into())],
                ..original
            },
        );
        assert!(
            relay
                .submit_local(packet_for_key("other", 2))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn placement_metadata_does_not_change_effective_routed_policy() {
        let dir = tempfile::tempdir().unwrap();
        let relay = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &routed_config(&["c"]),
        )
        .await;
        let live = relay
            .routed
            .as_ref()
            .unwrap()
            .live_authority_policy
            .as_ref()
            .unwrap();
        let mut changed = live
            .read()
            .unwrap()
            .operation_placement_for_key("ns", "key")
            .unwrap()
            .clone();
        changed.version = crate::types::PolicyVersion(2);
        changed.replica_count += 1;
        changed
            .required_tags
            .insert(crate::types::Tag("region:test".into()));
        changed.max_read_latency_ms = Some(25.0);
        live.write()
            .unwrap()
            .set_placement_policy(changed.clone())
            .unwrap();
        let body = packet(b"same effective placement");
        assert_eq!(
            relay.submit_local(body.clone()).await.unwrap().commitment,
            body.commitment()
        );

        changed.certified = false;
        live.write().unwrap().set_placement_policy(changed).unwrap();
        assert!(
            relay
                .submit_local(packet_for_key("other", 2))
                .await
                .is_err()
        );

        let shadow = crate::placement::PlacementPolicy::new(
            crate::types::PolicyVersion(2),
            crate::types::KeyRange {
                prefix: "private/".into(),
            },
            1,
        )
        .with_certified(true);
        live.write().unwrap().set_placement_policy(shadow).unwrap();
        assert!(
            relay
                .submit_local(packet_for_key("private/item", 3))
                .await
                .is_err()
        );
        assert_eq!(relay.inbox.lock().await.root_packets().unwrap(), vec![body]);
    }

    #[tokio::test]
    async fn unrelated_policy_change_preserves_enabled_submit() {
        let dir = tempfile::tempdir().unwrap();
        let relay = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &routed_config(&["c"]),
        )
        .await;
        let routed = relay.routed.as_ref().unwrap();
        let mounted = routed.submit_authority_policy.as_ref().unwrap();
        let live = routed.live_authority_policy.as_ref().unwrap();
        live.write()
            .unwrap()
            .set_placement_policy(
                crate::placement::PlacementPolicy::new(
                    crate::types::PolicyVersion(1),
                    crate::types::KeyRange {
                        prefix: "other/".into(),
                    },
                    1,
                )
                .with_certified(true),
            )
            .unwrap();
        assert_eq!(
            serde_json::to_vec(&mounted.operation_placement_for_key("ns", "key")).unwrap(),
            serde_json::to_vec(
                &live
                    .read()
                    .unwrap()
                    .operation_placement_for_key("ns", "key")
            )
            .unwrap()
        );
        let body = packet(b"unrelated policy");
        assert_eq!(
            relay.submit_local(body.clone()).await.unwrap().commitment,
            body.commitment()
        );
        assert_eq!(relay.inbox.lock().await.root_packets().unwrap(), vec![body]);
    }

    #[tokio::test]
    async fn unrelated_policy_change_reopens_with_old_root_obligations() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let neighbors_json = r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#;
        let relay = routed_relay(dir.path(), "a", neighbors_json, &config).await;
        let first = packet_for_key("key", 1);
        relay.submit_local(first.clone()).await.unwrap();
        let mut live = relay
            .routed
            .as_ref()
            .unwrap()
            .live_authority_policy
            .as_ref()
            .unwrap()
            .read()
            .unwrap()
            .clone();
        live.set_placement_policy(
            crate::placement::PlacementPolicy::new(
                crate::types::PolicyVersion(1),
                crate::types::KeyRange {
                    prefix: "other/".into(),
                },
                1,
            )
            .with_certified(true),
        )
        .unwrap();
        drop(relay);

        let inbox_dir = dir.path().join("inbox-a");
        let mounted = OperationInbox::read_mounted_authority_policy_at(&inbox_dir)
            .unwrap()
            .unwrap();
        assert!(mounted.get_placement_policy("other/").is_none());
        let reopened = OperationRelay::new_routed_with_authorities_and_policy(
            Arc::new(Mutex::new(
                OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap(),
            )),
            Arc::new(Mutex::new(
                OperationOutbox::open_routed_for_node(
                    dir.path().join("outbox-a"),
                    NodeId("a".into()),
                )
                .unwrap(),
            )),
            OperationNeighborManifest::from_json(neighbors_json, &NodeId("a".into())).unwrap(),
            OperationOwnedPrefixes::from_json(r#"[""]"#).unwrap(),
            config.0,
            config.1,
            Vec::new(),
            mounted,
            Arc::new(std::sync::RwLock::new(live)),
        )
        .await
        .unwrap();
        assert!(
            reopened
                .outbox
                .lock()
                .await
                .pending()
                .any(|entry| entry.destination == Some(NodeId("c".into())))
        );
        let second = later_packet(&first, b"after restart");
        reopened.submit_local(second.clone()).await.unwrap();
        assert!(
            reopened
                .inbox
                .lock()
                .await
                .root_packets()
                .unwrap()
                .contains(&second)
        );
    }

    #[tokio::test]
    async fn live_child_assignment_blocks_parent_terminal_intake() {
        use crate::control_plane::system_namespace::{AuthorityDefinition, OperationAssignment};
        use crate::placement::PlacementPolicy;
        use crate::types::{KeyRange, PolicyVersion};

        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let mut mounted = SystemNamespace::new();
        mounted
            .set_placement_policy(
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
        mounted.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "key/".into(),
            },
            authority_nodes: vec![NodeId("c".into())],
            auto_generated: false,
        });
        let live = Arc::new(std::sync::RwLock::new(mounted.clone()));
        let relay = OperationRelay::new_routed_with_authorities_and_policy(
            Arc::new(Mutex::new(
                OperationInbox::open(dir.path().join("inbox-c"), Arc::new(PermitTestOperations))
                    .unwrap(),
            )),
            Arc::new(Mutex::new(
                OperationOutbox::open_routed_for_node(
                    dir.path().join("outbox-c"),
                    NodeId("c".into()),
                )
                .unwrap(),
            )),
            OperationNeighborManifest::from_json(
                r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
                &NodeId("c".into()),
            )
            .unwrap(),
            OperationOwnedPrefixes::from_json("[]").unwrap(),
            config.0,
            config.1,
            vec![AuthorityTerminalRole {
                namespace: mounted.clone(),
                scope: OperationScope {
                    namespace_id: "ns".into(),
                    key_prefix: "key/".into(),
                },
            }],
            mounted,
            live.clone(),
        )
        .await
        .unwrap();
        live.write()
            .unwrap()
            .set_operation_assignment(OperationAssignment {
                namespace_id: "ns".into(),
                policy: PlacementPolicy::new(
                    PolicyVersion(1),
                    KeyRange {
                        prefix: "key/private/".into(),
                    },
                    1,
                )
                .with_certified(true),
                authority: AuthorityDefinition {
                    key_range: KeyRange {
                        prefix: "key/private/".into(),
                    },
                    authority_nodes: vec![NodeId("d".into())],
                    auto_generated: false,
                },
            })
            .unwrap();
        let transfer = |packet, transfer_id| OperationTransfer {
            version: 2,
            sender: NodeId("b".into()),
            transfer_id,
            receiver: NodeId("c".into()),
            destination: Some(NodeId("c".into())),
            parent: Some(TransferId {
                sender: NodeId("a".into()),
                sequence: transfer_id,
            }),
            depth: 1,
            packet,
        };
        let captured = transfer(packet_for_key("key/private/a", 1), 1);
        let sibling = transfer(packet_for_key("key/public", 2), 2);
        let test_relay = relay.clone();
        assert!(
            tokio::task::spawn_blocking(move || test_relay.receive_transfer(captured))
                .await
                .unwrap()
                .is_err()
        );
        assert!(
            relay
                .inbox
                .lock()
                .await
                .received_transfers()
                .unwrap()
                .is_empty()
        );
        let expected = sibling.packet.clone();
        let test_relay = relay.clone();
        assert!(
            tokio::task::spawn_blocking(move || test_relay.receive_transfer(sibling))
                .await
                .unwrap()
                .is_ok()
        );
        assert_eq!(
            relay.inbox.lock().await.signing_packets().unwrap(),
            vec![expected]
        );
    }

    #[tokio::test]
    async fn poisoned_inbox_read_returns_unavailable_until_recovered() {
        let dir = tempfile::tempdir().unwrap();
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(dir.path().join("inbox"), Arc::new(PermitTestOperations)).unwrap(),
        ));
        let relay = OperationRelay::new(
            inbox.clone(),
            Arc::new(Mutex::new(
                OperationOutbox::open(dir.path().join("outbox")).unwrap(),
            )),
            OperationNeighborManifest::from_json("[]", &NodeId("self".into())).unwrap(),
            OperationOwnedPrefixes::from_json("[\"\"]").unwrap(),
        )
        .await
        .unwrap();
        let transfer_dir = {
            let mut guard = inbox.lock().await;
            guard.poisoned = true;
            guard.transfer_dir.clone()
        };
        std::fs::remove_dir(&transfer_dir).unwrap();
        std::fs::write(&transfer_dir, b"blocked").unwrap();
        let app = relay_router(relay.clone(), "secret".into());
        let read = || {
            Request::builder()
                .uri("/api/internal/operations/read/ns/key")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .unwrap()
        };
        assert_eq!(
            app.clone().oneshot(read()).await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let view = unified_operation_view_router(relay, None);
        let request = || {
            Request::builder()
                .uri("/api/operations/view/ns/key?kind=local")
                .body(Body::empty())
                .unwrap()
        };
        assert_eq!(
            view.clone().oneshot(request()).await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        std::fs::remove_file(&transfer_dir).unwrap();
        std::fs::create_dir(&transfer_dir).unwrap();
        assert_eq!(app.oneshot(read()).await.unwrap().status(), StatusCode::OK);
        assert_eq!(
            view.oneshot(request()).await.unwrap().status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn nonowner_application_body_is_rejected_on_relay_open() {
        let dir = tempfile::tempdir().unwrap();
        let inbox_dir = dir.path().join("inbox");
        let outbox_dir = dir.path().join("outbox");
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap(),
        ));
        let outbox = Arc::new(Mutex::new(OperationOutbox::open(&outbox_dir).unwrap()));
        let relay = OperationRelay::new(
            inbox.clone(),
            outbox.clone(),
            OperationNeighborManifest::from_json("[]", &NodeId("self".into())).unwrap(),
            OperationOwnedPrefixes::from_json("[]").unwrap(),
        )
        .await
        .unwrap();
        inbox.lock().await.receive(packet(b"transit")).unwrap();
        drop(relay);
        drop(inbox);
        drop(outbox);

        let reopened_inbox = Arc::new(Mutex::new(
            OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap(),
        ));
        let reopened_outbox = Arc::new(Mutex::new(OperationOutbox::open(&outbox_dir).unwrap()));
        assert!(
            OperationRelay::new(
                reopened_inbox.clone(),
                reopened_outbox.clone(),
                OperationNeighborManifest::from_json("[]", &NodeId("self".into())).unwrap(),
                OperationOwnedPrefixes::from_json("[\"\"]").unwrap(),
            )
            .await
            .is_err()
        );
        assert!(
            OperationRelay::new(
                reopened_inbox,
                reopened_outbox,
                OperationNeighborManifest::from_json("[]", &NodeId("self".into())).unwrap(),
                OperationOwnedPrefixes::from_json("[]").unwrap(),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn nonrouted_transit_receive_keeps_only_custody_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let inbox_dir = dir.path().join("inbox");
        let outbox_dir = dir.path().join("outbox");
        let body = packet(b"transit value");
        let subject = body.signed.bound.operation.subject.clone();
        let neighbors =
            || OperationNeighborManifest::from_json("[]", &NodeId("transit".into())).unwrap();
        let owned = || OperationOwnedPrefixes::from_json("[]").unwrap();
        {
            let inbox = Arc::new(Mutex::new(
                OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap(),
            ));
            let relay = OperationRelay::new(
                inbox.clone(),
                Arc::new(Mutex::new(OperationOutbox::open(&outbox_dir).unwrap())),
                neighbors(),
                owned(),
            )
            .await
            .unwrap();
            let receipt = relay_receive_blocking(&relay, body.clone()).await;
            assert_eq!(receipt.commitment, body.commitment());
            assert!(!receipt.already_present);
            let inbox = inbox.lock().await;
            assert!(inbox.get(&subject).is_none());
            assert!(inbox.receipt(&subject).is_none());
            assert_eq!(inbox.get_packet(&subject), Some(body.clone()));
            assert_eq!(
                inbox.certified_read(&[], "ns", "key").result,
                CertifiedReadResult::Unknown
            );
        }
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap(),
        ));
        let relay = OperationRelay::new(
            inbox.clone(),
            Arc::new(Mutex::new(OperationOutbox::open(&outbox_dir).unwrap())),
            neighbors(),
            owned(),
        )
        .await
        .unwrap();
        assert!(inbox.lock().await.get(&subject).is_none());
        assert_eq!(inbox.lock().await.get_packet(&subject), Some(body.clone()));
        assert!(relay_receive_blocking(&relay, body).await.already_present);
    }

    #[tokio::test]
    async fn nonrouted_transit_late_create_recovers_followup_policy() {
        let dir = tempfile::tempdir().unwrap();
        let inbox_dir = dir.path().join("inbox");
        let outbox_dir = dir.path().join("outbox");
        let create = packet(b"initial");
        let add = later_packet(&create, b"next");
        let add_id = add.signed.bound.operation.subject.clone();
        let neighbors =
            || OperationNeighborManifest::from_json("[]", &NodeId("transit".into())).unwrap();
        let owned = || OperationOwnedPrefixes::from_json("[]").unwrap();
        {
            let inbox = Arc::new(Mutex::new(
                OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap(),
            ));
            let relay = OperationRelay::new(
                inbox.clone(),
                Arc::new(Mutex::new(OperationOutbox::open(&outbox_dir).unwrap())),
                neighbors(),
                owned(),
            )
            .await
            .unwrap();
            relay_receive_blocking(&relay, add.clone()).await;
            assert!(!inbox.lock().await.creation_policy_known(&add_id));
        }
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap(),
        ));
        let relay = OperationRelay::new(
            inbox.clone(),
            Arc::new(Mutex::new(OperationOutbox::open(&outbox_dir).unwrap())),
            neighbors(),
            owned(),
        )
        .await
        .unwrap();
        assert_eq!(inbox.lock().await.get_packet(&add_id), Some(add));
        relay_receive_blocking(&relay, create).await;
        let inbox = inbox.lock().await;
        assert!(inbox.creation_policy_known(&add_id));
        assert!(inbox.get(&add_id).is_none());
    }

    #[test]
    fn direct_custody_without_ack_marker_is_discarded_on_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let body = packet(b"unacknowledged");
        let subject = body.signed.bound.operation.subject.clone();
        {
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            inbox.receive_with_role(body.clone(), false).unwrap();
            assert!(inbox.get_packet(&subject).is_some());
        }
        let mut reopened =
            OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(reopened.get_packet(&subject).is_none());
        assert!(!reopened.custody_receipts.contains_key(&subject));
        reopened.receive_direct_custody(body.clone()).unwrap();
        drop(reopened);
        let recovered = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert_eq!(recovered.get_packet(&subject), Some(body));
        assert!(recovered.get(&subject).is_none());
    }

    #[tokio::test]
    async fn nonrouted_transit_transfer_keeps_only_custody_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let inbox_dir = dir.path().join("inbox");
        let outbox_dir = dir.path().join("outbox");
        let body = packet(b"transfer value");
        let subject = body.signed.bound.operation.subject.clone();
        let transfer = OperationTransfer {
            version: 1,
            sender: NodeId("sender".into()),
            transfer_id: 0,
            receiver: NodeId("transit".into()),
            destination: None,
            parent: None,
            depth: 0,
            packet: body.clone(),
        };
        let neighbors =
            || OperationNeighborManifest::from_json("[]", &NodeId("transit".into())).unwrap();
        let owned = || OperationOwnedPrefixes::from_json("[]").unwrap();
        {
            let inbox = Arc::new(Mutex::new(
                OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap(),
            ));
            let relay = OperationRelay::new(
                inbox.clone(),
                Arc::new(Mutex::new(OperationOutbox::open(&outbox_dir).unwrap())),
                neighbors(),
                owned(),
            )
            .await
            .unwrap();
            relay_receive_transfer_blocking(&relay, transfer.clone()).await;
            assert!(inbox.lock().await.get(&subject).is_none());
        }
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap(),
        ));
        let relay = OperationRelay::new(
            inbox.clone(),
            Arc::new(Mutex::new(OperationOutbox::open(&outbox_dir).unwrap())),
            neighbors(),
            owned(),
        )
        .await
        .unwrap();
        assert!(inbox.lock().await.get(&subject).is_none());
        assert_eq!(inbox.lock().await.get_packet(&subject), Some(body));
        assert!(
            relay_receive_transfer_blocking(&relay, transfer)
                .await
                .already_present
        );
    }

    #[tokio::test]
    async fn completion_tick_recovers_poisoned_inbox_without_new_transfer() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let relay = routed_relay(
            dir.path(),
            "b",
            r#"[{"node_id":"c","addr":"127.0.0.1:2","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let original = packet(b"periodic-recovery");
        let subject = original.signed.bound.operation.subject.clone();
        {
            let mut inbox = relay.inbox.lock().await;
            write_packet_file(&inbox.packet_dir, &subject, &original).unwrap();
            inbox.poisoned = true;
        }
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let worker = spawn_operation_completion_worker(relay.clone(), "secret".into(), shutdown_rx);
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                {
                    let inbox = relay.inbox.lock().await;
                    if !inbox.poisoned {
                        assert_eq!(inbox.get_packet(&subject), Some(original.clone()));
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        shutdown_tx.send(true).unwrap();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn completion_tick_enrolls_durable_root_and_child_after_outbox_failure() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let b = routed_relay(
            dir.path(),
            "b",
            r#"[{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let body = packet(b"online-enrollment-recovery");

        // Block descriptor creation itself. Once the obstruction is removed,
        // there is no persisted send for the retry worker to discover.
        let a_send = dir.path().join("outbox-a/0.send");
        std::fs::create_dir(&a_send).unwrap();
        assert_eq!(
            a.submit_local(body.clone()).await.unwrap(),
            OperationReceipt {
                commitment: body.commitment(),
                already_present: false
            }
        );
        assert_eq!(a.outbox.lock().await.pending().count(), 0);
        assert_eq!(
            a.inbox.lock().await.root_packets().unwrap(),
            vec![body.clone()]
        );
        std::fs::remove_dir(&a_send).unwrap();

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let a_worker = spawn_operation_completion_worker(a.clone(), "secret".into(), shutdown_rx);
        let root = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(entry) = a.outbox.lock().await.pending().next().cloned() {
                    break entry;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        shutdown_tx.send(true).unwrap();
        a_worker.await.unwrap();
        assert_eq!(root.destination, Some(NodeId("c".into())));

        let transfer = entry_transfer_for_test(&root);
        let b_send = dir.path().join("outbox-b/0.send");
        std::fs::create_dir(&b_send).unwrap();
        let b_copy = b.clone();
        let submitted = transfer.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || b_copy.receive_transfer(submitted))
                .await
                .unwrap()
                .unwrap()
                .commitment,
            body.commitment()
        );
        assert_eq!(b.outbox.lock().await.pending().count(), 0);
        assert_eq!(
            b.inbox.lock().await.received_transfers().unwrap(),
            vec![transfer]
        );
        std::fs::remove_dir(&b_send).unwrap();

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let b_worker = spawn_operation_completion_worker(b.clone(), "secret".into(), shutdown_rx);
        let child = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(entry) = b.outbox.lock().await.pending().next().cloned() {
                    break entry;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        shutdown_tx.send(true).unwrap();
        b_worker.await.unwrap();
        assert_eq!(
            child.parent,
            Some(TransferId {
                sender: NodeId("a".into()),
                sequence: root.id
            })
        );
        assert_eq!(child.receiver, NodeId("c".into()));
    }

    #[tokio::test]
    async fn direct_submit_returns_receipt_after_outbox_enrollment_failure() {
        let dir = tempfile::tempdir().unwrap();
        let node = NodeId("a".into());
        let relay = OperationRelay::new(
            Arc::new(Mutex::new(
                OperationInbox::open(dir.path().join("inbox"), Arc::new(PermitTestOperations))
                    .unwrap(),
            )),
            Arc::new(Mutex::new(
                OperationOutbox::open_for_node(dir.path().join("outbox"), node.clone()).unwrap(),
            )),
            OperationNeighborManifest::from_json(
                r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
                &node,
            )
            .unwrap(),
            OperationOwnedPrefixes::from_json(r#"[""]"#).unwrap(),
        )
        .await
        .unwrap();
        let body = packet(b"direct-local-receipt");
        let obstruction = dir.path().join("outbox/0.send");
        std::fs::create_dir(&obstruction).unwrap();
        assert_eq!(
            relay.submit_local(body.clone()).await.unwrap(),
            OperationReceipt {
                commitment: body.commitment(),
                already_present: false
            }
        );
        assert_eq!(relay.outbox.lock().await.pending().count(), 0);
        assert_eq!(
            relay
                .inbox
                .lock()
                .await
                .get_packet(&body.signed.bound.operation.subject),
            Some(body.clone())
        );
        std::fs::remove_dir(&obstruction).unwrap();
        let recovered = relay.clone();
        tokio::task::spawn_blocking(move || recovered.reconcile_durable_enrollments())
            .await
            .unwrap()
            .unwrap();
        let pending = relay.outbox.lock().await.pending().next().cloned().unwrap();
        assert_eq!(pending.packet, Some(body));
    }

    #[tokio::test]
    async fn direct_receive_acks_durable_packet_after_outbox_enrollment_failure() {
        let dir = tempfile::tempdir().unwrap();
        let node = NodeId("b".into());
        let relay = OperationRelay::new(
            Arc::new(Mutex::new(
                OperationInbox::open(dir.path().join("inbox"), Arc::new(PermitTestOperations))
                    .unwrap(),
            )),
            Arc::new(Mutex::new(
                OperationOutbox::open_for_node(dir.path().join("outbox"), node.clone()).unwrap(),
            )),
            OperationNeighborManifest::from_json(
                r#"[{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#,
                &node,
            )
            .unwrap(),
            OperationOwnedPrefixes::from_json(r#"[""]"#).unwrap(),
        )
        .await
        .unwrap();
        let body = packet(b"direct-received-ack");
        let obstruction = dir.path().join("outbox/0.send");
        std::fs::create_dir(&obstruction).unwrap();
        let copy = relay.clone();
        let received = body.clone();
        let receipt = tokio::task::spawn_blocking(move || copy.receive(received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.commitment, body.commitment());
        assert!(!receipt.already_present);
        assert_eq!(relay.outbox.lock().await.pending().count(), 0);
        assert_eq!(
            relay
                .inbox
                .lock()
                .await
                .get_packet(&body.signed.bound.operation.subject),
            Some(body.clone())
        );
        std::fs::remove_dir(&obstruction).unwrap();
        let recovered = relay.clone();
        tokio::task::spawn_blocking(move || recovered.reconcile_durable_enrollments())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            relay.outbox.lock().await.pending().next().unwrap().packet,
            Some(body)
        );
    }

    #[tokio::test]
    async fn completion_tick_recovers_terminal_intake_after_marker_write_failure() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let b = routed_relay(
            dir.path(),
            "b",
            r#"[{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let c = routed_relay(
            dir.path(),
            "c",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        a.submit_local(packet(b"terminal-intake-recovery"))
            .await
            .unwrap();
        let root = a.outbox.lock().await.pending().next().unwrap().clone();
        receive_routed(&b, entry_transfer_for_test(&root)).await;
        let child = b.outbox.lock().await.pending().next().unwrap().clone();
        let transfer = entry_transfer_for_test(&child);

        let marker = transfer_path(
            &c.inbox.lock().await.terminal_dir,
            &transfer.sender,
            transfer.transfer_id,
        );
        std::fs::create_dir(&marker).unwrap();
        let c_copy = c.clone();
        let submitted = transfer.clone();
        assert!(
            tokio::task::spawn_blocking(move || c_copy.receive_transfer(submitted))
                .await
                .unwrap()
                .is_err()
        );
        std::fs::remove_dir(&marker).unwrap();

        let c_copy = c.clone();
        tokio::task::spawn_blocking(move || c_copy.reconcile_durable_enrollments().unwrap())
            .await
            .unwrap();
        assert_eq!(
            c.inbox.lock().await.terminal_transfers().unwrap(),
            vec![transfer]
        );
        let c_copy = c.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || c_copy.pending_completion_reports().unwrap())
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn ledger_gc_finishes_replayed_receipt_without_new_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let neighbors = r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#;
        let relay = routed_relay(dir.path(), "a", neighbors, &config).await;
        let old = packet(b"value awaiting physical collection");
        let subject = old.signed.bound.operation.subject.clone();
        {
            let mut inbox = relay.inbox.lock().await;
            inbox.receive_with_role(old, true).unwrap();
            // Simulate a crash after the receipt is durable and before the
            // GC cycle publishes its checkpoint.
            inbox.ledger.compact_and_sync(&subject).unwrap();
        }
        drop(relay);

        let relay = routed_relay(dir.path(), "a", neighbors, &config).await;
        assert!(relay.inbox.lock().await.ledger.checkpoint_pending());
        let candidate = relay.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || candidate.compact_certified_ledger_bodies(&[]))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        let inbox = relay.inbox.lock().await;
        assert!(!inbox.ledger.checkpoint_pending());
        assert!(inbox.ledger.receipt(&subject).is_some());
        assert!(dir.path().join("inbox-a/operation-checkpoint-v1").exists());
    }

    #[tokio::test]
    async fn ledger_gc_waits_for_required_enrollment_but_not_delivery_ack() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c", "d"]);
        let neighbors = r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#;
        let relay = routed_relay(dir.path(), "a", neighbors, &config).await;
        let old = packet(b"old");
        let winner = later_packet(&old, b"winner");
        let verified = verified_packets(&[old.clone(), winner.clone()]);
        {
            let mut inbox = relay.inbox.lock().await;
            for packet in [&old, &winner] {
                inbox.receive_with_role(packet.clone(), true).unwrap();
                inbox.record_root(packet).unwrap();
            }
        }
        // One durable send per operation is insufficient when another final
        // destination remains required by the pinned manifest.
        {
            let mut outbox = relay.outbox.lock().await;
            for packet in [&old, &winner] {
                outbox
                    .ensure_routed(
                        NodeId("b".into()),
                        "127.0.0.1:8001".into(),
                        packet.clone(),
                        NodeId("c".into()),
                        None,
                        0,
                    )
                    .unwrap();
            }
        }
        let candidate = relay.clone();
        let proofs = verified.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || candidate.compact_certified_ledger_bodies(&proofs))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        let sender = relay.clone();
        let sent = [old.clone(), winner.clone()];
        tokio::task::spawn_blocking(move || {
            for packet in sent {
                sender.submit(packet).unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(relay.outbox.lock().await.pending().count(), 4);
        let candidate = relay.clone();
        let proofs = verified.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || candidate.compact_certified_ledger_bodies(&proofs))
                .await
                .unwrap()
                .unwrap(),
            1
        );
        assert_eq!(relay.outbox.lock().await.pending().count(), 4);
        let ledger_dir = dir.path().join("inbox-a");
        assert!(ledger_dir.join("operation-checkpoint-v1").exists());
        for entry in std::fs::read_dir(&ledger_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                assert!(
                    !std::fs::read(path)
                        .unwrap()
                        .windows(b"old".len())
                        .any(|part| part == b"old")
                );
            }
        }
        drop(relay);
        let relay = routed_relay(dir.path(), "a", neighbors, &config).await;
        let old_id = old.signed.bound.operation.subject.clone();
        {
            let inbox = relay.inbox.lock().await;
            assert!(inbox.get(&old_id).is_none());
            assert_eq!(
                inbox.receipt(&old_id).unwrap().commitment,
                old.signed.bound.commitment()
            );
            assert_eq!(inbox.get_packet(&old_id), Some(old.clone()));
            assert_eq!(
                inbox.ordinary_read("ns", "key"),
                OperationRead::Value {
                    value_bytes: b"winner".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 2,
                        logical: 0,
                        node_id: "writer".into()
                    },
                }
            );
            assert_eq!(
                inbox.certified_read(&verified, "ns", "key").result,
                CertifiedReadResult::Value {
                    value_bytes: b"winner".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 2,
                        logical: 0,
                        node_id: "writer".into()
                    },
                }
            );
        }
        assert_eq!(relay.outbox.lock().await.pending().count(), 4);
        let retry = relay.clone();
        tokio::task::spawn_blocking(move || {
            assert!(retry.submit(old).unwrap().already_present);
            assert!(retry.submit(packet(b"conflict")).is_err());
            assert!(retry.submit(competing_creation()).is_err());
        })
        .await
        .unwrap();
        assert!(relay.inbox.lock().await.get(&old_id).is_none());

        let delete = later_action(&winner, 3, CandidateKvAction::Delete { key: "key".into() });
        let pending = later_action(
            &winner,
            4,
            CandidateKvAction::Add {
                key: "key".into(),
                value_bytes: b"pending".to_vec(),
                timestamp: HlcTimestamp {
                    physical: 4,
                    logical: 0,
                    node_id: "writer".into(),
                },
            },
        );
        let sender = relay.clone();
        let to_send = [pending.clone(), delete.clone()];
        tokio::task::spawn_blocking(move || {
            for packet in to_send {
                sender.submit(packet).unwrap();
            }
        })
        .await
        .unwrap();
        let mut proofs = verified;
        proofs.extend(verified_packets(std::slice::from_ref(&delete)));
        let candidate = relay.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || candidate.compact_certified_ledger_bodies(&proofs))
                .await
                .unwrap()
                .unwrap(),
            1
        );
        let inbox = relay.inbox.lock().await;
        assert!(inbox.get(&winner.signed.bound.operation.subject).is_none());
        assert!(inbox.get(&pending.signed.bound.operation.subject).is_some());
        assert_eq!(inbox.ordinary_read("ns", "key"), OperationRead::Deleted);
        drop(inbox);
        let after_delete = later_action(
            &winner,
            5,
            CandidateKvAction::Add {
                key: "key".into(),
                value_bytes: b"late-local".to_vec(),
                timestamp: HlcTimestamp {
                    physical: 5,
                    logical: 0,
                    node_id: "writer".into(),
                },
            },
        );
        let delayed_remote = after_delete.clone();
        let local = relay.clone();
        tokio::task::spawn_blocking(move || {
            assert!(local.submit(delete).unwrap().already_present);
            assert!(matches!(
                local.submit(after_delete),
                Err(RelayReceiveError::KeyDeleted)
            ));
        })
        .await
        .unwrap();
        let mut inbox = relay.inbox.lock().await;
        inbox.receive_with_role(delayed_remote, true).unwrap();
        assert_eq!(inbox.ordinary_read("ns", "key"), OperationRead::Deleted);
    }

    #[tokio::test]
    async fn nonowner_authority_terminal_intake_survives_restart_and_rejects_untrusted_role() {
        use crate::control_plane::system_namespace::AuthorityDefinition;
        use crate::placement::PlacementPolicy;
        use crate::types::{KeyRange, PolicyVersion};

        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        a.submit_local(packet(b"authority terminal")).await.unwrap();
        let root = a.outbox.lock().await.pending().next().unwrap().clone();
        let b = routed_relay(
            dir.path(),
            "b",
            r#"[{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        receive_routed(&b, entry_transfer_for_test(&root)).await;
        let child = b.outbox.lock().await.pending().next().unwrap().clone();
        let transfer = entry_transfer_for_test(&child);

        let mut namespace = SystemNamespace::new();
        namespace
            .set_placement_policy(
                PlacementPolicy::new(
                    PolicyVersion(1),
                    KeyRange {
                        prefix: "key".into(),
                    },
                    1,
                )
                .with_certified(true),
            )
            .unwrap();
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "key".into(),
            },
            authority_nodes: vec![NodeId("c".into())],
            auto_generated: false,
        });
        let role = AuthorityTerminalRole {
            namespace,
            scope: OperationScope {
                namespace_id: "ns".into(),
                key_prefix: "key".into(),
            },
        };
        let make_c = |role: Option<AuthorityTerminalRole>| {
            let config = config.clone();
            // The negative case has a different fixed Authority assignment.
            // Its empty first mount is pinned and cannot be reused as C.
            let path = if role.is_some() {
                dir.path().to_path_buf()
            } else {
                dir.path().join("unauthorized")
            };
            async move {
                let id = NodeId("c".into());
                OperationRelay::new_routed_with_authority(
                    Arc::new(Mutex::new(
                        OperationInbox::open(path.join("inbox-c"), Arc::new(PermitTestOperations))
                            .unwrap(),
                    )),
                    Arc::new(Mutex::new(
                        OperationOutbox::open_routed_for_node(path.join("outbox-c"), id.clone())
                            .unwrap(),
                    )),
                    OperationNeighborManifest::from_json(
                        r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
                        &id,
                    )
                    .unwrap(),
                    OperationOwnedPrefixes::from_json("[]").unwrap(),
                    config.0,
                    config.1,
                    role,
                )
                .await
            }
        };
        let unauthorized = make_c(None).await.unwrap();
        let rejected = {
            let relay = unauthorized.clone();
            let transfer = transfer.clone();
            tokio::task::spawn_blocking(move || relay.receive_transfer(transfer))
                .await
                .unwrap()
        };
        assert!(rejected.is_err());
        assert!(
            unauthorized
                .inbox
                .lock()
                .await
                .received_transfers()
                .unwrap()
                .is_empty()
        );
        drop(unauthorized);

        let c = make_c(Some(role.clone())).await.unwrap();
        let receipt = receive_routed(&c, transfer.clone()).await;
        assert_eq!(receipt.commitment, transfer.packet.commitment());
        assert_eq!(
            c.inbox.lock().await.signing_packets().unwrap(),
            vec![transfer.packet.clone()]
        );
        assert_eq!(
            c.inbox.lock().await.terminal_transfers().unwrap(),
            vec![transfer.clone()]
        );
        let completion = {
            let relay = c.clone();
            tokio::task::spawn_blocking(move || relay.pending_completion_reports().unwrap())
                .await
                .unwrap()
        };
        assert_eq!(completion.len(), 1);
        drop(c);
        let c = make_c(Some(role)).await.unwrap();
        assert_eq!(
            c.inbox.lock().await.signing_packets().unwrap(),
            vec![transfer.packet]
        );
        let completion = {
            let relay = c.clone();
            tokio::task::spawn_blocking(move || relay.pending_completion_reports().unwrap())
                .await
                .unwrap()
        };
        assert_eq!(completion.len(), 1);
    }

    #[tokio::test]
    async fn disabled_operation_completes_at_required_authority_without_signing() {
        use crate::control_plane::system_namespace::AuthorityDefinition;
        use crate::placement::PlacementPolicy;
        use crate::types::{KeyRange, PolicyVersion};

        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let b = routed_relay(
            dir.path(),
            "b",
            r#"[{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let mut body = packet(b"eventual authority terminal").signed.bound;
        body.declaration.mode = CertificationMode::Disabled;
        let key = SigningKey::from_bytes(&[7; 32]);
        let proof = SignedCreation::sign(&body, &key).unwrap();
        let packet = OperationPacket {
            signed: SignedBoundOperation::sign(body, &key, proof).unwrap(),
        };
        let subject = packet.signed.bound.operation.subject.clone();
        a.submit_local(packet).await.unwrap();
        let root = a.outbox.lock().await.pending().next().unwrap().clone();
        receive_routed(&b, entry_transfer_for_test(&root)).await;
        let child = b.outbox.lock().await.pending().next().unwrap().clone();
        let transfer = entry_transfer_for_test(&child);

        let mut namespace = SystemNamespace::new();
        namespace
            .set_placement_policy(
                PlacementPolicy::new(
                    PolicyVersion(1),
                    KeyRange {
                        prefix: "key".into(),
                    },
                    1,
                )
                .with_certified(true),
            )
            .unwrap();
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "key".into(),
            },
            authority_nodes: vec![NodeId("c".into())],
            auto_generated: false,
        });
        let role = AuthorityTerminalRole {
            namespace,
            scope: OperationScope {
                namespace_id: "ns".into(),
                key_prefix: "key".into(),
            },
        };
        let open_c = || {
            let role = role.clone();
            let config = config.clone();
            async {
                let id = NodeId("c".into());
                OperationRelay::new_routed_with_authority(
                    Arc::new(Mutex::new(
                        OperationInbox::open(
                            dir.path().join("inbox-c"),
                            Arc::new(PermitTestOperations),
                        )
                        .unwrap(),
                    )),
                    Arc::new(Mutex::new(
                        OperationOutbox::open_routed_for_node(
                            dir.path().join("outbox-c"),
                            id.clone(),
                        )
                        .unwrap(),
                    )),
                    OperationNeighborManifest::from_json(
                        r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
                        &id,
                    )
                    .unwrap(),
                    OperationOwnedPrefixes::from_json("[]").unwrap(),
                    config.0,
                    config.1,
                    Some(role),
                )
                .await
                .unwrap()
            }
        };
        let c = open_c().await;
        let child_ack = receive_routed(&c, transfer.clone()).await;
        {
            let inbox = c.inbox.lock().await;
            assert!(inbox.get(&subject).is_none());
            assert!(inbox.signing_packets().unwrap().is_empty());
            assert_eq!(inbox.terminal_transfers().unwrap(), vec![transfer.clone()]);
        }
        let report_count = {
            let relay = c.clone();
            tokio::task::spawn_blocking(move || relay.pending_completion_reports().unwrap().len())
                .await
                .unwrap()
        };
        assert_eq!(report_count, 1);
        drop(c);
        let c = open_c().await;
        assert!(c.inbox.lock().await.get(&subject).is_none());
        let report_count = {
            let relay = c.clone();
            tokio::task::spawn_blocking(move || relay.pending_completion_reports().unwrap().len())
                .await
                .unwrap()
        };
        assert_eq!(report_count, 1);

        let b_copy = b.clone();
        let child_report = OperationCompletionReport {
            version: 1,
            transfer: transfer.clone(),
        };
        tokio::task::spawn_blocking(move || b_copy.receive_completion(child_report).unwrap())
            .await
            .unwrap();
        {
            let mut outbox = b.outbox.lock().await;
            outbox.record_receipt(child.id, child_ack).unwrap();
            assert_eq!(outbox.compact_one().unwrap(), Some(child.id));
        }
        let notice = b.outbox.lock().await.retirement_notices().pop().unwrap().1;
        let c_copy = c.clone();
        tokio::task::spawn_blocking(move || c_copy.receive_retirement(notice).unwrap())
            .await
            .unwrap();
        assert!(
            c.inbox
                .lock()
                .await
                .terminal_transfers()
                .unwrap()
                .is_empty()
        );
        let report_count = {
            let relay = c.clone();
            tokio::task::spawn_blocking(move || relay.pending_completion_reports().unwrap().len())
                .await
                .unwrap()
        };
        assert_eq!(report_count, 0);
    }

    #[tokio::test]
    async fn routed_terminal_selects_nested_authority_ranges_and_recovers_both() {
        use crate::control_plane::system_namespace::AuthorityDefinition;
        use crate::placement::PlacementPolicy;
        use crate::types::{KeyRange, PolicyVersion};

        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let b = routed_relay(
            dir.path(),
            "b",
            r#"[{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let packets = [
            packet_for_key("orders/public/1", 1),
            packet_for_key("orders/private/1", 2),
            packet_for_namespace_key("other-ns", "orders/public/1", 3),
        ];
        for packet in &packets {
            a.submit_local(packet.clone()).await.unwrap();
        }
        let roots = a.outbox.lock().await.pending().cloned().collect::<Vec<_>>();
        for root in &roots {
            receive_routed(&b, entry_transfer_for_test(root)).await;
        }
        let transfers = b
            .outbox
            .lock()
            .await
            .pending()
            .map(entry_transfer_for_test)
            .collect::<Vec<_>>();
        assert_eq!(transfers.len(), 3);

        let mut namespace = SystemNamespace::new();
        for prefix in ["orders/", "orders/private/"] {
            namespace
                .set_placement_policy(
                    PlacementPolicy::new(
                        PolicyVersion(1),
                        KeyRange {
                            prefix: prefix.into(),
                        },
                        1,
                    )
                    .with_certified(true),
                )
                .unwrap();
            namespace.set_authority_definition(AuthorityDefinition {
                key_range: KeyRange {
                    prefix: prefix.into(),
                },
                authority_nodes: vec![NodeId("c".into())],
                auto_generated: false,
            });
        }
        let roles = [
            ("ns", "orders/"),
            ("ns", "orders/private/"),
            ("other-ns", "orders/"),
        ]
        .map(|(namespace_id, prefix)| AuthorityTerminalRole {
            namespace: namespace.clone(),
            scope: OperationScope {
                namespace_id: namespace_id.into(),
                key_prefix: prefix.into(),
            },
        })
        .to_vec();
        let mount = |authorities: Vec<AuthorityTerminalRole>| {
            let path = dir.path().to_path_buf();
            let config = config.clone();
            async move {
                let id = NodeId("c".into());
                OperationRelay::new_routed_with_authorities(
                    Arc::new(Mutex::new(
                        OperationInbox::open(path.join("inbox-c"), Arc::new(PermitTestOperations))
                            .unwrap(),
                    )),
                    Arc::new(Mutex::new(
                        OperationOutbox::open_routed_for_node(path.join("outbox-c"), id.clone())
                            .unwrap(),
                    )),
                    OperationNeighborManifest::from_json(
                        r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
                        &id,
                    )
                    .unwrap(),
                    OperationOwnedPrefixes::from_json("[\"orders/\"]").unwrap(),
                    config.0,
                    config.1,
                    authorities,
                )
                .await
            }
        };
        let mut c = mount(roles.clone()).await.unwrap();
        for transfer in &transfers {
            receive_routed(&c, transfer.clone()).await;
        }
        assert_eq!(c.inbox.lock().await.signing_packets().unwrap().len(), 3);
        assert_eq!(c.inbox.lock().await.terminal_transfers().unwrap().len(), 3);
        let completion = {
            let relay = c.clone();
            tokio::task::spawn_blocking(move || relay.pending_completion_reports().unwrap())
                .await
                .unwrap()
        };
        assert_eq!(completion.len(), 3);
        drop(c);
        c = mount(roles.clone()).await.unwrap();
        assert_eq!(c.inbox.lock().await.signing_packets().unwrap().len(), 3);
        assert_eq!(c.inbox.lock().await.terminal_transfers().unwrap().len(), 3);
        drop(c);

        assert!(mount(vec![roles[0].clone()]).await.is_err());
        let mut duplicated = roles;
        duplicated[1].scope.key_prefix = duplicated[0].scope.key_prefix.clone();
        let duplicate = mount(duplicated).await.err().unwrap();
        assert!(
            duplicate
                .to_string()
                .contains("Authority terminal scope is duplicated")
        );
    }

    #[tokio::test]
    async fn routed_root_selects_final_destination_by_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let (routes, _) = routed_config(&["c", "d"]);
        let required = OperationRequiredDestinations::from_json(
            r#"[{"namespace_id":"ns","key_prefix":"","destinations":["c"]},{"namespace_id":"other-ns","key_prefix":"","destinations":["d"]}]"#,
        )
        .unwrap();
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &(routes, required),
        )
        .await;
        a.submit_local(packet_for_namespace_key("ns", "item/1", 1))
            .await
            .unwrap();
        a.submit_local(packet_for_namespace_key("other-ns", "item/1", 1))
            .await
            .unwrap();
        let pending = a.outbox.lock().await.pending().cloned().collect::<Vec<_>>();
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().any(|entry| {
            entry.subject.namespace_id == "ns" && entry.destination == Some(NodeId("c".into()))
        }));
        assert!(pending.iter().any(|entry| {
            entry.subject.namespace_id == "other-ns"
                && entry.destination == Some(NodeId("d".into()))
        }));
    }

    #[tokio::test]
    async fn routed_root_selects_namespace_specific_next_hop_and_recovers_it() {
        let dir = tempfile::tempdir().unwrap();
        let edges = vec![
            OperationRouteEdge {
                source: NodeId("a".into()),
                receiver: NodeId("b".into()),
                namespace_id: None,
                key_prefixes: vec!["".into()],
            },
            OperationRouteEdge {
                source: NodeId("b".into()),
                receiver: NodeId("d".into()),
                namespace_id: None,
                key_prefixes: vec!["".into()],
            },
            OperationRouteEdge {
                source: NodeId("a".into()),
                receiver: NodeId("c".into()),
                namespace_id: Some("other-ns".into()),
                key_prefixes: vec!["".into()],
            },
            OperationRouteEdge {
                source: NodeId("c".into()),
                receiver: NodeId("d".into()),
                namespace_id: Some("other-ns".into()),
                key_prefixes: vec!["".into()],
            },
        ];
        let rows = r#"[
            {"source":"a","destination":"d","key_prefix":"","next_hop":"b","rank":2},
            {"source":"b","destination":"d","key_prefix":"","next_hop":"d","rank":1},
            {"source":"a","destination":"d","namespace_id":"other-ns","key_prefix":"","next_hop":"c","rank":2},
            {"source":"c","destination":"d","namespace_id":"other-ns","key_prefix":"","next_hop":"d","rank":1}
        ]"#;
        let config = (
            OperationRouteManifest::from_json(rows, &edges).unwrap(),
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["d"]}]"#)
                .unwrap(),
        );
        let neighbors = r#"[
            {"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]},
            {"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[],"scoped_key_prefixes":[{"namespace_id":"other-ns","key_prefix":""}]}
        ]"#;
        let a = routed_relay(dir.path(), "a", neighbors, &config).await;
        a.submit_local(packet_for_namespace_key("ns", "item/1", 1))
            .await
            .unwrap();
        a.submit_local(packet_for_namespace_key("other-ns", "item/1", 1))
            .await
            .unwrap();
        let pending = a.outbox.lock().await.pending().cloned().collect::<Vec<_>>();
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().any(|entry| {
            entry.subject.namespace_id == "ns" && entry.peer_addr == "127.0.0.1:8001"
        }));
        assert!(pending.iter().any(|entry| {
            entry.subject.namespace_id == "other-ns" && entry.peer_addr == "127.0.0.1:8002"
        }));
        drop(a);
        let restarted = routed_relay(dir.path(), "a", neighbors, &config).await;
        assert_eq!(restarted.outbox.lock().await.pending().count(), 2);
    }

    #[tokio::test]
    async fn routed_custody_restarts_and_keeps_destinations_distinct_at_shared_hop() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c", "d"]);
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let body = packet(b"routed");
        a.submit_local(body.clone()).await.unwrap();
        let a_entries = a.outbox.lock().await.pending().cloned().collect::<Vec<_>>();
        assert_eq!(a_entries.len(), 2);
        assert_ne!(a_entries[0].destination, a_entries[1].destination);
        assert_eq!(a_entries[0].receiver, a_entries[1].receiver);
        let b_neighbors = r#"[{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]},{"node_id":"d","addr":"127.0.0.1:8003","key_prefixes":[""]}]"#;
        let b = routed_relay(dir.path(), "b", b_neighbors, &config).await;
        let mut first_child = None;
        for entry in &a_entries {
            let transfer = entry_transfer_for_test(entry);
            let receipt = receive_routed(&b, transfer.clone()).await;
            a.outbox
                .lock()
                .await
                .record_receipt(entry.id, receipt)
                .unwrap();
            if entry.destination.as_ref() == Some(&NodeId("c".into())) {
                first_child = Some(transfer);
            }
        }
        let child_entries = b.outbox.lock().await.pending().cloned().collect::<Vec<_>>();
        assert_eq!(child_entries.len(), 2);
        assert!(
            child_entries
                .iter()
                .all(|entry| entry.depth == 1 && entry.parent.is_some())
        );
        drop(b);
        let b = routed_relay(dir.path(), "b", b_neighbors, &config).await;
        assert_eq!(b.outbox.lock().await.pending().count(), 2);
        assert!(
            receive_routed(&b, first_child.unwrap())
                .await
                .already_present
        );
        assert_eq!(b.outbox.lock().await.pending().count(), 2);
        let c = routed_relay(dir.path(), "c", "[]", &config).await;
        let child = child_entries
            .into_iter()
            .find(|entry| entry.destination == Some(NodeId("c".into())))
            .unwrap();
        let receipt = receive_routed(&c, entry_transfer_for_test(&child)).await;
        b.outbox
            .lock()
            .await
            .record_receipt(child.id, receipt)
            .unwrap();
        assert_eq!(b.outbox.lock().await.pending().count(), 1);
        assert_eq!(c.outbox.lock().await.pending().count(), 0);
        drop(a);
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        assert_eq!(a.outbox.lock().await.pending().count(), 0);
    }

    #[tokio::test]
    async fn routed_receiver_rejects_wrong_hop_before_storing_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let b = routed_relay(
            dir.path(),
            "b",
            r#"[{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let wrong = OperationTransfer {
            version: 2,
            sender: NodeId("c".into()),
            transfer_id: 4,
            receiver: NodeId("b".into()),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet: packet(b"invalid"),
        };
        let b_clone = b.clone();
        assert!(
            tokio::task::spawn_blocking(move || b_clone.receive_transfer(wrong))
                .await
                .unwrap()
                .is_err()
        );
        assert!(
            b.inbox
                .lock()
                .await
                .received_transfers()
                .unwrap()
                .is_empty()
        );
        assert!(b.inbox.lock().await.packets().is_empty());
    }

    #[tokio::test]
    async fn completion_is_exact_and_does_not_replace_custody_ack() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        a.submit_local(packet(b"completion")).await.unwrap();
        let entry = a.outbox.lock().await.pending().next().unwrap().clone();
        let report = OperationCompletionReport {
            version: 1,
            transfer: entry_transfer_for_test(&entry),
        };
        let mut wrong = report.clone();
        wrong.transfer.destination = Some(NodeId("other".into()));
        let a_copy = a.clone();
        assert!(
            tokio::task::spawn_blocking(move || a_copy.receive_completion(wrong))
                .await
                .unwrap()
                .is_err()
        );
        let a_copy = a.clone();
        tokio::task::spawn_blocking(move || a_copy.receive_completion(report).unwrap())
            .await
            .unwrap();
        assert_eq!(a.outbox.lock().await.pending().count(), 1);
        drop(a);
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        assert_eq!(a.outbox.lock().await.pending().count(), 1);
        assert!(
            a.outbox
                .lock()
                .await
                .get(entry.id)
                .unwrap()
                .completion
                .is_some()
        );
    }

    #[tokio::test]
    async fn routed_relay_custody_stays_out_of_application_ledger_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let body = packet(b"relay-only");
        let subject = body.signed.bound.operation.subject.clone();
        a.submit_local(body.clone()).await.unwrap();
        let root = a.outbox.lock().await.pending().next().unwrap().clone();
        let transfer = entry_transfer_for_test(&root);
        let b = routed_relay(
            dir.path(),
            "b",
            r#"[{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let b_copy = b.clone();
        let first = tokio::task::spawn_blocking({
            let transfer = transfer.clone();
            move || b_copy.receive_transfer(transfer).unwrap()
        })
        .await
        .unwrap();
        assert!(!first.already_present);
        assert!(b.inbox.lock().await.get(&subject).is_none());
        assert_eq!(b.inbox.lock().await.packets(), vec![body.clone()]);
        assert_eq!(b.outbox.lock().await.pending().count(), 1);
        drop(b);
        let b = routed_relay(
            dir.path(),
            "b",
            r#"[{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        assert!(b.inbox.lock().await.get(&subject).is_none());
        assert_eq!(b.outbox.lock().await.pending().count(), 1);
        let b_copy = b.clone();
        let retry = tokio::task::spawn_blocking(move || b_copy.receive_transfer(transfer).unwrap())
            .await
            .unwrap();
        assert!(retry.already_present);
        let child = b.outbox.lock().await.pending().next().unwrap().clone();
        let c = routed_relay(
            dir.path(),
            "c",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let c_copy = c.clone();
        tokio::task::spawn_blocking(move || {
            c_copy
                .receive_transfer(entry_transfer_for_test(&child))
                .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(c.inbox.lock().await.get(&subject), Some(body.signed.bound));
    }

    #[tokio::test]
    async fn routed_inbox_rejects_standalone_receiver_routes() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let relay = routed_relay(
            dir.path(),
            "b",
            r#"[{"node_id":"a","addr":"127.0.0.1:8001","key_prefixes":[""]},{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let body = packet(b"standalone bypass");
        let transfer = OperationTransfer {
            version: 2,
            sender: NodeId("a".into()),
            transfer_id: 0,
            receiver: NodeId("b".into()),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet: body.clone(),
        };
        let app = receiver_router_for_node(relay.inbox(), "secret".into(), NodeId("b".into()));
        let request = |path: &str, value: Vec<u8>| {
            Request::builder()
                .method("POST")
                .uri(path)
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(value))
                .unwrap()
        };
        assert_eq!(
            app.clone()
                .oneshot(request(
                    OPERATION_RECEIVE_PATH,
                    serde_json::to_vec(&body).unwrap()
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            app.clone()
                .oneshot(request(
                    OPERATION_TRANSFER_PATH,
                    serde_json::to_vec(&transfer).unwrap()
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            app.oneshot(
                Request::builder()
                    .uri("/api/internal/operations/read/ns/key")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
            StatusCode::FORBIDDEN
        );
        assert!(
            relay
                .inbox
                .lock()
                .await
                .get(&body.signed.bound.operation.subject)
                .is_none()
        );
    }

    #[test]
    fn retired_relay_custody_reclaims_value_and_keeps_exact_identity() {
        let dir = tempfile::tempdir().unwrap();
        let node = NodeId("b".into());
        let body = packet(b"retired relay value");
        let subject = body.signed.bound.operation.subject.clone();
        let transfer = OperationTransfer {
            version: 2,
            sender: NodeId("a".into()),
            transfer_id: 0,
            receiver: node.clone(),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet: body.clone(),
        };
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox
            .receive_transfer_with_role(transfer.clone(), &node, false)
            .unwrap();
        assert!(inbox.get(&subject).is_none());
        assert!(inbox.get_packet(&subject).is_some());
        // Recovery can rebuild a value-free receipt from the authenticated
        // packet if a crash interrupts the custody receipt write.
        std::fs::remove_file(packet_path(&inbox.custody_receipt_dir, &subject)).unwrap();
        drop(inbox);
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(inbox.custody_receipts.contains_key(&subject));
        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: NodeId("a".into()),
                    receiver: node.clone(),
                    closed_before: 1,
                },
                &node,
            )
            .unwrap();
        assert!(inbox.get_packet(&subject).is_none());
        assert!(inbox.creation_policy_known(&subject));
        drop(inbox);

        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(inbox.get(&subject).is_none());
        assert!(inbox.get_packet(&subject).is_none());
        assert!(inbox.creation_policy_known(&subject));
        assert!(
            inbox
                .receive_transfer_with_role(transfer.clone(), &node, false)
                .is_err()
        );
        let mut conflicting = transfer.clone();
        conflicting.sender = NodeId("d".into());
        conflicting.packet = packet(b"different same dot");
        assert!(
            inbox
                .receive_transfer_with_role(conflicting, &node, false)
                .is_err()
        );
        let competing_create = OperationTransfer {
            version: 2,
            sender: NodeId("e".into()),
            transfer_id: 0,
            receiver: node.clone(),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet: packet_for_key("key", 2),
        };
        assert!(matches!(
            inbox.receive_transfer_with_role(competing_create, &node, false),
            Err(ReceiveError::Rejected(_))
        ));
        let mut fresh = transfer;
        fresh.sender = NodeId("d".into());
        inbox
            .receive_transfer_with_role(fresh, &node, false)
            .unwrap();
        assert!(inbox.get(&subject).is_none());
        assert_eq!(inbox.get_packet(&subject), Some(body));
        drop(inbox);
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(inbox.get(&subject).is_none());
        assert!(inbox.get_packet(&subject).is_some());
    }

    #[tokio::test]
    async fn ordinary_create_retirement_before_policy_worker_keeps_delivery_witness() {
        use crate::network::operation_policy_outbox::OperationPolicyOutbox;
        use crate::network::operation_policy_transport::{PolicyWitnessRelay, policy_route_digest};

        let dir = tempfile::tempdir().unwrap();
        let node = NodeId("b".into());
        let body = compact_create_packet(b"retired before policy worker");
        let subject = body.signed.bound.operation.subject.clone();
        let mut writers = WriterRegistry::new();
        writers
            .register(
                NodeId("writer".into()),
                SigningKey::from_bytes(&[7; 32]).verifying_key(),
                ["ns".to_owned()].into(),
            )
            .unwrap();
        let admission = Arc::new(AuthenticatedOperationAdmission::new(
            Arc::new(writers),
            "ns".into(),
        ));
        let edges: Vec<OperationRouteEdge> =
            serde_json::from_str(r#"[{"source":"b","receiver":"c","key_prefixes":[""]}]"#).unwrap();
        let routes = OperationRouteManifest::from_json(
            r#"[{"source":"b","destination":"c","key_prefix":"","next_hop":"c","rank":1}]"#,
            &edges,
        )
        .unwrap();
        let required =
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["c"]}]"#)
                .unwrap();
        let neighbors = OperationNeighborManifest::from_json(
            r#"[{"node_id":"c","addr":"127.0.0.1:8012","key_prefixes":[""]}]"#,
            &node,
        )
        .unwrap();
        let owned = OperationOwnedPrefixes::from_json("[]").unwrap();
        let route_digest = policy_route_digest(&routes, &required, &neighbors);
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(dir.path().join("inbox"), admission.clone()).unwrap(),
        ));
        let outbox = Arc::new(Mutex::new(
            OperationPolicyOutbox::open(
                dir.path().join("outbox"),
                node.clone(),
                route_digest,
                [8; 32],
            )
            .unwrap(),
        ));
        let relay = PolicyWitnessRelay::new(
            Arc::clone(&inbox),
            Arc::clone(&outbox),
            neighbors.clone(),
            owned.clone(),
            routes.clone(),
            required.clone(),
        )
        .await
        .unwrap();
        let transfer = OperationTransfer {
            version: 2,
            sender: NodeId("a".into()),
            transfer_id: 0,
            receiver: node.clone(),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet: body.clone(),
        };
        {
            let mut held = inbox.lock().await;
            held.receive_transfer_with_role(transfer, &node, false)
                .unwrap();
            // The policy worker has not scanned the ordinary custody packet.
            assert!(outbox.lock().await.pending().is_empty());
            assert_eq!(held.policy_witness_packet("ns", "key"), Some(body.clone()));
            held.save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: NodeId("a".into()),
                    receiver: node.clone(),
                    closed_before: 1,
                },
                &node,
            )
            .unwrap();
            assert!(held.get_packet(&subject).is_none());
            assert_eq!(held.policy_witness_packet("ns", "key"), Some(body.clone()));
        }
        drop(relay);
        drop(inbox);
        drop(outbox);
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(dir.path().join("inbox"), admission).unwrap(),
        ));
        let outbox = Arc::new(Mutex::new(
            OperationPolicyOutbox::open(dir.path().join("outbox"), node, route_digest, [8; 32])
                .unwrap(),
        ));
        let _recovered = PolicyWitnessRelay::new(
            inbox,
            Arc::clone(&outbox),
            neighbors,
            owned,
            routes,
            required,
        )
        .await
        .unwrap();
        let pending = outbox.lock().await.pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].packet, Some(body));
        assert_eq!(pending[0].destination, NodeId("c".into()));
    }

    #[test]
    fn orphan_relay_packet_before_transfer_catalog_is_removed_on_reopen() {
        for interrupted_cleanup in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let body = packet(b"unacknowledged custody");
            let subject = body.signed.bound.operation.subject.clone();
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            // This is the durable prefix of receive_transfer before its
            // transfer catalog is committed, so no custody ACK exists.
            inbox.receive_with_role(body, false).unwrap();
            assert!(inbox.get_packet(&subject).is_some());
            if interrupted_cleanup {
                std::fs::remove_file(packet_path(&inbox.packet_dir, &subject)).unwrap();
            }
            drop(inbox);
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            assert!(inbox.get_packet(&subject).is_none());
            assert!(!inbox.custody_receipts.contains_key(&subject));
            assert!(!packet_path(&inbox.custody_dir, &subject).exists());
            assert!(!packet_path(&inbox.custody_receipt_dir, &subject).exists());
            assert!(inbox.get(&subject).is_none());
            // The orphaned Create no longer blocks a different valid Create
            // for the same key when another sender starts a new transfer.
            let replacement = packet_for_key("key", 2);
            let transfer = OperationTransfer {
                version: 2,
                sender: NodeId("other".into()),
                transfer_id: 0,
                receiver: NodeId("b".into()),
                destination: Some(NodeId("c".into())),
                parent: None,
                depth: 0,
                packet: replacement.clone(),
            };
            inbox
                .receive_transfer_with_role(transfer, &NodeId("b".into()), false)
                .unwrap();
            assert_eq!(
                inbox.get_packet(&replacement.signed.bound.operation.subject),
                Some(replacement)
            );
        }
    }

    #[test]
    fn compact_relay_custody_keeps_create_witness_after_payload_reclaim() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("b".into());
        let key = SigningKey::from_bytes(&[7; 32]);
        let create_body = packet(b"create value").signed.bound;
        let proof = SignedCreation::sign(&create_body, &key).unwrap();
        let create = OperationPacket {
            signed: SignedBoundOperation::sign(create_body, &key, proof).unwrap(),
        };
        let add = later_packet(&create, b"add value");
        let add_subject = add.signed.bound.operation.subject.clone();
        let create_subject = create.signed.bound.operation.subject.clone();
        let transfer = |sender: &str, packet: OperationPacket| OperationTransfer {
            version: 2,
            sender: NodeId(sender.into()),
            transfer_id: 0,
            receiver: receiver.clone(),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet,
        };
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox
            .receive_transfer_with_role(transfer("a", add), &receiver, false)
            .unwrap();
        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: NodeId("a".into()),
                    receiver: receiver.clone(),
                    closed_before: 1,
                },
                &receiver,
            )
            .unwrap();
        assert!(!inbox.creation_policy_known(&add_subject));
        assert!(inbox.get_packet(&add_subject).is_some());
        inbox
            .receive_transfer_with_role(transfer("d", create), &receiver, false)
            .unwrap();
        assert!(inbox.creation_policy_known(&add_subject));
        assert_eq!(inbox.reclaim_retired_payloads().unwrap(), 1);
        assert!(inbox.get_packet(&add_subject).is_none());
        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: NodeId("d".into()),
                    receiver: receiver.clone(),
                    closed_before: 1,
                },
                &receiver,
            )
            .unwrap();
        assert!(inbox.get_packet(&create_subject).is_none());
        assert!(inbox.creation_policy_known(&add_subject));
        drop(inbox);
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(inbox.get(&add_subject).is_none());
        assert!(inbox.get(&create_subject).is_none());
        assert!(inbox.creation_policy_known(&add_subject));
    }

    #[tokio::test]
    async fn routed_startup_recovers_root_and_received_child_after_crash_window() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let mounted_policy = routed_policy_for_test(&config);
        let body = packet(b"recover");
        let a_inbox_dir = dir.path().join("inbox-a");
        {
            let mut inbox =
                OperationInbox::open(&a_inbox_dir, Arc::new(PermitTestOperations)).unwrap();
            inbox
                .ensure_mounted_authority_policy(&mounted_policy)
                .unwrap();
            inbox
                .ensure_owned_manifest(
                    OperationOwnedPrefixes::from_json("[\"\"]")
                        .unwrap()
                        .digest(),
                )
                .unwrap();
            inbox
                .ensure_authority_manifest(
                    routed_authority_manifest_digest(
                        &NodeId("a".into()),
                        &[],
                        Some(&mounted_policy),
                    )
                    .unwrap(),
                )
                .unwrap();
            inbox.receive_with_role(body.clone(), true).unwrap();
            inbox.record_root(&body).unwrap();
        }
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let root = a.outbox.lock().await.pending().next().unwrap().clone();
        assert_eq!(root.destination, Some(NodeId("c".into())));
        let b_inbox_dir = dir.path().join("inbox-b");
        {
            let mut inbox =
                OperationInbox::open(&b_inbox_dir, Arc::new(PermitTestOperations)).unwrap();
            inbox
                .ensure_mounted_authority_policy(&mounted_policy)
                .unwrap();
            inbox
                .ensure_owned_manifest(OperationOwnedPrefixes::from_json("[]").unwrap().digest())
                .unwrap();
            inbox
                .ensure_authority_manifest(
                    routed_authority_manifest_digest(
                        &NodeId("b".into()),
                        &[],
                        Some(&mounted_policy),
                    )
                    .unwrap(),
                )
                .unwrap();
            inbox
                .receive_transfer_with_role(
                    entry_transfer_for_test(&root),
                    &NodeId("b".into()),
                    false,
                )
                .unwrap();
        }
        let b = routed_relay(
            dir.path(),
            "b",
            r#"[{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let subject = body.signed.bound.operation.subject.clone();
        assert!(b.inbox.lock().await.get(&subject).is_none());
        assert!(b.inbox.lock().await.custody_receipts.contains_key(&subject));
        assert!(!b.inbox.lock().await.ledger.checkpoint_pending());
        let child = b.outbox.lock().await.pending().next().unwrap().clone();
        assert_eq!(
            child.parent,
            Some(TransferId {
                sender: NodeId("a".into()),
                sequence: root.id
            })
        );
        assert_eq!(child.depth, 1);
        assert_eq!(child.receiver, NodeId("c".into()));
    }

    #[tokio::test]
    async fn routed_application_body_without_local_role_is_rejected_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let body = packet(b"unowned application body");
        let node = NodeId("b".into());
        let config = routed_config(&["c"]);
        let namespace = routed_policy_for_test(&config);
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox
            .ensure_owned_manifest(OperationOwnedPrefixes::from_json("[]").unwrap().digest())
            .unwrap();
        inbox
            .ensure_authority_manifest(
                routed_authority_manifest_digest(&node, &[], Some(&namespace)).unwrap(),
            )
            .unwrap();
        inbox
            .receive_transfer_with_role(
                OperationTransfer {
                    version: 2,
                    sender: NodeId("a".into()),
                    transfer_id: 1,
                    receiver: node.clone(),
                    destination: Some(NodeId("c".into())),
                    parent: None,
                    depth: 0,
                    packet: body,
                },
                &node,
                true,
            )
            .unwrap();
        drop(inbox);

        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(
            inbox
                .verify_application_bodies(&OperationOwnedPrefixes::from_json("[]").unwrap())
                .is_err()
        );
        assert!(
            inbox
                .verify_application_bodies(&OperationOwnedPrefixes::from_json("[\"\"]").unwrap())
                .is_ok()
        );
        let result = OperationRelay::new_routed_with_authorities_and_policy(
            Arc::new(Mutex::new(inbox)),
            Arc::new(Mutex::new(
                OperationOutbox::open_routed_for_node(dir.path().join("outbox"), node.clone())
                    .unwrap(),
            )),
            OperationNeighborManifest::from_json(
                r#"[{"node_id":"a","addr":"127.0.0.1:8000","key_prefixes":[""]},{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#,
                &node,
            )
            .unwrap(),
            OperationOwnedPrefixes::from_json("[]").unwrap(),
            config.0,
            config.1,
            Vec::new(),
            namespace.clone(),
            Arc::new(std::sync::RwLock::new(namespace)),
        )
        .await;
        let error = result.err().unwrap().to_string();
        assert!(
            error.contains("application body lacks a local role"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn certified_completed_origin_reclaims_packet_and_exact_retry_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let neighbors = r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#;
        let relay = routed_relay(dir.path(), "a", neighbors, &config).await;
        let old = packet(b"old origin");
        let winner = later_packet(&old, b"new origin");
        relay.submit_local(old.clone()).await.unwrap();
        relay.submit_local(winner.clone()).await.unwrap();
        let old_send = relay
            .outbox
            .lock()
            .await
            .pending()
            .find(|entry| entry.subject == old.signed.bound.operation.subject)
            .unwrap()
            .clone();
        let proofs = verified_packets(&[old.clone(), winner.clone()]);
        let candidate = relay.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || candidate
                .compact_certified_ledger_bodies(&proofs)
                .unwrap())
            .await
            .unwrap(),
            1
        );
        let subject = old.signed.bound.operation.subject.clone();
        assert!(relay.inbox.lock().await.get_packet(&subject).is_some());
        let transfer = entry_transfer_for_test(&old_send);
        {
            let mut outbox = relay.outbox.lock().await;
            outbox
                .record_completion(OperationCompletionReport {
                    version: 1,
                    transfer: transfer.clone(),
                })
                .unwrap();
            outbox
                .record_receipt(
                    old_send.id,
                    OperationTransferReceipt {
                        version: 2,
                        sender: transfer.sender,
                        transfer_id: transfer.transfer_id,
                        receiver: transfer.receiver,
                        commitment: old.commitment(),
                        already_present: false,
                    },
                )
                .unwrap();
            assert_eq!(outbox.compact_one().unwrap(), Some(old_send.id));
        }
        let candidate = relay.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || candidate.reclaim_completed_roots().unwrap())
                .await
                .unwrap(),
            1
        );
        {
            let inbox = relay.inbox.lock().await;
            assert!(inbox.get_packet(&subject).is_none());
            assert!(
                inbox.root_packets().unwrap().iter().all(|packet| packet
                    .signed
                    .bound
                    .operation
                    .subject
                    != subject)
            );
            assert!(packet_path(&inbox.root_tombstone_dir, &subject).exists());
        }
        drop(relay);
        let lost_outbox = Arc::new(Mutex::new(
            OperationOutbox::open_routed_for_node(
                dir.path().join("outbox-forgotten"),
                NodeId("a".into()),
            )
            .unwrap(),
        ));
        let reopened_inbox = Arc::new(Mutex::new(
            OperationInbox::open(dir.path().join("inbox-a"), Arc::new(PermitTestOperations))
                .unwrap(),
        ));
        assert!(
            OperationRelay::new_routed(
                reopened_inbox,
                lost_outbox,
                OperationNeighborManifest::from_json(neighbors, &NodeId("a".into())).unwrap(),
                OperationOwnedPrefixes::from_json("[\"\"]").unwrap(),
                config.0.clone(),
                config.1.clone(),
            )
            .await
            .is_err()
        );
        // A crash after the durable tombstone but before unlink may leave a
        // complete root file. Recovery must never replay it into custody.
        let root_dir = dir.path().join("inbox-a/roots");
        write_exact_file(
            &root_dir,
            &packet_path(&root_dir, &subject),
            &serde_json::to_vec(&old).unwrap(),
        )
        .unwrap();
        let relay = routed_relay(dir.path(), "a", neighbors, &config).await;
        assert!(relay.inbox.lock().await.get_packet(&subject).is_none());
        assert!(!packet_path(&root_dir, &subject).exists());
        let retry = relay.submit_local(old.clone()).await.unwrap();
        assert!(retry.already_present);
        assert!(relay.inbox.lock().await.get_packet(&subject).is_none());
        let delete = later_action(&old, 3, CandidateKvAction::Delete { key: "key".into() });
        relay.submit_local(delete).await.unwrap();
        let retry_after_delete = relay.submit_local(old.clone()).await.unwrap();
        assert!(retry_after_delete.already_present);
        assert!(relay.inbox.lock().await.get_packet(&subject).is_none());
        assert!(
            relay
                .submit_local(packet(b"conflicting old origin"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn local_authority_origin_reclaims_only_with_verified_issuance_and_reseeds_on_restart() {
        let dir = tempfile::tempdir().unwrap();
        let (routes, _) = routed_config(&["c"]);
        let required = OperationRequiredDestinations::from_json(
            r#"[{"key_prefix":"","destinations":["a","c"]}]"#,
        )
        .unwrap();
        let config = (routes, required);
        let relay = local_authority_origin_relay(dir.path(), &config, None)
            .await
            .unwrap();
        let old = packet(b"authority origin to reclaim");
        let winner = later_packet(&old, b"new authority winner");
        relay.submit_local(old.clone()).await.unwrap();
        relay.submit_local(winner.clone()).await.unwrap();
        let subject = old.signed.bound.operation.subject.clone();
        let bound_commitment = old.signed.bound.commitment();
        let send = relay
            .outbox
            .lock()
            .await
            .pending()
            .find(|entry| entry.subject == subject)
            .unwrap()
            .clone();
        let proofs = verified_packets(&[old.clone(), winner]);
        let candidate = relay.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || candidate
                .compact_certified_ledger_bodies(&proofs)
                .unwrap())
            .await
            .unwrap(),
            1
        );
        let transfer = entry_transfer_for_test(&send);
        {
            let mut outbox = relay.outbox.lock().await;
            outbox
                .record_completion(OperationCompletionReport {
                    version: 1,
                    transfer: transfer.clone(),
                })
                .unwrap();
            outbox
                .record_receipt(
                    send.id,
                    OperationTransferReceipt {
                        version: 2,
                        sender: transfer.sender,
                        transfer_id: transfer.transfer_id,
                        receiver: transfer.receiver,
                        commitment: old.commitment(),
                        already_present: false,
                    },
                )
                .unwrap();
            assert_eq!(outbox.compact_one().unwrap(), Some(send.id));
        }
        let candidate = relay.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || candidate.reclaim_completed_roots().unwrap())
                .await
                .unwrap(),
            0
        );
        assert!(relay.inbox.lock().await.get_packet(&subject).is_some());
        // In production only the Authority worker can provide this witness,
        // after verifying its fsynced root log against its admitted choice.
        relay
            .inbox
            .lock()
            .await
            .note_verified_local_issuance(subject.clone(), bound_commitment)
            .unwrap();
        let candidate = relay.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || candidate.reclaim_completed_roots().unwrap())
                .await
                .unwrap(),
            1
        );
        assert!(relay.inbox.lock().await.get_packet(&subject).is_none());
        drop(relay);

        // An inbox opened without the verified Authority worker fails closed.
        assert!(
            local_authority_origin_relay(dir.path(), &config, None)
                .await
                .is_err()
        );
        assert!(
            local_authority_origin_relay(dir.path(), &config, Some((subject.clone(), [0; 32])))
                .await
                .is_err()
        );
        let recovered = local_authority_origin_relay(
            dir.path(),
            &config,
            Some((subject.clone(), bound_commitment)),
        )
        .await
        .unwrap();
        assert!(recovered.inbox.lock().await.get_packet(&subject).is_none());
    }

    #[test]
    fn reclaimed_origin_guard_recognizes_local_authority_role() {
        use crate::control_plane::system_namespace::AuthorityDefinition;
        use crate::placement::PlacementPolicy;
        use crate::types::{KeyRange, PolicyVersion};

        let packet = packet(b"local signing work");
        let bound = &packet.signed.bound;
        let mut namespace = SystemNamespace::new();
        namespace
            .set_placement_policy(
                PlacementPolicy::new(PolicyVersion(1), KeyRange { prefix: "".into() }, 1)
                    .with_certified(true),
            )
            .unwrap();
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange { prefix: "".into() },
            authority_nodes: vec![NodeId("a".into())],
            auto_generated: false,
        });
        let (routes, required) = routed_config(&["c"]);
        let config = RoutedCustody {
            routes,
            required,
            authorities: vec![AuthorityTerminalRole {
                namespace,
                scope: OperationScope {
                    namespace_id: "ns".into(),
                    key_prefix: "".into(),
                },
            }],
            submit_authority_policy: None,
            live_authority_policy: None,
        };
        let origin = RootTombstone {
            version: 1,
            subject: bound.operation.subject.clone(),
            key: bound.operation.action.key().into(),
            mode: bound.declaration.mode,
            packet_commitment: packet.commitment(),
            bound_commitment: bound.commitment(),
        };
        assert!(config.may_need_local_signing(&NodeId("a".into()), &origin));
        assert!(!config.may_need_local_signing(&NodeId("b".into()), &origin));
    }

    #[test]
    fn authority_terminal_manifest_pins_restart_and_refuses_unpinned_custody() {
        let dir = tempfile::tempdir().unwrap();
        let (routes, required) = routed_config(&["c"]);
        let config = RoutedCustody {
            routes,
            required,
            authorities: Vec::new(),
            submit_authority_policy: None,
            live_authority_policy: None,
        };
        assert_ne!(
            config
                .authority_manifest_digest(&NodeId("a".into()))
                .unwrap(),
            config
                .authority_manifest_digest(&NodeId("b".into()))
                .unwrap()
        );
        let inbox_dir = dir.path().join("pinned");
        let inbox = OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap();
        inbox.ensure_authority_manifest([7; 32]).unwrap();
        drop(inbox);
        let inbox = OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap();
        inbox.ensure_authority_manifest([7; 32]).unwrap();
        assert!(inbox.ensure_authority_manifest([8; 32]).is_err());

        let legacy_dir = dir.path().join("unpinned");
        let mut legacy = OperationInbox::open(&legacy_dir, Arc::new(PermitTestOperations)).unwrap();
        legacy.receive(packet(b"prior custody")).unwrap();
        assert!(legacy.ensure_authority_manifest([7; 32]).is_err());
        drop(legacy);
        let legacy = OperationInbox::open(&legacy_dir, Arc::new(PermitTestOperations)).unwrap();
        assert!(legacy.ensure_authority_manifest([7; 32]).is_err());
    }

    #[tokio::test]
    async fn route_mismatch_does_not_pin_authority_on_legacy_empty_inbox() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let mount = |addr: &str, owned: &str| {
            let config = config.clone();
            let root = dir.path().to_path_buf();
            let addr = addr.to_owned();
            let owned = owned.to_owned();
            async move {
                let node = NodeId("a".into());
                OperationRelay::new_routed(
                    Arc::new(Mutex::new(
                        OperationInbox::open(root.join("inbox"), Arc::new(PermitTestOperations))
                            .unwrap(),
                    )),
                    Arc::new(Mutex::new(
                        OperationOutbox::open_routed_for_node(root.join("outbox"), node.clone())
                            .unwrap(),
                    )),
                    OperationNeighborManifest::from_json(
                        &format!(r#"[{{"node_id":"b","addr":"{addr}","key_prefixes":[""]}}]"#),
                        &node,
                    )
                    .unwrap(),
                    OperationOwnedPrefixes::from_json(&owned).unwrap(),
                    config.0,
                    config.1,
                )
                .await
            }
        };
        drop(mount("127.0.0.1:8001", "[\"\"]").await.unwrap());
        let pin = dir.path().join("inbox/.authority-terminal-manifest");
        let owned_pin = dir.path().join("inbox/.owned-prefixes-manifest");
        std::fs::remove_file(&pin).unwrap(); // Simulate an empty inbox from before the pin.
        std::fs::remove_file(&owned_pin).unwrap();
        assert!(mount("127.0.0.1:8002", "[]").await.is_err());
        assert!(!pin.exists());
        assert!(!owned_pin.exists());
        assert!(mount("127.0.0.1:8001", "[\"\"]").await.is_ok());

        let expected_authority_pin = std::fs::read(&pin).unwrap();
        let route_pin = dir.path().join("outbox/.routing");
        std::fs::remove_file(&route_pin).unwrap(); // Simulate an older unpinned outbox.
        std::fs::write(&pin, [7; 32]).unwrap();
        assert!(mount("127.0.0.1:8001", "[\"\"]").await.is_err());
        assert!(!route_pin.exists());
        std::fs::write(&pin, expected_authority_pin).unwrap();
        assert!(mount("127.0.0.1:8001", "[\"\"]").await.is_ok());
    }

    #[tokio::test]
    async fn routed_root_cannot_restart_without_its_original_authority_role() {
        use crate::control_plane::system_namespace::AuthorityDefinition;
        use crate::placement::PlacementPolicy;
        use crate::types::{KeyRange, PolicyVersion};

        let dir = tempfile::tempdir().unwrap();
        let (routes, _) = routed_config(&["c"]);
        let config = (
            routes,
            OperationRequiredDestinations::from_json(
                r#"[{"key_prefix":"","destinations":["a","c"]}]"#,
            )
            .unwrap(),
        );
        let mut namespace = SystemNamespace::new();
        namespace
            .set_placement_policy(
                PlacementPolicy::new(
                    PolicyVersion(1),
                    KeyRange {
                        prefix: "key".into(),
                    },
                    1,
                )
                .with_certified(true),
            )
            .unwrap();
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "key".into(),
            },
            authority_nodes: vec![NodeId("a".into())],
            auto_generated: false,
        });
        let submit_namespace = Arc::new(std::sync::RwLock::new(namespace.clone()));
        let role = AuthorityTerminalRole {
            namespace,
            scope: OperationScope {
                namespace_id: "ns".into(),
                key_prefix: "key".into(),
            },
        };
        let mount = |role: Option<AuthorityTerminalRole>| {
            let config = config.clone();
            let submit_namespace = Arc::clone(&submit_namespace);
            async {
                let node = NodeId("a".into());
                let mounted_namespace = submit_namespace.read().unwrap().clone();
                OperationRelay::new_routed_with_authorities_and_policy(
                    Arc::new(Mutex::new(
                        OperationInbox::open(
                            dir.path().join("inbox"),
                            Arc::new(PermitTestOperations),
                        )
                        .unwrap(),
                    )),
                    Arc::new(Mutex::new(
                        OperationOutbox::open_routed_for_node(
                            dir.path().join("outbox"),
                            node.clone(),
                        )
                        .unwrap(),
                    )),
                    OperationNeighborManifest::from_json(
                        r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
                        &node,
                    )
                    .unwrap(),
                    OperationOwnedPrefixes::from_json("[\"\"]").unwrap(),
                    config.0,
                    config.1,
                    role.into_iter().collect(),
                    mounted_namespace,
                    submit_namespace,
                )
                .await
            }
        };
        let relay = mount(Some(role.clone())).await.unwrap();
        relay
            .submit_local(packet(b"root before role change"))
            .await
            .unwrap();
        drop(relay);
        assert!(mount(None).await.is_err());
        let reopened = mount(Some(role.clone())).await.unwrap();
        drop(reopened);
        submit_namespace
            .write()
            .unwrap()
            .set_authority_definition(AuthorityDefinition {
                key_range: KeyRange {
                    prefix: "key".into(),
                },
                authority_nodes: vec![NodeId("c".into())],
                auto_generated: false,
            });
        assert!(mount(Some(role)).await.is_err());
    }

    #[tokio::test]
    async fn routed_startup_replays_root_intent_before_packet_and_ledger() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let mounted_policy = routed_policy_for_test(&config);
        let body = packet(b"root intent crash");
        let subject = body.signed.bound.operation.subject.clone();
        let inbox_dir = dir.path().join("inbox-a");
        {
            let inbox = OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap();
            inbox
                .ensure_mounted_authority_policy(&mounted_policy)
                .unwrap();
            inbox
                .ensure_owned_manifest(
                    OperationOwnedPrefixes::from_json("[\"\"]")
                        .unwrap()
                        .digest(),
                )
                .unwrap();
            inbox
                .ensure_authority_manifest(
                    routed_authority_manifest_digest(
                        &NodeId("a".into()),
                        &[],
                        Some(&mounted_policy),
                    )
                    .unwrap(),
                )
                .unwrap();
            let path = packet_path(&inbox.root_dir, &subject);
            write_exact_file(&inbox.root_dir, &path, &serde_json::to_vec(&body).unwrap()).unwrap();
            assert!(inbox.get(&subject).is_none());
        }
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let inbox = a.inbox.lock().await;
        assert_eq!(inbox.get_packet(&subject), Some(body.clone()));
        assert_eq!(inbox.get(&subject), Some(body.signed.bound.clone()));
        drop(inbox);
        let send = a.outbox.lock().await.pending().next().unwrap().clone();
        assert_eq!(send.packet, Some(body));
        assert_eq!(send.destination, Some(NodeId("c".into())));
    }

    #[tokio::test]
    async fn routed_http_worker_delivers_through_transit_to_terminal() {
        use crate::network::operation_outbox::spawn_operation_retry_worker_with_auth;
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let a_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let b_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let c_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a_addr = a_listener.local_addr().unwrap().to_string();
        let b_addr = b_listener.local_addr().unwrap().to_string();
        let c_addr = c_listener.local_addr().unwrap().to_string();
        let a_neighbors = serde_json::json!([{"node_id":"b","addr":b_addr,"key_prefixes":[""]}]);
        let b_neighbors = serde_json::json!([{"node_id":"a","addr":a_addr,"key_prefixes":[""]},{"node_id":"c","addr":c_addr,"key_prefixes":[""]}]);
        let c_neighbors = serde_json::json!([{"node_id":"b","addr":b_addr,"key_prefixes":[""]}]);
        let a = routed_relay(dir.path(), "a", &a_neighbors.to_string(), &config).await;
        let b = routed_relay(dir.path(), "b", &b_neighbors.to_string(), &config).await;
        let c = routed_relay(dir.path(), "c", &c_neighbors.to_string(), &config).await;
        let a_link = test_link("a", 1, &[("b", &b_addr, 2)]);
        let b_link = test_link("b", 2, &[("a", &a_addr, 1), ("c", &c_addr, 3)]);
        let c_link = test_link("c", 3, &[("b", &b_addr, 2)]);
        let a_router = relay_router_authenticated(a.clone(), "secret".into(), a_link.clone());
        let b_router = relay_router_authenticated(b.clone(), "secret".into(), b_link.clone());
        let c_router = relay_router_authenticated(c.clone(), "secret".into(), c_link.clone());
        let a_server =
            tokio::spawn(async move { axum::serve(a_listener, a_router).await.unwrap() });
        let b_server =
            tokio::spawn(async move { axum::serve(b_listener, b_router).await.unwrap() });
        let c_server =
            tokio::spawn(async move { axum::serve(c_listener, c_router).await.unwrap() });
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let a_worker = spawn_operation_retry_worker_with_auth(
            a.outbox.clone(),
            "secret".into(),
            Some(a_link),
            shutdown_rx.clone(),
        );
        let b_worker = spawn_operation_retry_worker_with_auth(
            b.outbox.clone(),
            "secret".into(),
            Some(b_link.clone()),
            shutdown_rx,
        );
        let (completion_shutdown_tx, completion_shutdown_rx) = tokio::sync::watch::channel(false);
        let b_completion = spawn_operation_completion_worker_with_auth(
            b.clone(),
            "secret".into(),
            Some(b_link),
            completion_shutdown_rx.clone(),
        );
        let body = packet(b"http-routed");
        a.submit_local(body.clone()).await.unwrap();
        let delivered = tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                if c.inbox
                    .lock()
                    .await
                    .get_packet(&body.signed.bound.operation.subject)
                    .is_some()
                    && a.outbox.lock().await.pending().count() == 0
                    && b.outbox.lock().await.pending().count() == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if delivered.is_err() {
            panic!(
                "routed delivery timed out: A pending {}, B pending {}, B packets {}, C packets {}",
                a.outbox.lock().await.pending().count(),
                b.outbox.lock().await.pending().count(),
                b.inbox.lock().await.packets().len(),
                c.inbox.lock().await.packets().len()
            );
        }
        assert!(!b.owned.owns("key"));
        assert_eq!(
            c.inbox
                .lock()
                .await
                .get_packet(&body.signed.bound.operation.subject),
            Some(body)
        );
        assert_eq!(c.inbox.lock().await.terminal_transfers().unwrap().len(), 1);
        // Lose the process before it has sent any completion report. The
        // persisted terminal intake must drive a fresh process's retry.
        c_server.abort();
        let _ = c_server.await;
        drop(c);
        let c = routed_relay(dir.path(), "c", &c_neighbors.to_string(), &config).await;
        let c_completion = spawn_operation_completion_worker_with_auth(
            c.clone(),
            "secret".into(),
            Some(c_link),
            completion_shutdown_rx,
        );
        let complete = tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                if (a.outbox.lock().await.get(0).unwrap().completion.is_some()
                    || a.outbox.lock().await.is_closed(0))
                    && (b.outbox.lock().await.get(0).unwrap().completion.is_some()
                        || b.outbox.lock().await.is_closed(0))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(complete.is_ok(), "completion did not propagate C→B→A");
        assert!(
            a.outbox.lock().await.get(0).unwrap().receipt.is_some()
                || a.outbox.lock().await.is_closed(0)
        );
        assert!(
            b.outbox.lock().await.get(0).unwrap().receipt.is_some()
                || b.outbox.lock().await.is_closed(0)
        );
        let _ = shutdown_tx.send(true);
        let _ = completion_shutdown_tx.send(true);
        a_worker.await.unwrap();
        b_worker.await.unwrap();
        b_completion.await.unwrap();
        c_completion.await.unwrap();
        a_server.abort();
        b_server.abort();
        let _ = a_server.await;
        let _ = b_server.await;
        drop(a);
        drop(b);
        drop(c);
        let a = routed_relay(dir.path(), "a", &a_neighbors.to_string(), &config).await;
        let b = routed_relay(dir.path(), "b", &b_neighbors.to_string(), &config).await;
        let c = routed_relay(dir.path(), "c", &c_neighbors.to_string(), &config).await;
        assert!(
            a.outbox.lock().await.get(0).unwrap().completion.is_some()
                || a.outbox.lock().await.is_closed(0)
        );
        assert!(
            b.outbox.lock().await.get(0).unwrap().completion.is_some()
                || b.outbox.lock().await.is_closed(0)
        );
        let parent = b
            .inbox
            .lock()
            .await
            .received_transfers()
            .unwrap()
            .pop()
            .unwrap();
        assert!(b.inbox.lock().await.completion_report_acked(&parent));
        assert_eq!(c.inbox.lock().await.terminal_transfers().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn routed_chain_restarts_after_both_receiver_fences_without_reviving_work() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let a = routed_relay(
            dir.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        let b_neighbors = r#"[{"node_id":"a","addr":"127.0.0.1:8000","key_prefixes":[""]},{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#;
        let c_neighbors = r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#;
        let b = routed_relay(dir.path(), "b", b_neighbors, &config).await;
        let c = routed_relay(dir.path(), "c", c_neighbors, &config).await;
        a.submit_local(packet(b"chain-fence")).await.unwrap();
        let root = a.outbox.lock().await.get(0).unwrap().clone();
        let root_transfer = entry_transfer_for_test(&root);
        let root_ack = receive_routed(&b, root_transfer.clone()).await;
        let child = b.outbox.lock().await.get(0).unwrap().clone();
        let child_transfer = entry_transfer_for_test(&child);
        let child_ack = receive_routed(&c, child_transfer.clone()).await;
        let b_copy = b.clone();
        tokio::task::spawn_blocking(move || {
            b_copy
                .receive_completion(OperationCompletionReport {
                    version: 1,
                    transfer: child_transfer,
                })
                .unwrap()
        })
        .await
        .unwrap();
        {
            let mut outbox = b.outbox.lock().await;
            outbox.record_receipt(0, child_ack).unwrap();
            assert_eq!(outbox.compact_one().unwrap(), Some(0));
            assert_eq!(outbox.closed_before(), 1);
        }
        let a_copy = a.clone();
        tokio::task::spawn_blocking(move || {
            a_copy
                .receive_completion(OperationCompletionReport {
                    version: 1,
                    transfer: root_transfer,
                })
                .unwrap()
        })
        .await
        .unwrap();
        {
            let mut outbox = a.outbox.lock().await;
            outbox.record_receipt(0, root_ack).unwrap();
            assert_eq!(outbox.compact_one().unwrap(), Some(0));
            assert_eq!(outbox.closed_before(), 1);
        }
        let b_notice = a.outbox.lock().await.retirement_notices().pop().unwrap().1;
        let c_notice = b.outbox.lock().await.retirement_notices().pop().unwrap().1;
        let b_copy = b.clone();
        tokio::task::spawn_blocking(move || b_copy.receive_retirement(b_notice).unwrap())
            .await
            .unwrap();
        let c_copy = c.clone();
        tokio::task::spawn_blocking(move || c_copy.receive_retirement(c_notice).unwrap())
            .await
            .unwrap();
        assert!(
            b.inbox
                .lock()
                .await
                .received_transfers()
                .unwrap()
                .is_empty()
        );
        assert!(
            c.inbox
                .lock()
                .await
                .received_transfers()
                .unwrap()
                .is_empty()
        );
        let b_copy = b.clone();
        assert!(
            tokio::task::spawn_blocking(move || b_copy.pending_completion_reports().unwrap())
                .await
                .unwrap()
                .is_empty()
        );
        let c_copy = c.clone();
        assert!(
            tokio::task::spawn_blocking(move || c_copy.pending_completion_reports().unwrap())
                .await
                .unwrap()
                .is_empty()
        );
        drop(b);
        drop(c);
        let b = routed_relay(dir.path(), "b", b_neighbors, &config).await;
        let c = routed_relay(dir.path(), "c", c_neighbors, &config).await;
        assert!(
            b.inbox
                .lock()
                .await
                .received_transfers()
                .unwrap()
                .is_empty()
        );
        assert!(
            c.inbox
                .lock()
                .await
                .received_transfers()
                .unwrap()
                .is_empty()
        );
        let b_copy = b.clone();
        assert!(
            tokio::task::spawn_blocking(move || b_copy.pending_completion_reports().unwrap())
                .await
                .unwrap()
                .is_empty()
        );
        let c_copy = c.clone();
        assert!(
            tokio::task::spawn_blocking(move || c_copy.pending_completion_reports().unwrap())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(b.outbox.lock().await.is_closed(0));
        assert!(c.inbox.lock().await.get_packet(&child.subject).is_some());
    }

    fn entry_transfer_for_test(
        entry: &crate::network::operation_outbox::OutboxEntry,
    ) -> OperationTransfer {
        OperationTransfer {
            version: if entry.destination.is_some() { 2 } else { 1 },
            sender: entry.sender.clone(),
            transfer_id: entry.id,
            receiver: entry.receiver.clone(),
            destination: entry.destination.clone(),
            parent: entry.parent.clone(),
            depth: entry.depth,
            packet: entry.packet.clone().unwrap(),
        }
    }

    async fn receive_routed(
        relay: &OperationRelay,
        transfer: OperationTransfer,
    ) -> OperationTransferReceipt {
        let relay = relay.clone();
        tokio::task::spawn_blocking(move || relay.receive_transfer(transfer).unwrap())
            .await
            .unwrap()
    }

    #[test]
    fn receiver_retirement_fence_survives_restart_and_preserves_exact_ids() {
        let dir = tempfile::tempdir().unwrap();
        let sender = NodeId("a".into());
        let receiver = NodeId("b".into());
        let body = packet(b"fenced");
        let transfer = |id| OperationTransfer {
            version: 2,
            sender: sender.clone(),
            transfer_id: id,
            receiver: receiver.clone(),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet: body.clone(),
        };
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.receive_transfer(transfer(0), &receiver).unwrap();
        inbox.receive_transfer(transfer(1), &receiver).unwrap();
        let notice = OperationRetirementNotice {
            version: 1,
            sender: sender.clone(),
            receiver: receiver.clone(),
            closed_before: 1,
        };
        inbox.save_retirement_fence(&notice, &receiver).unwrap();
        assert_eq!(inbox.received_transfers().unwrap(), vec![transfer(1)]);
        assert!(inbox.receive_transfer(transfer(0), &receiver).is_err());
        let mut rebound = transfer(0);
        rebound.destination = Some(NodeId("d".into()));
        assert!(inbox.receive_transfer(rebound, &receiver).is_err());
        let stale = OperationRetirementNotice {
            closed_before: 0,
            ..notice.clone()
        };
        inbox.save_retirement_fence(&stale, &receiver).unwrap();
        drop(inbox);
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.verify_transfer_receiver(&receiver).unwrap();
        assert_eq!(inbox.received_transfers().unwrap(), vec![transfer(1)]);
        assert!(inbox.receive_transfer(transfer(0), &receiver).is_err());
        inbox.receive_transfer(transfer(2), &receiver).unwrap();
        assert_eq!(inbox.received_transfers().unwrap().len(), 2);
        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    closed_before: 2,
                    ..notice
                },
                &receiver,
            )
            .unwrap();
        drop(inbox);
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert_eq!(inbox.received_transfers().unwrap(), vec![transfer(2)]);
        assert_eq!(
            inbox.get_packet(&body.signed.bound.operation.subject),
            Some(body)
        );
    }

    #[test]
    fn catalog_only_crash_window_restores_unretired_transfer() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("b".into());
        let transfer = OperationTransfer {
            version: 2,
            sender: NodeId("a".into()),
            transfer_id: 3,
            receiver: receiver.clone(),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet: packet(b"crash"),
        };
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.receive_transfer(transfer.clone(), &receiver).unwrap();
        std::fs::remove_file(transfer_path(
            &inbox.transfer_dir,
            &transfer.sender,
            transfer.transfer_id,
        ))
        .unwrap();
        drop(inbox);
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert_eq!(inbox.received_transfers().unwrap(), vec![transfer]);
    }

    #[test]
    fn retired_receiver_catalog_and_packet_drop_value_but_preserve_exact_retry() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("b".into());
        let old = OperationTransfer {
            version: 2,
            sender: NodeId("a".into()),
            transfer_id: 0,
            receiver: receiver.clone(),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet: packet(b"large retired value"),
        };
        let subject = old.packet.signed.bound.operation.subject.clone();
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.receive_transfer(old.clone(), &receiver).unwrap();
        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: old.sender.clone(),
                    receiver: receiver.clone(),
                    closed_before: 1,
                },
                &receiver,
            )
            .unwrap();
        assert_eq!(inbox.reclaim_retired_payloads().unwrap(), 0); // Ledger body is live.
        inbox.ledger.compact_and_sync(&subject).unwrap();
        inbox.ledger.checkpoint_and_sync().unwrap();
        assert_eq!(inbox.reclaim_retired_payloads().unwrap(), 1);
        let catalog_path = transfer_path(&inbox.catalog_dir, &old.sender, 0);
        let catalog = std::fs::read(&catalog_path).unwrap();
        assert!(serde_json::from_slice::<OperationTransfer>(&catalog).is_err());
        assert!(catalog.len() < serde_json::to_vec(&old).unwrap().len());
        let descriptor: CompactInboundTransfer = serde_json::from_slice(&catalog).unwrap();
        assert_eq!(descriptor.packet_commitment, old.packet.commitment());
        assert_eq!(
            descriptor.bound_commitment,
            inbox.receipt(&subject).unwrap().commitment
        );
        assert!(!packet_path(&inbox.packet_dir, &subject).exists());
        assert_eq!(inbox.get_packet(&subject), None);
        assert!(inbox.creation_policy_known(&subject));
        assert!(inbox.receive(old.packet.clone()).unwrap().already_present);
        assert!(inbox.receive_transfer(old.clone(), &receiver).is_err());
        drop(inbox);

        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.verify_transfer_receiver(&receiver).unwrap();
        assert!(inbox.get_packet(&subject).is_none());
        assert!(inbox.creation_policy_known(&subject));
        assert!(inbox.receive_transfer(old.clone(), &receiver).is_err());
        assert!(inbox.receive(packet(b"different signed choice")).is_err());
        // An independent later transfer is a real new forwarding obligation.
        let later = OperationTransfer {
            sender: NodeId("other".into()),
            transfer_id: 0,
            ..old
        };
        inbox.receive_transfer(later.clone(), &receiver).unwrap();
        assert_eq!(inbox.get_packet(&subject), Some(later.packet.clone()));
        assert_eq!(inbox.reclaim_retired_payloads().unwrap(), 0);
        assert_eq!(inbox.received_transfers().unwrap(), vec![later.clone()]);
        drop(inbox);
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert_eq!(inbox.received_transfers().unwrap(), vec![later]);
    }

    #[test]
    fn owner_completes_new_transfer_after_ledger_and_old_custody_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("owner".into());
        let original = packet(b"replayed value");
        let subject = original.signed.bound.operation.subject.clone();
        let old = OperationTransfer {
            version: 2,
            sender: NodeId("first".into()),
            transfer_id: 0,
            receiver: receiver.clone(),
            destination: Some(receiver.clone()),
            parent: None,
            depth: 0,
            packet: original.clone(),
        };
        {
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            inbox
                .receive_transfer_with_role(old.clone(), &receiver, true)
                .unwrap();
            inbox.mark_terminal(&old).unwrap();
            inbox
                .save_retirement_fence(
                    &OperationRetirementNotice {
                        version: 1,
                        sender: old.sender.clone(),
                        receiver: receiver.clone(),
                        closed_before: 1,
                    },
                    &receiver,
                )
                .unwrap();
            inbox.ledger.compact_and_sync(&subject).unwrap();
            inbox.ledger.checkpoint_and_sync().unwrap();
            assert_eq!(inbox.reclaim_retired_payloads().unwrap(), 1);
            assert!(inbox.get(&subject).is_none());
            assert!(inbox.get_packet(&subject).is_none());
        }

        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        let later = OperationTransfer {
            sender: NodeId("second".into()),
            ..old
        };
        let mut conflicting = later.clone();
        conflicting.sender = NodeId("conflict".into());
        conflicting.packet = packet(b"different value for same dot");
        assert!(
            inbox
                .receive_transfer_with_role(conflicting, &receiver, true)
                .is_err()
        );
        assert!(inbox.get_packet(&subject).is_none());

        inbox
            .receive_transfer_with_role(later.clone(), &receiver, true)
            .unwrap();
        assert!(inbox.get(&subject).is_none());
        assert_eq!(inbox.get_packet(&subject), Some(original));
        inbox.mark_terminal(&later).unwrap();
        assert_eq!(inbox.terminal_transfers().unwrap(), vec![later.clone()]);
        drop(inbox);

        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(inbox.get(&subject).is_none());
        assert_eq!(inbox.terminal_transfers().unwrap(), vec![later]);
    }

    #[tokio::test]
    async fn routed_owner_reports_new_sender_after_collected_value_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("c".into());
        let edges = ["b", "d"]
            .into_iter()
            .map(|sender| OperationRouteEdge {
                source: NodeId(sender.into()),
                receiver: receiver.clone(),
                namespace_id: None,
                key_prefixes: vec!["".into()],
            })
            .collect::<Vec<_>>();
        let rows = ["b", "d"]
            .into_iter()
            .map(|sender| OperationRouteRow {
                source: NodeId(sender.into()),
                destination: receiver.clone(),
                namespace_id: None,
                key_prefix: "".into(),
                next_hop: receiver.clone(),
                rank: 1,
            })
            .collect();
        let config = (
            OperationRouteManifest::new(rows, &edges).unwrap(),
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["c"]}]"#)
                .unwrap(),
        );
        let neighbors = r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[""]},{"node_id":"d","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#;
        let body = packet(b"collected routed value");
        let subject = body.signed.bound.operation.subject.clone();
        let first = OperationTransfer {
            version: 2,
            sender: NodeId("b".into()),
            transfer_id: 0,
            receiver: receiver.clone(),
            destination: Some(receiver.clone()),
            parent: None,
            depth: 0,
            packet: body.clone(),
        };
        let owner = routed_relay(dir.path(), "c", neighbors, &config).await;
        assert!(!receive_routed(&owner, first.clone()).await.already_present);
        {
            let mut inbox = owner.inbox.lock().await;
            inbox
                .save_retirement_fence(
                    &OperationRetirementNotice {
                        version: 1,
                        sender: first.sender.clone(),
                        receiver: receiver.clone(),
                        closed_before: 1,
                    },
                    &receiver,
                )
                .unwrap();
            inbox.ledger.compact_and_sync(&subject).unwrap();
            inbox.ledger.checkpoint_and_sync().unwrap();
            assert_eq!(inbox.reclaim_retired_payloads().unwrap(), 1);
            assert!(inbox.get(&subject).is_none());
            assert!(inbox.get_packet(&subject).is_none());
        }
        drop(owner);

        let owner = routed_relay(dir.path(), "c", neighbors, &config).await;
        let later = OperationTransfer {
            sender: NodeId("d".into()),
            ..first
        };
        assert!(receive_routed(&owner, later.clone()).await.already_present);
        assert!(owner.inbox.lock().await.get(&subject).is_none());
        let ready = {
            let owner = owner.clone();
            tokio::task::spawn_blocking(move || owner.pending_completion_reports().unwrap())
                .await
                .unwrap()
        };
        assert_eq!(ready, vec![("127.0.0.1:8002".into(), later.clone())]);
        drop(owner);

        let owner = routed_relay(dir.path(), "c", neighbors, &config).await;
        assert!(owner.inbox.lock().await.get(&subject).is_none());
        let ready = {
            let owner = owner.clone();
            tokio::task::spawn_blocking(move || owner.pending_completion_reports().unwrap())
                .await
                .unwrap()
        };
        assert_eq!(ready, vec![("127.0.0.1:8002".into(), later)]);
    }

    #[test]
    fn compact_followup_custody_waits_for_create_before_packet_reclaim() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("b".into());
        let key = SigningKey::from_bytes(&[7; 32]);
        let body = packet(b"initial").signed.bound;
        let proof = SignedCreation::sign(&body, &key).unwrap();
        let create = OperationPacket {
            signed: SignedBoundOperation::sign(body, &key, proof).unwrap(),
        };
        let add = later_packet(&create, b"later");
        let subject = add.signed.bound.operation.subject.clone();
        let transfer = OperationTransfer {
            version: 2,
            sender: NodeId("a".into()),
            transfer_id: 0,
            receiver: receiver.clone(),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet: add,
        };
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.receive_transfer(transfer.clone(), &receiver).unwrap();
        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: transfer.sender.clone(),
                    receiver: receiver.clone(),
                    closed_before: 1,
                },
                &receiver,
            )
            .unwrap();
        // An early certificate cannot make an Add's policy known by itself.
        inbox.ledger.compact_and_sync(&subject).unwrap();
        assert!(!inbox.creation_policy_known(&subject));
        assert_eq!(inbox.reclaim_retired_payloads().unwrap(), 0);
        inbox.receive(create).unwrap();
        assert!(inbox.creation_policy_known(&subject));
        assert_eq!(inbox.reclaim_retired_payloads().unwrap(), 1);
        assert!(inbox.get_packet(&subject).is_none());
        assert!(inbox.creation_policy_known(&subject));
        drop(inbox);
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(inbox.creation_policy_known(&subject));
        let marker = packet_path(&inbox.known_policy_dir, &subject);
        drop(inbox);
        std::fs::remove_file(marker).unwrap();
        assert!(OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).is_err());
    }

    #[test]
    fn reclaimed_followup_binds_create_witness() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("b".into());
        let create = packet(b"original Create value");
        let add = later_packet(&create, b"later Add value");
        let subject = add.signed.bound.operation.subject.clone();
        let transfer = OperationTransfer {
            version: 2,
            sender: NodeId("a".into()),
            transfer_id: 0,
            receiver: receiver.clone(),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet: add,
        };
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.receive_transfer(transfer.clone(), &receiver).unwrap();
        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: transfer.sender.clone(),
                    receiver,
                    closed_before: 1,
                },
                &NodeId("b".into()),
            )
            .unwrap();
        inbox.ledger.compact_and_sync(&subject).unwrap();
        inbox.ledger.checkpoint_and_sync().unwrap();
        assert_eq!(inbox.reclaim_retired_payloads().unwrap(), 0);
        inbox.receive_policy_witness(create.clone()).unwrap();
        assert_eq!(inbox.reclaim_retired_payloads().unwrap(), 1);
        assert!(inbox.get_packet(&subject).is_none());
        drop(inbox);

        let forged = compact_create_packet(b"different Create value");
        let forged_commitment = creation_commitment(&forged);
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(matches!(
            inbox.receive_policy_witness(forged.clone()),
            Err(ReceiveError::Rejected(_))
        ));
        assert!(matches!(
            inbox.receive(forged),
            Err(ReceiveError::Rejected(_))
        ));
        inbox
            .receive_policy_witness(compact_create_packet(b"original Create value"))
            .unwrap();
        inbox.receive(create).unwrap();
        let marker_path = packet_path(&inbox.known_policy_dir, &subject);
        drop(inbox);
        let mut marker: KnownCreationPolicy =
            serde_json::from_slice(&std::fs::read(&marker_path).unwrap()).unwrap();
        marker.creation_commitment = forged_commitment;
        std::fs::write(marker_path, serde_json::to_vec(&marker).unwrap()).unwrap();
        assert!(OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).is_err());
    }

    #[test]
    fn compact_catalog_crash_window_is_recovered_before_packet_unlink() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("b".into());
        let transfer = OperationTransfer {
            version: 2,
            sender: NodeId("a".into()),
            transfer_id: 0,
            receiver: receiver.clone(),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet: packet(b"crash window value"),
        };
        let subject = transfer.packet.signed.bound.operation.subject.clone();
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.receive_transfer(transfer.clone(), &receiver).unwrap();
        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: transfer.sender.clone(),
                    receiver,
                    closed_before: 1,
                },
                &NodeId("b".into()),
            )
            .unwrap();
        inbox.ledger.compact_and_sync(&subject).unwrap();
        inbox.ledger.checkpoint_and_sync().unwrap();
        let path = transfer_path(&inbox.catalog_dir, &transfer.sender, 0);
        replace_file(
            &inbox.catalog_dir,
            &path,
            &serde_json::to_vec(&CompactInboundTransfer::from(&transfer)).unwrap(),
        )
        .unwrap();
        assert!(packet_path(&inbox.packet_dir, &subject).exists());
        drop(inbox);
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(!packet_path(&inbox.packet_dir, &subject).exists());
        assert!(inbox.get_packet(&subject).is_none());
        assert!(packet_path(&inbox.known_policy_dir, &subject).exists());
        assert!(inbox.creation_policy_known(&subject));
    }

    #[test]
    fn compacted_delete_exact_retry_stays_open_to_its_original_id() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("b".into());
        let creation = packet(b"before delete");
        let deletion = later_action(
            &creation,
            2,
            CandidateKvAction::Delete { key: "key".into() },
        );
        let transfer = OperationTransfer {
            version: 2,
            sender: NodeId("a".into()),
            transfer_id: 0,
            receiver: receiver.clone(),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet: deletion.clone(),
        };
        let subject = deletion.signed.bound.operation.subject.clone();
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.receive(creation).unwrap();
        inbox.receive_transfer(transfer, &receiver).unwrap();
        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: NodeId("a".into()),
                    receiver: receiver.clone(),
                    closed_before: 1,
                },
                &receiver,
            )
            .unwrap();
        inbox.ledger.compact_and_sync(&subject).unwrap();
        inbox.ledger.checkpoint_and_sync().unwrap();
        assert_eq!(inbox.reclaim_retired_payloads().unwrap(), 1);
        assert!(!inbox.key_deleted_for_new_local(&deletion));
        assert!(inbox.receive(deletion).unwrap().already_present);
    }

    #[test]
    fn root_and_signing_obligations_keep_signed_packet_after_retirement() {
        for signing in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let receiver = NodeId("b".into());
            let packet = packet(if signing { b"signing" } else { b"root" });
            let subject = packet.signed.bound.operation.subject.clone();
            let transfer = OperationTransfer {
                version: 2,
                sender: NodeId("a".into()),
                transfer_id: 0,
                receiver: receiver.clone(),
                destination: Some(receiver.clone()),
                parent: None,
                depth: 0,
                packet: packet.clone(),
            };
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            inbox.receive_transfer(transfer.clone(), &receiver).unwrap();
            if signing {
                inbox.mark_signing_intake(&transfer).unwrap();
            } else {
                inbox.record_root(&packet).unwrap();
            }
            inbox
                .save_retirement_fence(
                    &OperationRetirementNotice {
                        version: 1,
                        sender: NodeId("a".into()),
                        receiver: receiver.clone(),
                        closed_before: 1,
                    },
                    &receiver,
                )
                .unwrap();
            inbox.ledger.compact_and_sync(&subject).unwrap();
            inbox.ledger.checkpoint_and_sync().unwrap();
            assert_eq!(inbox.reclaim_retired_payloads().unwrap(), 0);
            assert_eq!(inbox.get_packet(&subject), Some(packet));
        }
    }

    #[test]
    fn compact_catalog_must_match_durable_ledger_receipt_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("b".into());
        let transfer = OperationTransfer {
            version: 2,
            sender: NodeId("a".into()),
            transfer_id: 0,
            receiver: receiver.clone(),
            destination: Some(NodeId("c".into())),
            parent: None,
            depth: 0,
            packet: packet(b"must bind receipt"),
        };
        let subject = transfer.packet.signed.bound.operation.subject.clone();
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.receive_transfer(transfer.clone(), &receiver).unwrap();
        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: transfer.sender.clone(),
                    receiver,
                    closed_before: 1,
                },
                &NodeId("b".into()),
            )
            .unwrap();
        inbox.ledger.compact_and_sync(&subject).unwrap();
        inbox.ledger.checkpoint_and_sync().unwrap();
        assert_eq!(inbox.reclaim_retired_payloads().unwrap(), 1);
        let path = transfer_path(&inbox.catalog_dir, &transfer.sender, 0);
        let mut descriptor: CompactInboundTransfer =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        descriptor.bound_commitment[0] ^= 1;
        replace_file(
            &inbox.catalog_dir,
            &path,
            &serde_json::to_vec(&descriptor).unwrap(),
        )
        .unwrap();
        drop(inbox);
        assert!(OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).is_err());
    }

    #[test]
    fn issued_signing_cleanup_recovers_after_marker_removal_before_packet_gc() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("authority".into());
        let transfer = OperationTransfer {
            version: 2,
            sender: NodeId("relay".into()),
            transfer_id: 0,
            receiver: receiver.clone(),
            destination: Some(receiver.clone()),
            parent: None,
            depth: 0,
            packet: packet(b"issued signing crash window"),
        };
        let subject = transfer.packet.signed.bound.operation.subject.clone();
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.receive_transfer(transfer.clone(), &receiver).unwrap();
        inbox.mark_signing_intake(&transfer).unwrap();
        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: transfer.sender.clone(),
                    receiver: receiver.clone(),
                    closed_before: 1,
                },
                &receiver,
            )
            .unwrap();
        assert!(inbox.ledger.compact_and_sync(&subject).unwrap());
        inbox.ledger.checkpoint_and_sync().unwrap();
        // The Authority worker checks its durable local root before this
        // call. Stop at the gap between marker deletion and packet GC.
        assert!(inbox.retire_issued_signing_intake(&transfer).unwrap());
        assert!(inbox.get_packet(&subject).is_some());
        drop(inbox);

        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(inbox.signing_transfers().unwrap().is_empty());
        assert!(inbox.get_packet(&subject).is_none());
        assert!(inbox.ledger.receipt(&subject).is_some());
        assert_eq!(
            inbox.compacted_packets.get(&subject).unwrap().0,
            transfer.packet.commitment()
        );
    }

    #[test]
    fn terminal_and_signing_intake_remain_after_receiver_detail_reclaim() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("authority".into());
        let transfer = OperationTransfer {
            version: 2,
            sender: NodeId("b".into()),
            transfer_id: 0,
            receiver: receiver.clone(),
            destination: Some(receiver.clone()),
            parent: Some(TransferId {
                sender: NodeId("a".into()),
                sequence: 0,
            }),
            depth: 1,
            packet: packet(b"signing"),
        };
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.receive_transfer(transfer.clone(), &receiver).unwrap();
        inbox.mark_terminal(&transfer).unwrap();
        inbox.mark_signing_intake(&transfer).unwrap();
        let terminal_path =
            transfer_path(&inbox.terminal_dir, &transfer.sender, transfer.transfer_id);
        let signing_path =
            transfer_path(&inbox.signing_dir, &transfer.sender, transfer.transfer_id);
        assert!(terminal_path.exists());
        assert!(signing_path.exists());
        inbox
            .record_completion_receipt(
                &transfer,
                OperationCompletionReceipt {
                    version: 1,
                    transfer_id: TransferId {
                        sender: transfer.sender.clone(),
                        sequence: 0,
                    },
                    commitment: transfer.packet.commitment(),
                },
            )
            .unwrap();
        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: transfer.sender.clone(),
                    receiver: receiver.clone(),
                    closed_before: 1,
                },
                &receiver,
            )
            .unwrap();
        assert!(inbox.received_transfers().unwrap().is_empty());
        assert!(inbox.terminal_transfers().unwrap().is_empty());
        assert!(inbox.completion_report_acked(&transfer));
        assert!(!terminal_path.exists());
        assert!(signing_path.exists());
        drop(inbox);
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(inbox.received_transfers().unwrap().is_empty());
        assert!(inbox.terminal_transfers().unwrap().is_empty());
        assert!(!terminal_path.exists());
        assert!(signing_path.exists());
        assert_eq!(
            inbox.signing_packets().unwrap(),
            vec![transfer.packet.clone()]
        );
        assert!(inbox.completion_report_acked(&transfer));
    }

    #[test]
    fn recovery_reclaims_terminal_detail_after_durable_fence_only() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("c".into());
        let sender = NodeId("b".into());
        let make_transfer = |id| OperationTransfer {
            version: 2,
            sender: sender.clone(),
            transfer_id: id,
            receiver: receiver.clone(),
            destination: Some(receiver.clone()),
            parent: Some(TransferId {
                sender: NodeId("a".into()),
                sequence: id,
            }),
            depth: 1,
            packet: packet(b"terminal"),
        };
        let old = make_transfer(0);
        let live = make_transfer(1);
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        for transfer in [&old, &live] {
            inbox.receive_transfer(transfer.clone(), &receiver).unwrap();
            inbox.mark_terminal(transfer).unwrap();
        }
        let old_path = transfer_path(&inbox.terminal_dir, &sender, 0);
        let live_path = transfer_path(&inbox.terminal_dir, &sender, 1);
        drop(inbox);
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(old_path.exists());
        assert!(live_path.exists());
        let notice = OperationRetirementNotice {
            version: 1,
            sender: sender.clone(),
            receiver,
            closed_before: 1,
        };
        replace_file(
            &inbox.fence_dir,
            &retirement_path(&inbox.fence_dir, &sender),
            &serde_json::to_vec(&notice).unwrap(),
        )
        .unwrap();
        drop(inbox); // Crash after fence fsync and before detail deletion.
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(!old_path.exists());
        assert!(live_path.exists());
        assert_eq!(inbox.terminal_transfers().unwrap(), vec![live]);
        assert_eq!(inbox.catalog_transfers().unwrap().len(), 2);
        assert!(inbox.receive_transfer(old, &NodeId("c".into())).is_err());
    }

    #[tokio::test]
    async fn routed_retirement_http_requires_token_and_signed_adjacent_sender() {
        let dir = tempfile::tempdir().unwrap();
        let config = routed_config(&["c"]);
        let b = routed_relay(
            dir.path(),
            "b",
            r#"[{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":[""]}]"#,
            &config,
        )
        .await;
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                relay_router(b.clone(), "secret".into())
            }))
            .is_err()
        );
        let notice = OperationRetirementNotice {
            version: 1,
            sender: NodeId("c".into()),
            receiver: NodeId("b".into()),
            closed_before: 1,
        };
        let b_link = test_link("b", 2, &[("c", "127.0.0.1:8002", 3)]);
        let c_link = test_link("c", 3, &[("b", "127.0.0.1:8001", 2)]);
        let request = |notice: &OperationRetirementNotice, token: Option<&str>, signed: bool| {
            let mut builder = Request::builder()
                .method("POST")
                .uri(OPERATION_RETIREMENT_PATH)
                .header("content-type", "application/json");
            if let Some(token) = token {
                builder = builder.header("authorization", format!("Bearer {token}"));
            }
            let body = serde_json::to_vec(notice).unwrap();
            if signed {
                let (_, signature) = c_link.sign_request(
                    &NodeId("b".into()),
                    "POST",
                    OPERATION_RETIREMENT_PATH,
                    &body,
                );
                builder = builder
                    .header(LINK_SENDER, "c")
                    .header(LINK_RECEIVER, "b")
                    .header(LINK_SIGNATURE, signature);
            }
            builder.body(Body::from(body)).unwrap()
        };
        let router = relay_router_authenticated(b.clone(), "secret".into(), b_link);
        assert_eq!(
            router
                .clone()
                .oneshot(request(&notice, None, false))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let mut unknown = notice.clone();
        unknown.sender = NodeId("outsider".into());
        assert_eq!(
            router
                .clone()
                .oneshot(request(&unknown, Some("secret"), true))
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        let response = router
            .oneshot(request(&notice, Some("secret"), true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<OperationRetirementNotice>(&body).unwrap(),
            notice
        );
    }

    #[test]
    fn durable_receipt_survives_reopen_and_rejects_same_id_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let original = packet(b"first");
        let commitment = original.commitment();
        {
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            assert_eq!(
                inbox.receive(original.clone()).unwrap(),
                OperationReceipt {
                    commitment,
                    already_present: false
                }
            );
        }
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert_eq!(
            inbox.get(&original.signed.bound.operation.subject),
            Some(original.signed.bound.clone())
        );
        assert_eq!(
            inbox.get_packet(&original.signed.bound.operation.subject),
            Some(original.clone())
        );
        assert_eq!(
            inbox.ordinary_read("ns", "key"),
            OperationRead::Value {
                value_bytes: b"first".to_vec(),
                timestamp: HlcTimestamp {
                    physical: 1,
                    logical: 0,
                    node_id: "writer".into(),
                },
            }
        );
        let wal_bytes = inbox_ledger_wal_bytes(dir.path());
        assert_eq!(
            inbox.receive(original).unwrap(),
            OperationReceipt {
                commitment,
                already_present: true
            }
        );
        assert_eq!(inbox_ledger_wal_bytes(dir.path()), wal_bytes);
        assert!(matches!(
            inbox.receive(packet(b"second")),
            Err(ReceiveError::Persist(PersistOperationError::Operation(
                crate::operation::OperationError::ConflictingContent(_)
            )))
        ));
    }

    #[test]
    fn arbitrary_value_bytes_survive_signed_transport_and_restart() {
        for value in [Vec::new(), vec![0, 0xff, 0x80, 0, 42]] {
            let dir = tempfile::tempdir().unwrap();
            let original = packet(&value);
            let subject = original.signed.bound.operation.subject.clone();
            let commitment = original.commitment();
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            assert_eq!(
                inbox.receive(original.clone()).unwrap().commitment,
                commitment
            );
            drop(inbox);

            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            assert_eq!(inbox.get_packet(&subject), Some(original.clone()));
            assert_eq!(
                inbox.ordinary_read("ns", "key"),
                OperationRead::Value {
                    value_bytes: value,
                    timestamp: HlcTimestamp {
                        physical: 1,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                }
            );
            assert!(inbox.receive(original).unwrap().already_present);
        }
    }

    #[test]
    fn compacted_inbox_receipt_survives_restart_and_exact_retry() {
        let dir = tempfile::tempdir().unwrap();
        let original = packet(b"old value");
        let subject = original.signed.bound.operation.subject.clone();
        {
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            inbox.receive(original.clone()).unwrap();
            inbox.ledger.compact_and_sync(&subject).unwrap();
            inbox.ledger.checkpoint_and_sync().unwrap();
        }
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(inbox.get(&subject).is_none());
        assert_eq!(
            inbox.receipt(&subject).unwrap().commitment,
            original.signed.bound.commitment()
        );
        assert_eq!(inbox.get_packet(&subject), Some(original.clone()));
        let wal_bytes = inbox_ledger_wal_bytes(dir.path());
        assert!(inbox.receive(original).unwrap().already_present);
        assert_eq!(inbox_ledger_wal_bytes(dir.path()), wal_bytes);
        assert!(inbox.get(&subject).is_none());
        assert!(matches!(
            inbox.receive(packet(b"changed")),
            Err(ReceiveError::Persist(PersistOperationError::Operation(
                crate::operation::OperationError::ConflictingContent(_)
            )))
        ));
    }

    #[test]
    fn transfer_identity_is_durable_and_cannot_be_rebound() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("receiver".into());
        let transfer = OperationTransfer {
            version: 1,
            sender: NodeId("sender".into()),
            transfer_id: 7,
            receiver: receiver.clone(),
            destination: None,
            parent: None,
            depth: 0,
            packet: packet(b"first"),
        };
        {
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            let receipt = inbox.receive_transfer(transfer.clone(), &receiver).unwrap();
            assert_eq!(receipt.transfer_id, 7);
            assert_eq!(receipt.receiver, receiver);
        }
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(inbox.verify_transfer_receiver(&receiver).is_ok());
        assert!(
            inbox
                .verify_transfer_receiver(&NodeId("other".into()))
                .is_err()
        );
        let wal_bytes = inbox_ledger_wal_bytes(dir.path());
        assert!(
            inbox
                .receive_transfer(transfer.clone(), &receiver)
                .unwrap()
                .already_present
        );
        assert_eq!(inbox_ledger_wal_bytes(dir.path()), wal_bytes);
        let mut rebound = transfer.clone();
        let mut other = packet(b"different").signed.bound;
        other.operation.subject.event_dot.counter = 2;
        other.declaration.creation_id = other.operation.subject.clone();
        let CandidateKvAction::Create { key, .. } = &mut other.operation.action else {
            unreachable!()
        };
        *key = "other".into();
        other.declaration.key = "other".into();
        let other_subject = other.operation.subject.clone();
        let key = SigningKey::from_bytes(&[7; 32]);
        let creation = SignedCreation::sign(&other, &key).unwrap();
        rebound.packet = OperationPacket {
            signed: SignedBoundOperation::sign(other, &key, creation).unwrap(),
        };
        assert!(matches!(
            inbox.receive_transfer(rebound, &receiver),
            Err(ReceiveError::Rejected(_))
        ));
        assert!(
            inbox
                .get_packet(&transfer.packet.signed.bound.operation.subject)
                .is_some()
        );
        assert!(inbox.get_packet(&other_subject).is_none());
        let wrong_receiver = NodeId("other".into());
        assert!(matches!(
            inbox.receive_transfer(transfer, &wrong_receiver),
            Err(ReceiveError::Rejected(_))
        ));
    }

    #[test]
    fn failed_transfer_publication_recovers_in_place_after_storage_returns() {
        let dir = tempfile::tempdir().unwrap();
        let receiver = NodeId("receiver".into());
        let transfer = OperationTransfer {
            version: 1,
            sender: NodeId("sender".into()),
            transfer_id: 0,
            receiver: receiver.clone(),
            destination: None,
            parent: None,
            depth: 0,
            packet: packet(b"first"),
        };
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        let transfer_dir = inbox.transfer_dir.clone();
        std::fs::remove_dir(&transfer_dir).unwrap();
        std::fs::write(&transfer_dir, b"blocked").unwrap();

        assert!(inbox.receive_transfer(transfer.clone(), &receiver).is_err());
        assert!(inbox.poisoned);
        assert!(inbox.received_transfers().is_err());
        assert!(inbox.catalog_transfers().is_err());
        assert!(inbox.terminal_transfers().is_err());
        assert!(inbox.recover_if_poisoned().is_err());
        assert!(inbox.poisoned);

        std::fs::remove_file(&transfer_dir).unwrap();
        std::fs::create_dir(&transfer_dir).unwrap();
        inbox.recover_if_poisoned().unwrap();
        assert!(!inbox.poisoned);
        assert_eq!(inbox.received_transfers().unwrap(), vec![transfer]);
        assert!(OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).is_err());
    }

    #[test]
    fn admission_rejection_does_not_write_an_inbox_record() {
        let dir = tempfile::tempdir().unwrap();
        let original = packet(b"body");
        {
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(RejectTestOperations)).unwrap();
            assert!(matches!(
                inbox.receive(original.clone()),
                Err(ReceiveError::Rejected(_))
            ));
        }
        let mut inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(!inbox.receive(original).unwrap().already_present);
    }

    #[test]
    fn creation_policy_pin_rejects_legacy_state_changes_and_removal() {
        let dir = tempfile::tempdir().unwrap();
        let digest = [3; 32];
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.verify_creation_policy_manifest(digest).unwrap();
        assert!(!dir.path().join(".creation-policy-manifest").exists());
        inbox.ensure_creation_policy_manifest(digest).unwrap();
        inbox.verify_creation_policy_manifest(digest).unwrap();
        assert!(inbox.verify_creation_policy_manifest([4; 32]).is_err());
        inbox.ensure_creation_policy_manifest(digest).unwrap();
        assert!(inbox.ensure_creation_policy_manifest([4; 32]).is_err());
        assert!(inbox.ensure_no_creation_policy_manifest().is_err());
        drop(inbox);

        let legacy = tempfile::tempdir().unwrap();
        let mut inbox =
            OperationInbox::open(legacy.path(), Arc::new(PermitTestOperations)).unwrap();
        inbox.ensure_no_creation_policy_manifest().unwrap();
        inbox.receive(packet(b"legacy")).unwrap();
        assert!(inbox.verify_creation_policy_manifest(digest).is_err());
        assert!(!legacy.path().join(".creation-policy-manifest").exists());
        assert!(inbox.ensure_creation_policy_manifest(digest).is_err());
    }

    #[test]
    fn policy_only_witness_cannot_acquire_a_later_creation_policy_pin() {
        let dir = tempfile::tempdir().unwrap();
        let witness = compact_create_packet(b"initial value");
        {
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            inbox.receive_policy_witness(witness.clone()).unwrap();
            assert!(inbox.get(&witness.signed.bound.operation.subject).is_none());
        }
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert_eq!(inbox.policy_witness_packet("ns", "key"), Some(witness));
        assert!(inbox.verify_creation_policy_manifest([3; 32]).is_err());
        assert!(inbox.ensure_creation_policy_manifest([3; 32]).is_err());
        assert!(!dir.path().join(".creation-policy-manifest").exists());
    }

    #[test]
    fn local_root_rejects_competing_policy_witness_before_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let witness = packet_for_key("key", 1);
        let competing = packet_for_key("key", 2);
        {
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            inbox.receive_policy_witness(witness.clone()).unwrap();
            assert!(matches!(
                inbox.receive_local_root(competing.clone()),
                Err(ReceiveError::Rejected(_))
            ));
            assert!(
                !packet_path(&inbox.root_dir, &competing.signed.bound.operation.subject).exists()
            );
        }
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert_eq!(inbox.policy_witness_packet("ns", "key"), Some(witness));
        assert!(inbox.root_packets().unwrap().is_empty());
    }

    #[test]
    fn local_root_rejects_compact_witness_identity_before_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let witness = packet(b"original");
        let mut different_packet = witness.clone();
        // Permissive admission isolates storage ordering from signature
        // verification. The compact witness fixes one exact packet identity.
        different_packet.signed.operation_signature[0] ^= 1;
        {
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitCompactTestOperations)).unwrap();
            inbox.receive_policy_witness(witness.clone()).unwrap();
            assert!(inbox.compact_policy_witness("ns", "key").unwrap());
            assert!(matches!(
                inbox.receive_local_root(different_packet.clone()),
                Err(ReceiveError::Rejected(_))
            ));
            assert!(
                !packet_path(&inbox.root_dir, &witness.signed.bound.operation.subject).exists()
            );
        }
        let inbox =
            OperationInbox::open(dir.path(), Arc::new(PermitCompactTestOperations)).unwrap();
        assert!(inbox.root_packets().unwrap().is_empty());
    }

    #[test]
    fn recovery_discards_only_uncommitted_packet_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let original = packet(b"body");
        {
            let mut inbox =
                OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            inbox.receive(original.clone()).unwrap();
        }
        let packet_dir = dir.path().join("packets");
        std::fs::write(packet_dir.join(".packet-123-0.tmp"), b"partial").unwrap();
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert_eq!(
            inbox.get_packet(&original.signed.bound.operation.subject),
            Some(original)
        );
        assert!(!packet_dir.join(".packet-123-0.tmp").exists());
        std::fs::write(packet_dir.join("unexpected.json"), b"partial").unwrap();
        assert!(OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).is_err());
    }

    #[test]
    fn recovery_replays_signed_packet_before_ledger_append() {
        let dir = tempfile::tempdir().unwrap();
        let original = packet(b"durable-packet");
        let subject = original.signed.bound.operation.subject.clone();
        {
            let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
            write_packet_file(&inbox.packet_dir, &subject, &original).unwrap();
            // Simulate a crash before the ledger append, after packet fsync.
        }
        let inbox = OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert_eq!(inbox.get(&subject), Some(original.signed.bound.clone()));
        assert_eq!(inbox.get_packet(&subject), Some(original.clone()));
        drop(inbox);
        let mut reopened =
            OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap();
        assert!(reopened.receive(original).unwrap().already_present);
    }

    #[test]
    fn recovery_refuses_ledger_body_without_signed_packet() {
        let dir = tempfile::tempdir().unwrap();
        let original = packet(b"orphan");
        OperationLedger::open(dir.path())
            .unwrap()
            .record_and_sync(original.signed.bound)
            .unwrap();
        assert!(OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).is_err());
    }

    #[test]
    fn authenticated_inbox_rejects_forged_packet_before_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let key = SigningKey::from_bytes(&[7; 32]);
        let mut registry = WriterRegistry::new();
        registry
            .register(
                NodeId("writer".into()),
                key.verifying_key(),
                ["ns".to_owned()].into(),
            )
            .unwrap();
        let admission = Arc::new(AuthenticatedOperationAdmission::new(
            Arc::new(registry),
            "ns".into(),
        ));
        let mut forged = packet(b"body");
        forged.signed.operation_signature[0] ^= 1;
        {
            let mut inbox = OperationInbox::open(dir.path(), admission.clone()).unwrap();
            assert!(matches!(
                inbox.receive(forged),
                Err(ReceiveError::Rejected(_))
            ));
        }
        let mut inbox = OperationInbox::open(dir.path(), admission).unwrap();
        assert!(!inbox.receive(packet(b"body")).unwrap().already_present);
    }

    #[test]
    fn authenticated_inbox_accepts_only_configured_namespaces() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = WriterRegistry::new();
        registry
            .register(
                NodeId("writer".into()),
                SigningKey::from_bytes(&[7; 32]).verifying_key(),
                ["ns".to_owned(), "other-ns".to_owned(), "unused".to_owned()].into(),
            )
            .unwrap();
        let admission = Arc::new(AuthenticatedOperationAdmission::new_for_namespaces(
            Arc::new(registry),
            BTreeSet::from(["ns".into(), "other-ns".into()]),
        ));
        let mut inbox = OperationInbox::open(dir.path(), admission).unwrap();
        assert!(
            inbox
                .receive(packet_for_namespace_key("ns", "key/1", 1))
                .is_ok()
        );
        assert!(
            inbox
                .receive(packet_for_namespace_key("other-ns", "key/1", 1))
                .is_ok()
        );
        assert!(matches!(
            inbox.receive(packet_for_namespace_key("unused", "key/1", 1)),
            Err(ReceiveError::Rejected(_))
        ));
    }

    #[tokio::test]
    async fn scoped_owner_accepts_local_submit_only_in_its_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let relay = OperationRelay::new(
            Arc::new(Mutex::new(
                OperationInbox::open(dir.path().join("inbox"), Arc::new(PermitTestOperations))
                    .unwrap(),
            )),
            Arc::new(Mutex::new(
                OperationOutbox::open(dir.path().join("outbox")).unwrap(),
            )),
            OperationNeighborManifest::from_json("[]", &NodeId("self".into())).unwrap(),
            OperationOwnedPrefixes::from_json(r#"[{"namespace_id":"ns","key_prefix":"item/"}]"#)
                .unwrap(),
        )
        .await
        .unwrap();
        assert!(
            relay
                .submit_local(packet_for_namespace_key("ns", "item/1", 1))
                .await
                .is_ok()
        );
        assert!(matches!(
            relay
                .submit_local(packet_for_namespace_key("other-ns", "item/1", 1))
                .await,
            Err(StatusCode::FORBIDDEN)
        ));
    }

    #[test]
    fn authenticated_inbox_checks_signed_creation_mode_before_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let key = SigningKey::from_bytes(&[7; 32]);
        let mut registry = WriterRegistry::new();
        registry
            .register(
                NodeId("writer".into()),
                key.verifying_key(),
                ["ns".to_owned()].into(),
            )
            .unwrap();
        let policy = Arc::new(
            CreationPolicyManifest::from_json(
                r#"{"version":1,"ranges":[{"namespace_id":"ns","key_prefix":"","default_mode":"Disabled","override_writers":[]}]}"#,
            )
            .unwrap(),
        );
        let admission = Arc::new(AuthenticatedOperationAdmission::new_with_creation_policy(
            Arc::new(registry),
            "ns".into(),
            policy.clone(),
        ));
        let mut inbox = OperationInbox::open(dir.path(), admission).unwrap();
        inbox
            .ensure_creation_policy_manifest(policy.digest())
            .unwrap();
        assert!(matches!(
            inbox.receive(packet(b"body")),
            Err(ReceiveError::Rejected(_))
        ));
        assert!(inbox.packets.is_empty());
    }

    #[test]
    fn authenticated_inbox_rejects_different_creation_policy_digest_before_ack() {
        let dir = tempfile::tempdir().unwrap();
        let key = SigningKey::from_bytes(&[7; 32]);
        let mut registry = WriterRegistry::new();
        registry
            .register(
                NodeId("writer".into()),
                key.verifying_key(),
                ["ns".to_owned()].into(),
            )
            .unwrap();
        let local = Arc::new(
            CreationPolicyManifest::from_json(
                r#"{"version":1,"ranges":[{"namespace_id":"ns","key_prefix":"","default_mode":"Disabled","override_writers":[]}]}"#,
            )
            .unwrap(),
        );
        let remote = CreationPolicyManifest::from_json(
            r#"{"version":1,"ranges":[{"namespace_id":"ns","key_prefix":"","default_mode":"Disabled","override_writers":["other"]}]}"#,
        )
        .unwrap();
        let admission = Arc::new(AuthenticatedOperationAdmission::new_with_creation_policy(
            Arc::new(registry),
            "ns".into(),
            local.clone(),
        ));
        let mut body = packet(b"body").signed.bound;
        body.declaration.mode = CertificationMode::Disabled;
        let proof = SignedCreation::sign_with_policy_digest(&body, &key, remote.digest()).unwrap();
        let incoming = OperationPacket {
            signed: SignedBoundOperation::sign(body, &key, proof.clone()).unwrap(),
        };
        assert!(incoming.signed.verify(&admission.registry).is_ok());
        assert!(admission.compact_creation_trust(&proof).is_err());
        let mut inbox = OperationInbox::open(dir.path(), admission).unwrap();
        inbox
            .ensure_creation_policy_manifest(local.digest())
            .unwrap();
        assert!(matches!(
            inbox.receive(incoming),
            Err(ReceiveError::Rejected(_))
        ));
        assert!(inbox.packets.is_empty());
    }

    #[test]
    fn authenticated_inbox_rejects_oversized_certificate_before_root_ack() {
        use crate::authority::certificate::KeysetVersion;

        let dir = tempfile::tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[7; 32]);
        let mut registry = WriterRegistry::new();
        registry
            .register(
                NodeId("writer".into()),
                writer.verifying_key(),
                ["ns".to_owned()].into(),
            )
            .unwrap();
        let mut keyset = KeysetRegistry::new();
        keyset
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![(
                    NodeId("authority".into()),
                    SigningKey::from_bytes(&[8; 32]).verifying_key(),
                )],
            )
            .unwrap();
        let admission = Arc::new(
            AuthenticatedOperationAdmission::new(Arc::new(registry), "ns".into())
                .with_certificate_capacity_scopes(vec![(
                    OperationScope {
                        namespace_id: "ns".into(),
                        key_prefix: "".into(),
                    },
                    keyset,
                )]),
        );
        let mut inbox = OperationInbox::open(dir.path(), admission).unwrap();
        let oversized = packet(&vec![0; 4_300_000]);
        assert!(matches!(
            inbox.receive_local_root(oversized),
            Err(ReceiveError::Rejected(_))
        ));
        assert!(inbox.packets.is_empty());
    }

    #[test]
    fn compact_followup_policy_waits_for_authenticated_create_or_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let key = SigningKey::from_bytes(&[7; 32]);
        let mut registry = WriterRegistry::new();
        registry
            .register(
                NodeId("writer".into()),
                key.verifying_key(),
                ["ns".to_owned()].into(),
            )
            .unwrap();
        let admission = Arc::new(AuthenticatedOperationAdmission::new(
            Arc::new(registry),
            "ns".into(),
        ));
        let mut inbox =
            OperationInbox::open(dir.path().join("compact"), admission.clone()).unwrap();
        let mut create_body = packet(b"initial").signed.bound;
        create_body.declaration.mode = CertificationMode::Disabled;
        let compact_proof = SignedCreation::sign(&create_body, &key).unwrap();
        let create = OperationPacket {
            signed: SignedBoundOperation::sign(create_body, &key, compact_proof).unwrap(),
        };
        let add = later_packet(&create, b"later");
        let add_id = add.signed.bound.operation.subject.clone();
        let create_id = create.signed.bound.operation.subject.clone();
        inbox.receive(add).unwrap();
        assert!(!inbox.creation_policy_known(&add_id));
        assert_eq!(
            inbox.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::PolicyUnknown
        );
        assert!(matches!(
            inbox.ordinary_read("ns", "key"),
            OperationRead::Value { .. }
        ));

        drop(inbox);
        let mut inbox =
            OperationInbox::open(dir.path().join("compact"), admission.clone()).unwrap();
        assert!(!inbox.creation_policy_known(&add_id));
        assert_eq!(
            inbox.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::PolicyUnknown
        );

        inbox.receive(create).unwrap();
        assert!(inbox.creation_policy_known(&add_id));
        assert_eq!(
            inbox.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::NotTargeted
        );
        inbox.ledger.compact_and_sync(&create_id).unwrap();
        assert!(inbox.creation_policy_known(&add_id));
        assert_eq!(
            inbox.certified_read(&[], "ns", "key").result,
            CertifiedReadResult::NotTargeted
        );
    }

    #[tokio::test]
    async fn http_receipt_requires_auth_and_rejects_conflicting_payload() {
        let dir = tempfile::tempdir().unwrap();
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(dir.path(), Arc::new(PermitTestOperations)).unwrap(),
        ));
        let app = receiver_router(inbox, "secret".into());
        let request = |packet: OperationPacket, token: Option<&str>| {
            let mut builder = Request::builder()
                .method("POST")
                .uri(OPERATION_RECEIVE_PATH)
                .header("content-type", "application/json");
            if let Some(token) = token {
                builder = builder.header("authorization", format!("Bearer {token}"));
            }
            builder
                .body(Body::from(serde_json::to_vec(&packet).unwrap()))
                .unwrap()
        };
        assert_eq!(
            app.clone()
                .oneshot(request(packet(b"one"), None))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            app.clone()
                .oneshot(request(packet(b"one"), Some("secret")))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            app.clone()
                .oneshot(request(packet(b"two"), Some("secret")))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/internal/operations/read/ns/key")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<OperationRead>(&bytes).unwrap(),
            OperationRead::Value {
                value_bytes: b"one".to_vec(),
                timestamp: HlcTimestamp {
                    physical: 1,
                    logical: 0,
                    node_id: "writer".into(),
                },
            }
        );
    }

    #[tokio::test]
    async fn relay_recovers_missing_neighbor_obligations_before_serving() {
        let dir = tempfile::tempdir().unwrap();
        let original = packet(b"relay");
        let inbox_dir = dir.path().join("inbox");
        let outbox_dir = dir.path().join("outbox");
        {
            let mut inbox =
                OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap();
            inbox
                .ensure_owned_manifest(
                    OperationOwnedPrefixes::from_json("[\"\"]")
                        .unwrap()
                        .digest(),
                )
                .unwrap();
            inbox.receive(original.clone()).unwrap();
            // Simulate a crash after inbox fsync but before a neighbor
            // obligation was enrolled. No relay ACK could have been sent.
        }
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(&inbox_dir, Arc::new(PermitTestOperations)).unwrap(),
        ));
        let outbox = Arc::new(Mutex::new(OperationOutbox::open(&outbox_dir).unwrap()));
        let peers = OperationNeighborManifest::from_json(
            r#"[{"node_id":"a","addr":"127.0.0.1:4101","key_prefixes":["key"]},{"node_id":"b","addr":"127.0.0.1:4102","key_prefixes":[""]},{"node_id":"c","addr":"127.0.0.1:4103","key_prefixes":["other"]}]"#,
            &NodeId("self".into()),
        )
        .unwrap();
        let relay = OperationRelay::new(
            inbox.clone(),
            outbox.clone(),
            peers.clone(),
            OperationOwnedPrefixes::from_json("[\"\"]").unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(outbox.lock().await.pending().count(), 2);
        let receipt = tokio::task::spawn_blocking(move || relay.receive(original))
            .await
            .unwrap()
            .unwrap();
        assert!(receipt.already_present);
        assert_eq!(outbox.lock().await.pending().count(), 2);
        drop(outbox);
        let outbox = Arc::new(Mutex::new(OperationOutbox::open(&outbox_dir).unwrap()));
        OperationRelay::new(
            inbox,
            outbox.clone(),
            peers,
            OperationOwnedPrefixes::from_json("[\"\"]").unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(outbox.lock().await.pending().count(), 2);
    }

    #[tokio::test]
    async fn relay_http_enrolls_permitted_neighbor_before_ack() {
        let dir = tempfile::tempdir().unwrap();
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(dir.path().join("inbox"), Arc::new(PermitTestOperations)).unwrap(),
        ));
        let outbox = Arc::new(Mutex::new(
            OperationOutbox::open(dir.path().join("outbox")).unwrap(),
        ));
        let peers = OperationNeighborManifest::from_json(
            r#"[{"node_id":"a","addr":"127.0.0.1:4101","key_prefixes":["key"]},{"node_id":"b","addr":"127.0.0.1:4102","key_prefixes":["other"]}]"#,
            &NodeId("self".into()),
        )
        .unwrap();
        let relay = OperationRelay::new(
            inbox,
            outbox.clone(),
            peers,
            OperationOwnedPrefixes::from_json("[]").unwrap(),
        )
        .await
        .unwrap();
        let app = relay_router(relay, "secret".into());
        let request = Request::builder()
            .method("POST")
            .uri(OPERATION_RECEIVE_PATH)
            .header("authorization", "Bearer secret")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&packet(b"value")).unwrap()))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::OK
        );
        let read = Request::builder()
            .uri("/api/internal/operations/read/ns/key")
            .header("authorization", "Bearer secret")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(read).await.unwrap();
        let bytes = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<OperationRead>(&bytes).unwrap(),
            OperationRead::Unknown
        );
        let guard = outbox.lock().await;
        let entries: Vec<_> = guard.pending().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].peer_addr, "127.0.0.1:4101");
    }

    #[tokio::test]
    async fn local_submit_rejects_transfer_that_exceeds_direct_http_limit_before_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let long_neighbor = "n".repeat(4096);
        let neighbors = OperationNeighborManifest::from_json(
            &serde_json::json!([{
                "node_id": long_neighbor,
                "addr": "127.0.0.1:4101",
                "key_prefixes": [""]
            }])
            .to_string(),
            &NodeId("self".into()),
        )
        .unwrap();
        let initial = packet(&vec![0; 1_000_000]);
        let initial_len = serde_json::to_vec(&initial).unwrap().len();
        let target_len = DIRECT_OPERATION_BODY_LIMIT - 1024;
        assert!(initial_len < target_len);
        let body = packet(&vec![0; 1_000_000 + (target_len - initial_len) / 2]);
        assert!(serde_json::to_vec(&body).unwrap().len() < DIRECT_OPERATION_BODY_LIMIT);
        let transfer = OperationTransfer {
            version: 1,
            sender: NodeId("self".into()),
            transfer_id: u64::MAX,
            receiver: NodeId("n".repeat(4096)),
            destination: None,
            parent: None,
            depth: 0,
            packet: body.clone(),
        };
        assert!(serde_json::to_vec(&transfer).unwrap().len() > DIRECT_OPERATION_BODY_LIMIT);

        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(dir.path().join("inbox"), Arc::new(PermitTestOperations)).unwrap(),
        ));
        let outbox = Arc::new(Mutex::new(
            OperationOutbox::open(dir.path().join("outbox")).unwrap(),
        ));
        let relay = OperationRelay::new(
            inbox.clone(),
            outbox.clone(),
            neighbors,
            OperationOwnedPrefixes::from_json("[\"\"]").unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            relay.submit_local(body.clone()).await,
            Err(StatusCode::PAYLOAD_TOO_LARGE)
        );
        let inbound = OperationTransfer {
            version: 1,
            sender: NodeId("source".into()),
            transfer_id: 0,
            receiver: NodeId("self".into()),
            destination: None,
            parent: None,
            depth: 0,
            packet: body,
        };
        assert!(serde_json::to_vec(&inbound).unwrap().len() < DIRECT_OPERATION_BODY_LIMIT);
        let receive_result = tokio::task::spawn_blocking(move || {
            relay.receive_transfer(inbound).map_err(relay_error_status)
        })
        .await
        .unwrap();
        assert_eq!(receive_result, Err(StatusCode::PAYLOAD_TOO_LARGE));
        assert!(inbox.lock().await.ledger.history().iter().next().is_none());
        assert!(inbox.lock().await.received_transfers().unwrap().is_empty());
        assert!(outbox.lock().await.pending().next().is_none());
    }

    #[tokio::test]
    async fn public_submit_requires_owned_key_and_valid_writer_signature() {
        let dir = tempfile::tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[7; 32]);
        let mut registry = WriterRegistry::new();
        registry
            .register(
                NodeId("writer".into()),
                writer.verifying_key(),
                BTreeSet::from(["ns".into()]),
            )
            .unwrap();
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(
                dir.path().join("owned-inbox"),
                Arc::new(AuthenticatedOperationAdmission::new(
                    Arc::new(registry.clone()),
                    "ns".into(),
                )),
            )
            .unwrap(),
        ));
        let relay = OperationRelay::new(
            Arc::clone(&inbox),
            Arc::new(Mutex::new(
                OperationOutbox::open(dir.path().join("owned-outbox")).unwrap(),
            )),
            OperationNeighborManifest::from_json("[]", &NodeId("self".into())).unwrap(),
            OperationOwnedPrefixes::from_json("[\"key\"]").unwrap(),
        )
        .await
        .unwrap();
        let app = submit_router(relay);
        let request = |packet: OperationPacket| {
            Request::builder()
                .method("POST")
                .uri(OPERATION_SUBMIT_PATH)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&packet).unwrap()))
                .unwrap()
        };
        let mut forged = packet(b"value");
        forged.signed.operation_signature[0] ^= 1;
        assert_eq!(
            app.clone().oneshot(request(forged)).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
        assert!(inbox.lock().await.packets().is_empty());
        let valid = packet(b"value");
        let response = app.clone().oneshot(request(valid.clone())).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let receipt: OperationReceipt =
            serde_json::from_slice(&to_bytes(response.into_body(), 1024).await.unwrap()).unwrap();
        assert_eq!(receipt.commitment, valid.commitment());
        assert!(!receipt.already_present);
        let response = app.oneshot(request(valid.clone())).await.unwrap();
        let retry: OperationReceipt =
            serde_json::from_slice(&to_bytes(response.into_body(), 1024).await.unwrap()).unwrap();
        assert!(retry.already_present);

        let transit_inbox = Arc::new(Mutex::new(
            OperationInbox::open(
                dir.path().join("transit-inbox"),
                Arc::new(AuthenticatedOperationAdmission::new(
                    Arc::new(registry),
                    "ns".into(),
                )),
            )
            .unwrap(),
        ));
        let transit = OperationRelay::new(
            Arc::clone(&transit_inbox),
            Arc::new(Mutex::new(
                OperationOutbox::open(dir.path().join("transit-outbox")).unwrap(),
            )),
            OperationNeighborManifest::from_json("[]", &NodeId("transit".into())).unwrap(),
            OperationOwnedPrefixes::from_json("[]").unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            submit_router(transit)
                .oneshot(request(valid))
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert!(transit_inbox.lock().await.packets().is_empty());
    }

    #[tokio::test]
    async fn persisted_neighbor_hops_deliver_to_a_leaf() {
        use crate::network::operation_outbox::spawn_operation_retry_worker;

        let dir = tempfile::tempdir().unwrap();
        let listeners = [
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        ];
        let addresses: Vec<_> = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().to_string())
            .collect();
        let mut inboxes = Vec::new();
        let mut outboxes = Vec::new();
        let mut servers = Vec::new();
        let mut workers = Vec::new();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        for (index, listener) in listeners.into_iter().enumerate() {
            let inbox = Arc::new(Mutex::new(
                OperationInbox::open(
                    dir.path().join(format!("inbox-{index}")),
                    Arc::new(PermitTestOperations),
                )
                .unwrap(),
            ));
            let outbox = Arc::new(Mutex::new(
                OperationOutbox::open(dir.path().join(format!("outbox-{index}"))).unwrap(),
            ));
            let rows = if index < 2 {
                serde_json::json!([{"node_id": format!("node-{}", index + 1), "addr": addresses[index + 1], "key_prefixes": [""]}])
            } else {
                serde_json::json!([])
            };
            let manifest = OperationNeighborManifest::from_json(
                &rows.to_string(),
                &NodeId(format!("node-{index}")),
            )
            .unwrap();
            let relay = OperationRelay::new(
                inbox.clone(),
                outbox.clone(),
                manifest,
                OperationOwnedPrefixes::from_json(if index == 1 { "[]" } else { "[\"\"]" })
                    .unwrap(),
            )
            .await
            .unwrap();
            servers.push(tokio::spawn(async move {
                axum::serve(listener, relay_router(relay, "secret".into()))
                    .await
                    .unwrap();
            }));
            workers.push(spawn_operation_retry_worker(
                outbox.clone(),
                "secret".into(),
                shutdown_rx.clone(),
            ));
            inboxes.push(inbox);
            outboxes.push(outbox);
        }
        let original = packet(b"through-relay");
        OperationTransportClient::new("secret".into())
            .send(&addresses[0], &original)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                if inboxes[2]
                    .lock()
                    .await
                    .get_packet(&original.signed.bound.operation.subject)
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            inboxes[1]
                .lock()
                .await
                .get(&original.signed.bound.operation.subject)
                .is_none()
        );
        assert!(
            inboxes[1]
                .lock()
                .await
                .get_packet(&original.signed.bound.operation.subject)
                .is_some()
        );
        assert_eq!(outboxes[0].lock().await.pending().count(), 0);
        let _ = shutdown_tx.send(true);
        for worker in workers {
            worker.await.unwrap();
        }
        for server in servers {
            server.abort();
        }
    }
}
