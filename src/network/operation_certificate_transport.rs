//! Opt-in reception of operation certificates under a pinned static trust
//! configuration. Proofs may arrive before their operation bodies; reads use
//! only proofs whose exact bodies are present in the local inbox.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use axum::body::{Body, to_bytes};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router, middleware};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, MutexGuard};

use crate::api::operation_view::CertifiedReadResult;
use crate::authority::certificate::{EpochConfig, KeysetRegistry};
use crate::authority::operation_evidence::{EvidenceStoreError, EvidenceVerifier};
use crate::authority::operation_merkle::{
    CompactOperationCertificate, OperationMerkleCertificate, OperationScope, VerifiedOperation,
};
use crate::authority::operation_support_store::{OperationSupportStore, SupportStoreError};
use crate::authority::static_operation_trust::{StaticOperationEvidenceStore, StaticTrustError};
use crate::control_plane::system_namespace::SystemNamespace;
use crate::http::auth::require_bearer_token;
pub use crate::network::operation_certificate_outbox::{
    CERTIFICATE_RECEIVE_PATH, CertificateReceipt,
};
use crate::network::operation_certificate_outbox::{
    CertificateOutbox, CertificateOutboxError, certificate_commitment,
};
use crate::network::operation_link_auth::{
    OperationLinkAuth, RECEIVER as LINK_RECEIVER, REQUEST_DIGEST as LINK_REQUEST_DIGEST,
    SENDER as LINK_SENDER, SIGNATURE as LINK_SIGNATURE,
};
use crate::network::operation_neighbors::{OperationNeighborManifest, OperationOwnedPrefixes};
use crate::network::operation_support_transport::{
    SUPPORT_RECEIVE_PATH, SupportPacket, SupportReceipt,
};
use crate::network::operation_support_transport::{SupportOutbox, SupportOutboxError};
use crate::network::operation_transport::{
    INTERNAL_OPERATION_BODY_LIMIT, OperationInbox, OperationPacket, OperationReceipt,
    OperationRelay,
};
use crate::operation::{BoundOperation, RecordOutcome};
use crate::operation::{CertificationMode, CrdtSubject, EventDot};
use crate::types::NodeId;

pub const CERTIFIED_READ_PATH: &str =
    "/api/internal/operations/certified-read/{namespace_id}/{*key}";
pub const PUBLIC_CERTIFIED_READ_PATH: &str = "/api/operations/certified-read/{namespace_id}/{*key}";
pub const OPERATION_STATUS_PATH: &str =
    "/api/internal/operations/status/{namespace_id}/{writer}/{counter}";
pub const PUBLIC_OPERATION_STATUS_PATH: &str =
    "/api/operations/status/{namespace_id}/{writer}/{counter}";
pub const CERTIFIED_SUBMIT_PATH: &str = "/api/operations/submit-certified";
const DEFAULT_CERTIFIED_WAIT_MS: u64 = 30_000;
const MAX_CERTIFIED_WAIT_MS: u64 = 60_000;

#[derive(Clone)]
pub struct OperationCertificateState {
    pub inbox: Arc<Mutex<OperationInbox>>,
    pub store: Arc<Mutex<StaticOperationEvidenceStore>>,
    pub supports: Arc<Mutex<OperationSupportStore>>,
    pub support_outbox: Arc<Mutex<SupportOutbox>>,
    pub certificate_outbox: Arc<Mutex<CertificateOutbox>>,
    pub neighbors: OperationNeighborManifest,
    pub owned: OperationOwnedPrefixes,
    pub registry: Arc<RwLock<KeysetRegistry>>,
    pub namespace: Arc<RwLock<SystemNamespace>>,
    pub scope: OperationScope,
    pub epoch_config: EpochConfig,
    pub faults: usize,
    pub link_auth: Option<OperationLinkAuth>,
}

/// Certificate verifiers over one locally owned operation inbox.
#[derive(Clone)]
pub struct MultiOperationCertificateState {
    scopes: Vec<OperationCertificateState>,
}

struct CheckedOperationScope<'a> {
    selected: Option<OperationCertificateState>,
    _registries: Vec<std::sync::RwLockReadGuard<'a, KeysetRegistry>>,
    _namespace: std::sync::RwLockReadGuard<'a, SystemNamespace>,
}

impl MultiOperationCertificateState {
    pub fn new(scopes: Vec<OperationCertificateState>) -> Result<Self, String> {
        let first = scopes
            .first()
            .ok_or("at least one certificate scope is required")?;
        for (index, scope) in scopes.iter().enumerate() {
            if !Arc::ptr_eq(&scope.inbox, &first.inbox)
                || !Arc::ptr_eq(&scope.namespace, &first.namespace)
                || scope.neighbors != first.neighbors
                || scope.owned != first.owned
                || scope
                    .link_auth
                    .as_ref()
                    .map(OperationLinkAuth::manifest_digest)
                    != first
                        .link_auth
                        .as_ref()
                        .map(OperationLinkAuth::manifest_digest)
            {
                return Err("certificate scopes must share inbox, control plane, neighbors, ownership and link authentication".into());
            }
            if scopes[..index]
                .iter()
                .any(|earlier| earlier.scope == scope.scope)
            {
                return Err("certificate scopes must be unique by namespace and prefix".into());
            }
        }
        Ok(Self { scopes })
    }

    pub fn select_for_key(
        &self,
        namespace_id: &str,
        key: &str,
    ) -> Result<Option<OperationCertificateState>, StatusCode> {
        let namespace = self
            .first()
            .namespace
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let selected = self
            .scopes
            .iter()
            .filter(|state| {
                state.scope.namespace_id == namespace_id
                    && scope_applies_to_key(&namespace, &state.scope, key)
            })
            .max_by_key(|state| state.scope.key_prefix.len())
            .cloned();
        Ok(selected)
    }

    fn check_matching_trust_blocking(
        &self,
        namespace_id: &str,
        key: &str,
    ) -> Result<(), StatusCode> {
        // A changed placement may make a previously pinned scope disappear
        // from selection. Check its pin before a no-verifier fallback can
        // report Pending or PolicyUnknown. Call only from a blocking context.
        for scope in self.scopes.iter().filter(|scope| {
            scope.scope.namespace_id == namespace_id && key.starts_with(&scope.scope.key_prefix)
        }) {
            scope.check_trust_blocking()?;
        }
        Ok(())
    }

    fn checked_scope_for_key(
        &self,
        namespace_id: &str,
        key: &str,
    ) -> Result<CheckedOperationScope<'_>, StatusCode> {
        // Acquire every registry before the shared namespace, matching the
        // trust-check lock order. All pins and the selector then see one
        // control-plane snapshot.
        let matching: Vec<_> = self
            .scopes
            .iter()
            .filter(|scope| {
                scope.scope.namespace_id == namespace_id && key.starts_with(&scope.scope.key_prefix)
            })
            .collect();
        let registries: Vec<_> = matching
            .iter()
            .map(|scope| {
                scope
                    .registry
                    .read()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
            })
            .collect::<Result<_, _>>()?;
        let namespace = self
            .first()
            .namespace
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        for (scope, registry) in matching.iter().zip(&registries) {
            scope.check_trust_with_snapshot_blocking(registry, &namespace)?;
        }
        let selected = self
            .scopes
            .iter()
            .filter(|scope| {
                scope.scope.namespace_id == namespace_id
                    && scope_applies_to_key(&namespace, &scope.scope, key)
            })
            .max_by_key(|scope| scope.scope.key_prefix.len())
            .cloned();
        Ok(CheckedOperationScope {
            selected,
            _registries: registries,
            _namespace: namespace,
        })
    }

    /// Read using this key's verifier, or report the local creation mode when
    /// this node has no verifier for the key.
    pub fn read_for_key_blocking(
        &self,
        namespace_id: &str,
        key: &str,
    ) -> Result<CertifiedOperationResponse, StatusCode> {
        // A newly selected child scope must not hide an older matching
        // scope's saved evidence when live placement changes. Check every
        // matching pin before choosing the current verifier.
        let checked = self.checked_scope_for_key(namespace_id, key)?;
        if let Some(scope) = checked.selected.clone() {
            drop(checked);
            return scope.read_for_blocking(namespace_id, key);
        }
        if !self.first().owned.owns_in_namespace(namespace_id, key) {
            let mut inbox = self.first().inbox.blocking_lock();
            let result = non_owner_certified_result(&mut inbox, namespace_id, key);
            drop(checked);
            return result;
        }
        let mut inbox = self.first().inbox.blocking_lock();
        inbox
            .recover_if_poisoned()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let read = inbox.certified_read(&[], namespace_id, key);
        let result = CertifiedOperationResponse {
            result: read.result,
            basis: Vec::new(),
            local_body_seen: read.local_body_seen,
        };
        drop(checked);
        Ok(result)
    }

    fn first(&self) -> &OperationCertificateState {
        &self.scopes[0]
    }
}

/// Whether this key is governed by the exact Authority and placement rows of
/// the scope. A more specific row shadows the parent range.
pub fn scope_applies_to_key(
    namespace: &SystemNamespace,
    scope: &OperationScope,
    key: &str,
) -> bool {
    key.starts_with(&scope.key_prefix)
        && namespace
            .operation_authorities_for_key(&scope.namespace_id, key)
            .is_some_and(|definition| definition.key_range.prefix == scope.key_prefix)
        && namespace
            .operation_placement_for_key(&scope.namespace_id, key)
            .is_some_and(|policy| policy.certified && policy.key_range.prefix == scope.key_prefix)
}

fn scope_applies_to_bound(
    namespace: &SystemNamespace,
    scope: &OperationScope,
    bound: &BoundOperation,
) -> bool {
    bound.operation.subject.namespace_id == scope.namespace_id
        && scope_applies_to_key(namespace, scope, &bound.declaration.key)
}

fn scope_applies_to_receipt(
    namespace: &SystemNamespace,
    scope: &OperationScope,
    receipt: &crate::operation::OperationReceipt,
) -> bool {
    receipt.subject.namespace_id == scope.namespace_id
        && scope_applies_to_key(namespace, scope, &receipt.declaration.key)
}

/// Pin the exact control-plane assignment for a static operation scope.
pub fn static_policy_digest(
    namespace: &SystemNamespace,
    scope: &OperationScope,
    registry: &KeysetRegistry,
) -> Result<[u8; 32], String> {
    let prefix = &scope.key_prefix;
    let authority = namespace
        .operation_authority_definition(&scope.namespace_id, prefix)
        .ok_or("operation scope has no exact Authority definition")?;
    let policy = namespace
        .operation_placement_policy(&scope.namespace_id, prefix)
        .ok_or("operation scope has no exact placement policy")?;
    if !policy.certified {
        return Err("operation scope placement policy is not certified".into());
    }
    let ids: BTreeSet<_> = authority.authority_nodes.iter().map(|id| &id.0).collect();
    if ids.is_empty() || ids.len() != authority.authority_nodes.len() {
        return Err("operation scope has empty or duplicate Authority IDs".into());
    }
    let version = registry.current_version();
    let keys = registry
        .get_keys(&version)
        .ok_or("operation scope has no trusted Authority keyset")?;
    let key_ids: BTreeSet<_> = keys.iter().map(|(id, _)| &id.0).collect();
    if key_ids != ids || key_ids.len() != keys.len() {
        return Err("operation Authority IDs differ from trusted keyset".into());
    }
    let mut hash = Sha256::new();
    if namespace.has_operation_assignment(&scope.namespace_id, prefix) {
        hash.update(b"asteroidb/static-operation-policy/v7".as_slice());
        hash.update((scope.namespace_id.len() as u64).to_be_bytes());
        hash.update(scope.namespace_id.as_bytes());
    } else {
        hash.update(b"asteroidb/static-operation-policy/v6".as_slice());
    }
    hash.update((prefix.len() as u64).to_be_bytes());
    hash.update(prefix.as_bytes());
    // The generation flag and member order do not change the effective
    // Authority for a key. The verifier counts distinct member IDs.
    let mut authority_members: Vec<_> = authority.authority_nodes.iter().map(|id| &id.0).collect();
    authority_members.sort();
    let authority_bytes = serde_json::to_vec(&(&authority.key_range.prefix, authority_members))
        .map_err(|error| format!("cannot encode Authority definition: {error}"))?;
    hash.update((authority_bytes.len() as u64).to_be_bytes());
    hash.update(authority_bytes);
    // Operation admission and proof selection use the effective range and
    // certification mode. Replica hints, tags, and policy bookkeeping do not
    // change which Authority may certify this key.
    let policy_bytes = serde_json::to_vec(&(&policy.key_range.prefix, policy.certified))
        .map_err(|error| format!("cannot encode operation placement: {error}"))?;
    hash.update((policy_bytes.len() as u64).to_be_bytes());
    hash.update(policy_bytes);
    hash.update((ids.len() as u64).to_be_bytes());
    for id in ids {
        hash.update((id.len() as u64).to_be_bytes());
        hash.update(id.as_bytes());
    }
    // A descendant changes which keys this scope owns, but it does not
    // change the trusted Authority or placement for an unaffected sibling.
    // Every use of saved evidence must check the live key selector instead.
    Ok(hash.finalize().into())
}

/// Evidence and application reads are valid only under the same static
/// control-plane policy and local assignment used at first mount.
pub fn static_read_trust_digest(
    namespace: &SystemNamespace,
    scope: &OperationScope,
    registry: &KeysetRegistry,
    owned: &OperationOwnedPrefixes,
) -> Result<[u8; 32], String> {
    let mut hash = Sha256::new();
    hash.update(b"asteroidb/static-operation-read-trust/v1");
    hash.update(static_policy_digest(namespace, scope, registry)?);
    hash.update(owned.digest());
    Ok(hash.finalize().into())
}

fn commitment(certificate: &OperationMerkleCertificate) -> Result<[u8; 32], StatusCode> {
    certificate_commitment(certificate).map_err(|_| StatusCode::BAD_REQUEST)
}

fn store_status(error: StaticTrustError) -> StatusCode {
    match error {
        StaticTrustError::TrustChanged => StatusCode::SERVICE_UNAVAILABLE,
        StaticTrustError::Evidence(EvidenceStoreError::Verification(_))
        | StaticTrustError::Evidence(EvidenceStoreError::ConflictingContent) => {
            StatusCode::FORBIDDEN
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn support_status(error: SupportStoreError) -> StatusCode {
    match error {
        SupportStoreError::Capacity(_) => StatusCode::PAYLOAD_TOO_LARGE,
        SupportStoreError::Verification(_) | SupportStoreError::Conflict => StatusCode::FORBIDDEN,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// The Authority worker must retry temporary storage failures without
/// treating a changed trust pin or invalid persisted evidence as temporary.
#[derive(Debug)]
pub enum AuthorityTrustCheckError {
    Retryable(String),
    Permanent(String),
}

fn authority_evidence_error(error: StaticTrustError) -> AuthorityTrustCheckError {
    if retryable_evidence_error(&error) {
        AuthorityTrustCheckError::Retryable(error.to_string())
    } else {
        AuthorityTrustCheckError::Permanent(error.to_string())
    }
}

fn retryable_evidence_error(error: &StaticTrustError) -> bool {
    matches!(
        error,
        StaticTrustError::Io(_)
            | StaticTrustError::Evidence(EvidenceStoreError::Io(_))
            | StaticTrustError::Evidence(EvidenceStoreError::Lock(_))
    )
}

fn authority_support_error(error: SupportStoreError) -> AuthorityTrustCheckError {
    match error {
        SupportStoreError::Io(_) | SupportStoreError::Lock(_) => {
            AuthorityTrustCheckError::Retryable(error.to_string())
        }
        _ => AuthorityTrustCheckError::Permanent(error.to_string()),
    }
}

#[derive(Debug, thiserror::Error)]
enum EvidenceDeliveryError {
    #[error("certificate delivery failed: {0}")]
    Certificate(#[from] CertificateOutboxError),
    #[error("support delivery failed: {0}")]
    Support(#[from] SupportOutboxError),
    #[error("certificate support fallback is invalid: {0}")]
    Invalid(String),
}

/// A full certificate can exceed the HTTP limit when its individual supports
/// are still small enough to travel separately. Keep the exact proof set: a
/// peer assembles the same quorum from durable individual support receipts.
fn split_certificate_at_limit(
    certificate: &OperationMerkleCertificate,
    body_limit: usize,
) -> Result<Option<Vec<SupportPacket>>, EvidenceDeliveryError> {
    let size = serde_json::to_vec(certificate)
        .map_err(|error| EvidenceDeliveryError::Invalid(error.to_string()))?
        .len();
    if size <= body_limit {
        return Ok(None);
    }
    let packets: Vec<_> = certificate
        .supports
        .iter()
        .cloned()
        .map(|support| SupportPacket {
            operation: certificate.operation.clone(),
            support,
        })
        .collect();
    for packet in &packets {
        let size = serde_json::to_vec(packet)
            .map_err(|error| EvidenceDeliveryError::Invalid(error.to_string()))?
            .len();
        if size > body_limit {
            return Err(EvidenceDeliveryError::Invalid(
                "an individual support exceeds the HTTP body limit".into(),
            ));
        }
    }
    Ok(Some(packets))
}

fn evidence_delivery_ready_at_limit(
    certificate: &OperationMerkleCertificate,
    neighbors: &OperationNeighborManifest,
    certificate_outbox: &CertificateOutbox,
    support_outbox: &SupportOutbox,
    body_limit: usize,
) -> bool {
    let namespace_id = &certificate.operation.operation.subject.namespace_id;
    let key = &certificate.operation.declaration.key;
    match split_certificate_at_limit(certificate, body_limit) {
        Ok(Some(packets)) => neighbors.permitted_for(namespace_id, key).all(|neighbor| {
            packets
                .iter()
                .all(|packet| support_outbox.has_durable_receipt(&neighbor.addr, packet))
        }),
        Ok(None) => neighbors.permitted_for(namespace_id, key).all(|neighbor| {
            certificate_outbox
                .has_enqueued(&neighbor.addr, certificate)
                .unwrap_or(false)
        }),
        Err(_) => false,
    }
}

/// A compact proof can be rebuilt with a newly received exact body. Reuse its
/// original support set so outbox identity remains stable across compaction.
fn retained_certificate_for_body(
    store: &StaticOperationEvidenceStore,
    operation: &BoundOperation,
    verifier: &EvidenceVerifier<'_>,
    policy_digest: [u8; 32],
) -> Result<OperationMerkleCertificate, StatusCode> {
    let subject = &operation.operation.subject;
    if let Some(proof) = store
        .get_verified(subject, verifier, policy_digest)
        .map_err(store_status)?
    {
        return Ok(proof.certificate().clone());
    }
    let receipt = crate::operation::OperationReceipt::from(operation);
    let proof = store
        .get_verified_compact(&receipt, verifier, policy_digest)
        .map_err(store_status)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(OperationMerkleCertificate {
        operation: operation.clone(),
        supports: proof.certificate().supports.clone(),
    })
}

fn retained_certificate_for_authority(
    store: &StaticOperationEvidenceStore,
    operation: &BoundOperation,
    verifier: &EvidenceVerifier<'_>,
    digest: [u8; 32],
) -> Result<OperationMerkleCertificate, AuthorityTrustCheckError> {
    let subject = &operation.operation.subject;
    if let Some(proof) = store
        .get_verified(subject, verifier, digest)
        .map_err(authority_evidence_error)?
    {
        return Ok(proof.certificate().clone());
    }
    let receipt = crate::operation::OperationReceipt::from(operation);
    let proof = store
        .get_verified_compact(&receipt, verifier, digest)
        .map_err(authority_evidence_error)?
        .ok_or_else(|| {
            AuthorityTrustCheckError::Permanent(
                "certified Authority support lacks its retained proof".into(),
            )
        })?;
    Ok(OperationMerkleCertificate {
        operation: operation.clone(),
        supports: proof.certificate().supports.clone(),
    })
}

async fn receive_support_http(
    State(state): State<OperationCertificateState>,
    Json(packet): Json<SupportPacket>,
) -> Result<Json<SupportReceipt>, StatusCode> {
    tokio::task::spawn_blocking(move || state.record_support_blocking(packet))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
}

impl OperationCertificateState {
    fn enqueue_evidence_delivery(
        &self,
        certificate: &OperationMerkleCertificate,
    ) -> Result<(), EvidenceDeliveryError> {
        self.enqueue_evidence_delivery_at_limit(certificate, INTERNAL_OPERATION_BODY_LIMIT)
    }

    fn enqueue_evidence_delivery_at_limit(
        &self,
        certificate: &OperationMerkleCertificate,
        body_limit: usize,
    ) -> Result<(), EvidenceDeliveryError> {
        let fallback = split_certificate_at_limit(certificate, body_limit)?;
        let mut first_error = None;
        for addr in self.neighbors.permitted_addrs_for(
            &certificate.operation.operation.subject.namespace_id,
            &certificate.operation.declaration.key,
        ) {
            if let Some(packets) = &fallback {
                for packet in packets {
                    if let Err(error) = self
                        .support_outbox
                        .blocking_lock()
                        .ensure_enqueued(addr.clone(), packet.clone())
                    {
                        first_error.get_or_insert(EvidenceDeliveryError::Support(error));
                    }
                }
            } else {
                if let Err(error) = self
                    .certificate_outbox
                    .blocking_lock()
                    .ensure_enqueued(addr, certificate.clone())
                {
                    first_error.get_or_insert(EvidenceDeliveryError::Certificate(error));
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn recovered_evidence(
        &self,
        verifier: &EvidenceVerifier<'_>,
        digest: [u8; 32],
    ) -> Result<MutexGuard<'_, StaticOperationEvidenceStore>, StaticTrustError> {
        let mut store = self.store.blocking_lock();
        store.reopen_if_poisoned(verifier, digest)?;
        Ok(store)
    }

    fn recovered_supports(
        &self,
        registry: &KeysetRegistry,
    ) -> Result<MutexGuard<'_, OperationSupportStore>, SupportStoreError> {
        let supports = self.supports.blocking_lock();
        if !supports.is_poisoned() {
            return Ok(supports);
        }
        drop(supports);
        let receipts = {
            let mut inbox = self.inbox.blocking_lock();
            inbox.recover_if_poisoned().map_err(|error| match error {
                crate::operation::PersistOperationError::Io(error) => SupportStoreError::Io(error),
                crate::operation::PersistOperationError::Operation(error) => {
                    SupportStoreError::Invalid(format!(
                        "cannot recover operation inbox for support verification: {error:?}"
                    ))
                }
            })?;
            inbox.receipts()
        };
        let mut supports = self.supports.blocking_lock();
        supports.reopen_if_poisoned(
            registry,
            &self.scope,
            &self.epoch_config,
            self.neighbors.digest(),
            &receipts,
        )?;
        Ok(supports)
    }

    pub fn pin_support_neighbor_manifest_blocking(&self) -> Result<(), AuthorityTrustCheckError> {
        self.check_authority_trust_blocking()?;
        let registry = self.registry.read().map_err(|error| {
            AuthorityTrustCheckError::Permanent(format!("poisoned keyset registry: {error}"))
        })?;
        self.recovered_supports(&registry)
            .map_err(authority_support_error)?
            .pin_neighbor_manifest(self.neighbors.digest())
            .map_err(authority_support_error)
    }

    /// Rebuild send obligations from durable support and certificate evidence.
    /// Evidence admission and outbox enrollment are separate disk writes, so
    /// this also closes the gap when enrollment failed without a restart.
    pub fn reconcile_evidence_delivery_blocking(&self) -> Result<(), String> {
        self.reconcile_evidence_delivery_at_limit(INTERNAL_OPERATION_BODY_LIMIT)
    }

    fn reconcile_evidence_delivery_at_limit(&self, body_limit: usize) -> Result<(), String> {
        let registry = self.registry.read().map_err(|error| error.to_string())?;
        let namespace = self.namespace.read().map_err(|error| error.to_string())?;
        let digest = static_read_trust_digest(&namespace, &self.scope, &registry, &self.owned)?;
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &self.scope,
            current_epoch: 0,
            epoch_config: &self.epoch_config,
            faults: self.faults,
        };
        let mut store = self
            .recovered_evidence(&verifier, digest)
            .map_err(|error| error.to_string())?;
        store
            .check_trust(&verifier, &digest)
            .map_err(|error| error.to_string())?;
        let supports = self
            .recovered_supports(&registry)
            .map_err(|error| error.to_string())?;
        let packets: Vec<_> = supports
            .packets()
            .filter(|(operation, _)| scope_applies_to_bound(&namespace, &self.scope, operation))
            .map(|(operation, support)| SupportPacket { operation, support })
            .collect();
        let mut first_error = None;
        for subject in supports.subjects() {
            match supports.assemble_certificate(subject, self.faults) {
                Ok(Some(certificate)) => {
                    if !scope_applies_to_bound(&namespace, &self.scope, &certificate.operation) {
                        continue;
                    }
                    if let Err(error) = store.reopen_if_poisoned(&verifier, digest) {
                        first_error.get_or_insert_with(|| error.to_string());
                        continue;
                    }
                    if let Err(error) = store.record_and_sync(certificate, &verifier, digest) {
                        first_error.get_or_insert_with(|| error.to_string());
                        // A failed fsync may still have published the certificate.
                        // Reopen and verify it before processing the next subject.
                        if let Err(recovery) = store.reopen_if_poisoned(&verifier, digest) {
                            first_error.get_or_insert_with(|| recovery.to_string());
                        }
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    first_error.get_or_insert_with(|| error.to_string());
                }
            }
        }
        let certificates: Vec<_> = match store.reopen_if_poisoned(&verifier, digest) {
            Ok(()) => match store.verified_all(&verifier, digest) {
                Ok(proofs) => proofs
                    .into_iter()
                    .filter(|proof| scope_applies_to_bound(&namespace, &self.scope, proof.bound()))
                    .map(|proof| proof.certificate().clone())
                    .collect(),
                Err(error) => {
                    first_error.get_or_insert_with(|| error.to_string());
                    Vec::new()
                }
            },
            Err(error) => {
                first_error.get_or_insert_with(|| error.to_string());
                Vec::new()
            }
        };
        drop(supports);
        drop(store);

        for packet in packets {
            for addr in self.neighbors.permitted_addrs_for(
                &packet.operation.operation.subject.namespace_id,
                &packet.operation.declaration.key,
            ) {
                if let Err(error) = self
                    .support_outbox
                    .blocking_lock()
                    .ensure_enqueued(addr, packet.clone())
                {
                    first_error.get_or_insert_with(|| error.to_string());
                }
            }
        }
        for certificate in certificates {
            if let Err(error) = self.enqueue_evidence_delivery_at_limit(&certificate, body_limit) {
                first_error.get_or_insert_with(|| error.to_string());
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Compute a public certified read from the exact locally held operation
    /// bodies and independently verified certificates. Call from a blocking
    /// thread because the evidence store and inbox use blocking locks here.
    pub fn read_for_blocking(
        &self,
        namespace_id: &str,
        key: &str,
    ) -> Result<CertifiedOperationResponse, StatusCode> {
        if !self.owned.owns_in_namespace(namespace_id, key) {
            // A locally pinned scope still has to be trusted before its
            // non-owner public fallback can report Unknown or NotTargeted.
            if namespace_id == self.scope.namespace_id && key.starts_with(&self.scope.key_prefix) {
                self.check_trust_blocking()?;
            }
            let mut inbox = self.inbox.blocking_lock();
            return non_owner_certified_result(&mut inbox, namespace_id, key);
        }
        let registry = self
            .registry
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let namespace = self
            .namespace
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if namespace_id != self.scope.namespace_id
            || !scope_applies_to_key(&namespace, &self.scope, key)
        {
            // A changed placement can remove this scope from live selection.
            // Preserve its pinned trust before reporting an unverified read.
            if namespace_id == self.scope.namespace_id && key.starts_with(&self.scope.key_prefix) {
                drop(namespace);
                drop(registry);
                self.check_trust_blocking()?;
            }
            let mut inbox = self.inbox.blocking_lock();
            inbox
                .recover_if_poisoned()
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
            let read = inbox.certified_read(&[], namespace_id, key);
            return Ok(CertifiedOperationResponse {
                result: read.result,
                basis: Vec::new(),
                local_body_seen: read.local_body_seen,
            });
        }
        let policy_digest =
            static_read_trust_digest(&namespace, &self.scope, &registry, &self.owned)
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &self.scope,
            current_epoch: 0,
            epoch_config: &self.epoch_config,
            faults: self.faults,
        };
        drop(self.recovered_supports(&registry).map_err(support_status)?);
        let verified = self
            .recovered_evidence(&verifier, policy_digest)
            .map_err(store_status)?
            .verified_all(&verifier, policy_digest)
            .map_err(store_status)?;
        let mut inbox = self.inbox.blocking_lock();
        inbox
            .recover_if_poisoned()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let read = inbox.certified_read(&verified, namespace_id, key);
        Ok(CertifiedOperationResponse {
            result: read.result,
            basis: read
                .basis
                .iter()
                .filter(|proof| {
                    proof.bound().operation.subject.namespace_id == namespace_id
                        && proof.bound().operation.action.key() == key
                })
                .map(|proof| proof.certificate().clone())
                .collect(),
            local_body_seen: read.local_body_seen,
        })
    }
    /// Reverify stored certificates under the current pinned trust inputs
    /// before using them as the accepted set for ledger body collection.
    pub fn compact_ledger_bodies_blocking(&self, relay: &OperationRelay) -> Result<usize, String> {
        self.compact_ledger_bodies_at_limit(relay, INTERNAL_OPERATION_BODY_LIMIT)
    }

    fn compact_ledger_bodies_at_limit(
        &self,
        relay: &OperationRelay,
        body_limit: usize,
    ) -> Result<usize, String> {
        let registry = self.registry.read().map_err(|e| e.to_string())?;
        let namespace = self.namespace.read().map_err(|e| e.to_string())?;
        let digest = static_read_trust_digest(&namespace, &self.scope, &registry, &self.owned)?;
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &self.scope,
            current_epoch: 0,
            epoch_config: &self.epoch_config,
            faults: self.faults,
        };
        let verified = self
            .recovered_evidence(&verifier, digest)
            .map_err(|e| e.to_string())?
            .verified_all(&verifier, digest)
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|proof| scope_applies_to_bound(&namespace, &self.scope, proof.bound()))
            .collect::<Vec<_>>();
        let ledger_collected = relay
            .compact_certified_ledger_bodies(&verified)
            .map_err(|e| format!("{e:?}"))?;
        let (receipts, retained_bodies) = {
            let inbox_handle = relay.inbox();
            let inbox = inbox_handle.blocking_lock();
            let receipts = inbox.receipts();
            let retained_bodies = receipts
                .iter()
                .filter(|receipt| inbox.get(&receipt.subject).is_some())
                .map(|receipt| receipt.subject.clone())
                .collect::<BTreeSet<_>>();
            (receipts, retained_bodies)
        };
        let outbox = self.certificate_outbox.blocking_lock();
        let support_outbox = self.support_outbox.blocking_lock();
        let proofs: BTreeMap<_, _> = verified
            .iter()
            .map(|proof| (proof.bound().operation.subject.clone(), proof))
            .collect();
        let compactable: Vec<_> = receipts
            .iter()
            .filter(|receipt| {
                // The public Certified read needs a full certificate for
                // every body that still contributes to its local projection.
                // The ledger GC keeps a certified live winner, so a receipt
                // alone does not mean the body is gone.
                if retained_bodies.contains(&receipt.subject) {
                    return false;
                }
                let Some(proof) = proofs.get(&receipt.subject) else {
                    return false;
                };
                if crate::operation::OperationReceipt::from(proof.bound()) != **receipt {
                    return false;
                }
                evidence_delivery_ready_at_limit(
                    proof.certificate(),
                    &self.neighbors,
                    &outbox,
                    &support_outbox,
                    body_limit,
                )
            })
            .cloned()
            .collect();
        drop(support_outbox);
        drop(outbox);
        self.recovered_evidence(&verifier, digest)
            .map_err(|e| e.to_string())?
            .compact_receipted_and_sync(&compactable, &verifier, digest)
            .map_err(|e| e.to_string())?;
        self.certificate_outbox
            .blocking_lock()
            .compact_acked()
            .map_err(|e| e.to_string())?;
        self.support_outbox
            .blocking_lock()
            .compact_acked_and_sync()
            .map_err(|e| e.to_string())?;
        let compact_evidence = {
            let store = self
                .recovered_evidence(&verifier, digest)
                .map_err(|e| e.to_string())?;
            let mut accepted = Vec::new();
            for receipt in &receipts {
                if !scope_applies_to_receipt(&namespace, &self.scope, receipt) {
                    continue;
                }
                if store
                    .get_verified_compact(receipt, &verifier, digest)
                    .map_err(|e| e.to_string())?
                    .is_some()
                {
                    accepted.push(receipt.clone());
                }
            }
            accepted
        };
        let mut supports = self
            .recovered_supports(&registry)
            .map_err(|e| e.to_string())?;
        let packets: BTreeMap<CrdtSubject, Vec<SupportPacket>> = {
            let mut packets = BTreeMap::new();
            for (operation, support) in supports.packets() {
                packets
                    .entry(operation.operation.subject.clone())
                    .or_insert_with(Vec::new)
                    .push(SupportPacket { operation, support });
            }
            packets
        };
        let support_outbox = self.support_outbox.blocking_lock();
        let releasable: Vec<_> = compact_evidence
            .into_iter()
            .filter(|receipt| {
                packets.get(&receipt.subject).is_some_and(|packets| {
                    packets.iter().all(|packet| {
                        crate::operation::OperationReceipt::from(&packet.operation) == *receipt
                            && self
                                .neighbors
                                .permitted_for(
                                    &receipt.subject.namespace_id,
                                    &receipt.declaration.key,
                                )
                                .all(|neighbor| {
                                    support_outbox.has_durable_receipt(&neighbor.addr, packet)
                                })
                    })
                })
            })
            .collect();
        drop(support_outbox);
        if !releasable.is_empty() {
            supports
                .pin_neighbor_manifest(self.neighbors.digest())
                .map_err(|e| e.to_string())?;
            supports
                .compact_receipted_and_sync(&releasable)
                .map_err(|e| e.to_string())?;
        }
        Ok(ledger_collected)
    }

    pub fn status_for_blocking(
        &self,
        subject: CrdtSubject,
    ) -> Result<OperationStatusResponse, StatusCode> {
        let mut inbox = self.inbox.blocking_lock();
        inbox
            .recover_if_poisoned()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let body = inbox.get(&subject);
        let receipt = inbox.receipt(&subject);
        let policy_known = inbox.creation_policy_known(&subject);
        drop(inbox);
        let Some(key) = body
            .as_ref()
            .map(|body| body.operation.action.key())
            .or_else(|| {
                receipt
                    .as_ref()
                    .map(|receipt| receipt.declaration.key.as_str())
            })
        else {
            return Ok(OperationStatusResponse {
                status: OperationCertificationStatus::Unknown,
                certificate: None,
                compact_certificate: None,
                operation_receipt: None,
            });
        };
        let matches_pinned_scope = subject.namespace_id == self.scope.namespace_id
            && key.starts_with(&self.scope.key_prefix);
        // An unrelated operation does not use this scope's saved evidence.
        // Release the inbox lock before checking the trust pin for a matching
        // key, including a key that this node does not own.
        if matches_pinned_scope {
            self.check_trust_blocking()?;
        }
        if !self.owned.owns_in_namespace(&subject.namespace_id, key) {
            return Ok(OperationStatusResponse {
                status: if policy_known
                    && body
                        .as_ref()
                        .map(|body| body.declaration.mode)
                        .or_else(|| receipt.as_ref().map(|receipt| receipt.declaration.mode))
                        == Some(CertificationMode::Disabled)
                {
                    OperationCertificationStatus::NotTarget
                } else {
                    OperationCertificationStatus::Unknown
                },
                certificate: None,
                compact_certificate: None,
                operation_receipt: None,
            });
        }
        let mode = body
            .as_ref()
            .map(|body| body.declaration.mode)
            .or_else(|| receipt.as_ref().map(|receipt| receipt.declaration.mode));
        if !policy_known && mode == Some(CertificationMode::Disabled) {
            return Ok(OperationStatusResponse {
                status: OperationCertificationStatus::PolicyUnknown,
                certificate: None,
                compact_certificate: None,
                operation_receipt: None,
            });
        }
        if mode == Some(CertificationMode::Disabled) {
            return Ok(OperationStatusResponse {
                status: OperationCertificationStatus::NotTarget,
                certificate: None,
                compact_certificate: None,
                operation_receipt: None,
            });
        }
        if !matches_pinned_scope {
            return Ok(OperationStatusResponse {
                status: if policy_known {
                    OperationCertificationStatus::Pending
                } else {
                    OperationCertificationStatus::PolicyUnknown
                },
                certificate: None,
                compact_certificate: None,
                operation_receipt: None,
            });
        }
        let registry = self
            .registry
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let namespace = self
            .namespace
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let policy_digest =
            static_read_trust_digest(&namespace, &self.scope, &registry, &self.owned)
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &self.scope,
            current_epoch: 0,
            epoch_config: &self.epoch_config,
            faults: self.faults,
        };
        drop(self.recovered_supports(&registry).map_err(support_status)?);
        let store = self
            .recovered_evidence(&verifier, policy_digest)
            .map_err(store_status)?;
        store
            .check_trust(&verifier, &policy_digest)
            .map_err(store_status)?;
        // The immutable creation mode determines whether certification is a
        // target. A certificate scope is only this node's verification reach;
        // an Enabled operation outside it is still pending, not opted out.
        let in_scope = body.as_ref().is_some_and(|body| self.scope.contains(body))
            || receipt.as_ref().is_some_and(|receipt| {
                receipt.subject.namespace_id == self.scope.namespace_id
                    && receipt.declaration.key.starts_with(&self.scope.key_prefix)
            });
        if !in_scope || !scope_applies_to_key(&namespace, &self.scope, key) {
            return Ok(OperationStatusResponse {
                status: if policy_known {
                    OperationCertificationStatus::Pending
                } else {
                    OperationCertificationStatus::PolicyUnknown
                },
                certificate: None,
                compact_certificate: None,
                operation_receipt: None,
            });
        }
        let verified = store
            .get_verified(&subject, &verifier, policy_digest)
            .map_err(store_status)?;
        let evidence_receipt = receipt
            .clone()
            .or_else(|| body.as_ref().map(crate::operation::OperationReceipt::from));
        let compact_verified = if verified.is_none() {
            evidence_receipt
                .as_ref()
                .map(|receipt| store.get_verified_compact(receipt, &verifier, policy_digest))
                .transpose()
                .map_err(store_status)?
                .flatten()
        } else {
            None
        };
        let (certificate, mut compact_certificate) =
            status_certificates(verified, body.as_ref(), receipt.as_ref());
        if let Some(proof) = compact_verified {
            compact_certificate = Some(proof.certificate().clone());
        }
        let operation_receipt = compact_certificate
            .is_some()
            .then_some(evidence_receipt)
            .flatten();
        Ok(OperationStatusResponse {
            status: if !policy_known {
                OperationCertificationStatus::PolicyUnknown
            } else if certificate.is_some() || compact_certificate.is_some() {
                OperationCertificationStatus::Certified
            } else {
                OperationCertificationStatus::Pending
            },
            certificate,
            compact_certificate,
            operation_receipt,
        })
    }

    pub fn check_trust_blocking(&self) -> Result<(), StatusCode> {
        let registry = self
            .registry
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let namespace = self
            .namespace
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        self.check_trust_with_snapshot_blocking(&registry, &namespace)
    }

    fn check_trust_with_snapshot_blocking(
        &self,
        registry: &KeysetRegistry,
        namespace: &SystemNamespace,
    ) -> Result<(), StatusCode> {
        let digest = static_read_trust_digest(namespace, &self.scope, registry, &self.owned)
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let verifier = EvidenceVerifier {
            registry,
            scope: &self.scope,
            current_epoch: 0,
            epoch_config: &self.epoch_config,
            faults: self.faults,
        };
        self.recovered_evidence(&verifier, digest)
            .map_err(store_status)?
            .check_trust(&verifier, &digest)
            .map_err(store_status)?;
        drop(self.recovered_supports(registry).map_err(support_status)?);
        Ok(())
    }

    /// Typed variant for the Authority loop. HTTP status codes combine
    /// recoverable disk errors and permanent pin or log violations.
    pub fn check_authority_trust_blocking(&self) -> Result<(), AuthorityTrustCheckError> {
        let registry = self.registry.read().map_err(|error| {
            AuthorityTrustCheckError::Permanent(format!("poisoned keyset registry: {error}"))
        })?;
        let namespace = self.namespace.read().map_err(|error| {
            AuthorityTrustCheckError::Permanent(format!("poisoned namespace registry: {error}"))
        })?;
        let digest = static_read_trust_digest(&namespace, &self.scope, &registry, &self.owned)
            .map_err(AuthorityTrustCheckError::Permanent)?;
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &self.scope,
            current_epoch: 0,
            epoch_config: &self.epoch_config,
            faults: self.faults,
        };
        self.recovered_evidence(&verifier, digest)
            .map_err(authority_evidence_error)?
            .check_trust(&verifier, &digest)
            .map_err(authority_evidence_error)?;
        drop(
            self.recovered_supports(&registry)
                .map_err(authority_support_error)?,
        );
        Ok(())
    }

    /// Return exactly the admitted receipts backed by currently verified
    /// full or compact certificate evidence. Authority root retirement uses
    /// this after checking every support delivery obligation separately.
    pub fn verified_receipt_subjects_blocking(
        &self,
        receipts: &[crate::operation::OperationReceipt],
    ) -> Result<BTreeSet<CrdtSubject>, AuthorityTrustCheckError> {
        let registry = self.registry.read().map_err(|error| {
            AuthorityTrustCheckError::Permanent(format!("poisoned keyset registry: {error}"))
        })?;
        let namespace = self.namespace.read().map_err(|error| {
            AuthorityTrustCheckError::Permanent(format!("poisoned namespace registry: {error}"))
        })?;
        let digest = static_read_trust_digest(&namespace, &self.scope, &registry, &self.owned)
            .map_err(AuthorityTrustCheckError::Permanent)?;
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &self.scope,
            current_epoch: 0,
            epoch_config: &self.epoch_config,
            faults: self.faults,
        };
        let store = self
            .recovered_evidence(&verifier, digest)
            .map_err(authority_evidence_error)?;
        let mut verified = BTreeSet::new();
        for receipt in receipts {
            if !scope_applies_to_receipt(&namespace, &self.scope, receipt) {
                continue;
            }
            if let Some(proof) = store
                .get_verified(&receipt.subject, &verifier, digest)
                .map_err(authority_evidence_error)?
            {
                if crate::operation::OperationReceipt::from(proof.bound()) != *receipt {
                    return Err(AuthorityTrustCheckError::Permanent(
                        "Authority root receipt differs from verified evidence".into(),
                    ));
                }
                verified.insert(receipt.subject.clone());
            } else if store
                .get_verified_compact(receipt, &verifier, digest)
                .map_err(authority_evidence_error)?
                .is_some()
            {
                verified.insert(receipt.subject.clone());
            }
        }
        Ok(verified)
    }

    /// The local Authority and HTTP receiver share the same durable support
    /// admission and promotion path.
    pub fn record_support_blocking(
        &self,
        packet: SupportPacket,
    ) -> Result<SupportReceipt, StatusCode> {
        let commitment = packet.commitment();
        let registry = self
            .registry
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let namespace = self
            .namespace
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if packet.operation.operation.subject.namespace_id != self.scope.namespace_id
            || !scope_applies_to_key(&namespace, &self.scope, &packet.operation.declaration.key)
        {
            if packet.operation.operation.subject.namespace_id == self.scope.namespace_id
                && packet
                    .operation
                    .declaration
                    .key
                    .starts_with(&self.scope.key_prefix)
            {
                drop(namespace);
                drop(registry);
                self.check_trust_blocking()?;
            }
            return Err(StatusCode::UNPROCESSABLE_ENTITY);
        }
        let policy_digest =
            static_read_trust_digest(&namespace, &self.scope, &registry, &self.owned)
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &self.scope,
            current_epoch: 0,
            epoch_config: &self.epoch_config,
            faults: self.faults,
        };
        let mut store = self
            .recovered_evidence(&verifier, policy_digest)
            .map_err(store_status)?;
        store
            .check_trust(&verifier, &policy_digest)
            .map_err(store_status)?;
        let mut supports = self.recovered_supports(&registry).map_err(support_status)?;
        let subject = packet.operation.operation.subject.clone();
        let outcome = supports
            .record_and_sync(packet.operation.clone(), packet.support.clone())
            .map_err(support_status)?;
        let certificate = supports
            .assemble_certificate(&subject, self.faults)
            .map_err(support_status)?;
        let existing_receipt = crate::operation::OperationReceipt::from(&packet.operation);
        let mut evidence_available = true;
        if let Some(certificate) = certificate
            && let Err(error) = store.record_and_sync(certificate, &verifier, policy_digest)
        {
            if !retryable_evidence_error(&error) {
                return Err(store_status(error));
            }
            tracing::warn!(error = %error, "durable support awaits certificate assembly retry");
            evidence_available = false;
        }
        let certificate_ready = if evidence_available {
            let full_ready = store
                .get_verified(&subject, &verifier, policy_digest)
                .map_err(store_status)?
                .is_some_and(|proof| proof.bound() == &packet.operation);
            let compact_ready = store
                .get_verified_compact(&existing_receipt, &verifier, policy_digest)
                .map_err(store_status)?
                .is_some();
            full_ready || compact_ready
        } else {
            false
        };
        if certificate_ready {
            let retained =
                retained_certificate_for_body(&store, &packet.operation, &verifier, policy_digest)?;
            match self.enqueue_evidence_delivery(&retained) {
                Ok(()) => {}
                Err(EvidenceDeliveryError::Invalid(error)) => {
                    tracing::error!(error = %error, "durable certificate has no valid delivery encoding");
                    return Err(StatusCode::INTERNAL_SERVER_ERROR);
                }
                Err(error) => {
                    tracing::warn!(error = %error, "durable certificate awaits delivery enrollment retry");
                }
            }
        }
        // A receipt is also a durable relay promise for each configured
        // neighbor. Retrying after a crash is safe because enrollment is
        // idempotent and startup reconciles the support log.
        for addr in self.neighbors.permitted_addrs_for(
            &packet.operation.operation.subject.namespace_id,
            &packet.operation.declaration.key,
        ) {
            if let Err(error) = self
                .support_outbox
                .blocking_lock()
                .ensure_enqueued(addr, packet.clone())
            {
                tracing::warn!(error = %error, "durable support awaits delivery enrollment retry");
            }
        }
        Ok(SupportReceipt {
            commitment,
            already_present: outcome == RecordOutcome::Retry,
            certificate_ready,
        })
    }

    /// Local Authority admission retains the underlying error category so
    /// the issuance loop can retry disk failures and stop on invalid trust.
    pub fn record_authority_support_blocking(
        &self,
        packet: SupportPacket,
    ) -> Result<SupportReceipt, AuthorityTrustCheckError> {
        let commitment = packet.commitment();
        let registry = self.registry.read().map_err(|error| {
            AuthorityTrustCheckError::Permanent(format!("poisoned keyset registry: {error}"))
        })?;
        let namespace = self.namespace.read().map_err(|error| {
            AuthorityTrustCheckError::Permanent(format!("poisoned namespace registry: {error}"))
        })?;
        if packet.operation.operation.subject.namespace_id != self.scope.namespace_id
            || !scope_applies_to_key(&namespace, &self.scope, &packet.operation.declaration.key)
        {
            return Err(AuthorityTrustCheckError::Permanent(
                "local Authority support is outside its scope".into(),
            ));
        }
        let digest = static_read_trust_digest(&namespace, &self.scope, &registry, &self.owned)
            .map_err(AuthorityTrustCheckError::Permanent)?;
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &self.scope,
            current_epoch: 0,
            epoch_config: &self.epoch_config,
            faults: self.faults,
        };
        let mut store = self
            .recovered_evidence(&verifier, digest)
            .map_err(authority_evidence_error)?;
        store
            .check_trust(&verifier, &digest)
            .map_err(authority_evidence_error)?;
        let mut supports = self
            .recovered_supports(&registry)
            .map_err(authority_support_error)?;
        let subject = packet.operation.operation.subject.clone();
        let outcome = supports
            .record_and_sync(packet.operation.clone(), packet.support.clone())
            .map_err(authority_support_error)?;
        let certificate = supports
            .assemble_certificate(&subject, self.faults)
            .map_err(authority_support_error)?;
        let receipt = crate::operation::OperationReceipt::from(&packet.operation);
        let mut evidence_available = true;
        if let Some(certificate) = certificate
            && let Err(error) = store.record_and_sync(certificate, &verifier, digest)
        {
            if !retryable_evidence_error(&error) {
                return Err(authority_evidence_error(error));
            }
            tracing::warn!(error = %error, "durable Authority support awaits certificate assembly retry");
            evidence_available = false;
        }
        let certificate_ready = if evidence_available {
            let full_ready = store
                .get_verified(&subject, &verifier, digest)
                .map_err(authority_evidence_error)?
                .is_some_and(|proof| proof.bound() == &packet.operation);
            let compact_ready = store
                .get_verified_compact(&receipt, &verifier, digest)
                .map_err(authority_evidence_error)?
                .is_some();
            full_ready || compact_ready
        } else {
            false
        };
        if certificate_ready {
            let retained =
                retained_certificate_for_authority(&store, &packet.operation, &verifier, digest)?;
            match self.enqueue_evidence_delivery(&retained) {
                Ok(()) => {}
                Err(EvidenceDeliveryError::Invalid(error)) => {
                    return Err(AuthorityTrustCheckError::Permanent(error));
                }
                Err(error) => {
                    tracing::warn!(error = %error, "durable certificate awaits delivery enrollment retry");
                }
            }
        }
        for addr in self.neighbors.permitted_addrs_for(
            &packet.operation.operation.subject.namespace_id,
            &packet.operation.declaration.key,
        ) {
            if let Err(error) = self
                .support_outbox
                .blocking_lock()
                .ensure_enqueued(addr, packet.clone())
            {
                tracing::warn!(error = %error, "durable Authority support awaits delivery enrollment retry");
            }
        }
        Ok(SupportReceipt {
            commitment,
            already_present: outcome == RecordOutcome::Retry,
            certificate_ready,
        })
    }
}

async fn receive_http(
    State(state): State<OperationCertificateState>,
    Json(certificate): Json<OperationMerkleCertificate>,
) -> Result<Json<CertificateReceipt>, StatusCode> {
    let digest = commitment(&certificate)?;
    tokio::task::spawn_blocking(move || {
        let registry = state
            .registry
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let namespace = state
            .namespace
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if certificate.operation.operation.subject.namespace_id != state.scope.namespace_id
            || !scope_applies_to_key(
                &namespace,
                &state.scope,
                &certificate.operation.declaration.key,
            )
        {
            if certificate.operation.operation.subject.namespace_id == state.scope.namespace_id
                && certificate
                    .operation
                    .declaration
                    .key
                    .starts_with(&state.scope.key_prefix)
            {
                drop(namespace);
                drop(registry);
                state.check_trust_blocking()?;
            }
            return Err(StatusCode::UNPROCESSABLE_ENTITY);
        }
        let policy_digest =
            static_read_trust_digest(&namespace, &state.scope, &registry, &state.owned)
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        // This opt-in mode pins one immutable keyset and scope. It does not
        // participate in dynamic epoch/key rotation; every operation proof
        // is accepted against the pinned registration epoch zero.
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &state.scope,
            current_epoch: 0,
            epoch_config: &state.epoch_config,
            faults: state.faults,
        };
        let operation = certificate.operation.clone();
        let mut store = state
            .recovered_evidence(&verifier, policy_digest)
            .map_err(store_status)?;
        let outcome = store
            .record_and_sync(certificate, &verifier, policy_digest)
            .map_err(store_status)?;
        let retained = retained_certificate_for_body(&store, &operation, &verifier, policy_digest)?;
        match state.enqueue_evidence_delivery(&retained) {
            Ok(()) => {}
            Err(EvidenceDeliveryError::Invalid(error)) => {
                tracing::error!(error = %error, "durable certificate has no valid delivery encoding");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
            Err(error) => {
                tracing::warn!(error = %error, "durable certificate awaits delivery enrollment retry");
            }
        }
        Ok(Json(CertificateReceipt {
            commitment: digest,
            already_present: outcome == RecordOutcome::Retry,
        }))
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertifiedOperationResponse {
    pub result: CertifiedReadResult,
    pub basis: Vec<OperationMerkleCertificate>,
    pub local_body_seen: bool,
}

/// A signed Create can establish Disabled at a transit node without an
/// application body. Publish that policy result while keeping all other
/// non-owner read states behind the owned-prefix boundary.
pub(crate) fn non_owner_certified_result(
    inbox: &mut OperationInbox,
    namespace_id: &str,
    key: &str,
) -> Result<CertifiedOperationResponse, StatusCode> {
    inbox
        .recover_if_poisoned()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let result = if inbox.certified_read(&[], namespace_id, key).result
        == CertifiedReadResult::NotTargeted
    {
        CertifiedReadResult::NotTargeted
    } else {
        CertifiedReadResult::Unknown
    };
    Ok(CertifiedOperationResponse {
        result,
        basis: Vec::new(),
        local_body_seen: false,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationCertificationStatus {
    Unknown,
    PolicyUnknown,
    NotTarget,
    Pending,
    Certified,
}

/// A ledger receipt preserves certification status without exposing the
/// retired value through the old full-certificate response field. The proof
/// has already been reverified by the pinned evidence store.
fn status_certificates(
    verified: Option<VerifiedOperation>,
    body: Option<&BoundOperation>,
    receipt: Option<&crate::operation::OperationReceipt>,
) -> (
    Option<OperationMerkleCertificate>,
    Option<CompactOperationCertificate>,
) {
    let Some(proof) = verified else {
        return (None, None);
    };
    if body.is_some_and(|body| proof.bound() == body) {
        return (Some(proof.certificate().clone()), None);
    }
    if receipt
        .is_some_and(|receipt| *receipt == crate::operation::OperationReceipt::from(proof.bound()))
    {
        return (None, Some(proof.certificate().compact()));
    }
    (None, None)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationStatusResponse {
    pub status: OperationCertificationStatus,
    pub certificate: Option<OperationMerkleCertificate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compact_certificate: Option<CompactOperationCertificate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_receipt: Option<crate::operation::OperationReceipt>,
}

#[derive(Clone)]
pub struct CertifiedSubmitState {
    pub relay: OperationRelay,
    pub certificates: OperationCertificateState,
}

#[derive(Debug, Deserialize)]
struct CertifiedSubmitQuery {
    #[serde(default = "default_certified_wait_ms")]
    wait_ms: u64,
}

fn default_certified_wait_ms() -> u64 {
    DEFAULT_CERTIFIED_WAIT_MS
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertifiedSubmitResponse {
    pub receipt: OperationReceipt,
    pub status: OperationCertificationStatus,
    pub certificate: Option<OperationMerkleCertificate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compact_certificate: Option<CompactOperationCertificate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_receipt: Option<crate::operation::OperationReceipt>,
}

async fn submit_certified_http(
    State(state): State<CertifiedSubmitState>,
    Query(query): Query<CertifiedSubmitQuery>,
    Json(packet): Json<OperationPacket>,
) -> Result<(StatusCode, Json<CertifiedSubmitResponse>), StatusCode> {
    if query.wait_ms > MAX_CERTIFIED_WAIT_MS
        || packet.signed.bound.declaration.mode != CertificationMode::Enabled
        || !state.certificates.scope.contains(&packet.signed.bound)
    {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }
    let certificates = state.certificates.clone();
    let relay = state.relay.clone();
    let key = packet.signed.bound.declaration.key.clone();
    let subject = packet.signed.bound.operation.subject.clone();
    let receipt = tokio::task::spawn_blocking(move || {
        let registry = certificates
            .registry
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let namespace = certificates
            .namespace
            .read()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        certificates.check_trust_with_snapshot_blocking(&registry, &namespace)?;
        if !scope_applies_to_key(&namespace, &certificates.scope, &key) {
            return Err(StatusCode::UNPROCESSABLE_ENTITY);
        }
        // Keep the trust snapshot pinned until the operation is durable.
        // Routed relay rechecks and pins the live namespace itself, while a
        // non-routed relay needs this guard through its durable submit.
        if relay.is_routed() {
            drop(namespace);
            let receipt = relay.submit_local_blocking(packet);
            drop(registry);
            receipt
        } else {
            let receipt = relay.submit_local_blocking(packet);
            drop(namespace);
            drop(registry);
            receipt
        }
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(query.wait_ms);
    loop {
        let certificates = state.certificates.clone();
        let subject = subject.clone();
        let status = tokio::task::spawn_blocking(move || certificates.status_for_blocking(subject))
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;
        if status.status == OperationCertificationStatus::Certified {
            return Ok((
                StatusCode::OK,
                Json(CertifiedSubmitResponse {
                    receipt,
                    status: status.status,
                    certificate: status.certificate,
                    compact_certificate: status.compact_certificate,
                    operation_receipt: status.operation_receipt,
                }),
            ));
        }
        if !matches!(
            status.status,
            OperationCertificationStatus::Pending | OperationCertificationStatus::PolicyUnknown
        ) {
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Ok((
                StatusCode::ACCEPTED,
                Json(CertifiedSubmitResponse {
                    receipt,
                    status: status.status,
                    certificate: None,
                    compact_certificate: None,
                    operation_receipt: None,
                }),
            ));
        }
        tokio::time::sleep((deadline - now).min(std::time::Duration::from_millis(100))).await;
    }
}

pub fn certified_submit_router(state: CertifiedSubmitState) -> Router {
    Router::new()
        .route(CERTIFIED_SUBMIT_PATH, post(submit_certified_http))
        .with_state(state)
}

pub fn public_operation_status_router(state: OperationCertificateState) -> Router {
    Router::new()
        .route(PUBLIC_OPERATION_STATUS_PATH, get(status_http))
        .route(PUBLIC_CERTIFIED_READ_PATH, get(read_http))
        .with_state(state)
}

/// Public observation for an operation node without a certificate verifier.
/// Disabled keys still have a definite policy result, and an enabled body's
/// operation ID remains pending. No certificate is inferred in this mode.
pub fn public_operation_uncertified_router(relay: OperationRelay) -> Router {
    Router::new()
        .route(PUBLIC_OPERATION_STATUS_PATH, get(uncertified_status_http))
        .route(PUBLIC_CERTIFIED_READ_PATH, get(uncertified_read_http))
        .with_state(relay)
}

async fn uncertified_status_http(
    State(relay): State<OperationRelay>,
    Path((namespace_id, writer, counter)): Path<(String, String, u64)>,
) -> Result<Json<OperationStatusResponse>, StatusCode> {
    tokio::task::spawn_blocking(move || {
        let subject = CrdtSubject {
            namespace_id,
            event_dot: EventDot {
                writer: NodeId(writer),
                counter,
            },
        };
        let inbox = relay.inbox();
        let mut inbox = inbox.blocking_lock();
        inbox
            .recover_if_poisoned()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let mode_and_key = inbox
            .get(&subject)
            .map(|body| (body.declaration.mode, body.declaration.key))
            .or_else(|| {
                inbox
                    .receipt(&subject)
                    .map(|receipt| (receipt.declaration.mode, receipt.declaration.key))
            });
        let policy_known = inbox.creation_policy_known(&subject);
        let status = match mode_and_key {
            None => OperationCertificationStatus::Unknown,
            Some((mode, key)) if !relay.owns_namespace_key(&subject.namespace_id, &key) => {
                if policy_known && mode == CertificationMode::Disabled {
                    OperationCertificationStatus::NotTarget
                } else {
                    OperationCertificationStatus::Unknown
                }
            }
            Some(_) if !policy_known => OperationCertificationStatus::PolicyUnknown,
            Some((CertificationMode::Disabled, _)) => OperationCertificationStatus::NotTarget,
            Some((CertificationMode::Enabled, _)) => OperationCertificationStatus::Pending,
        };
        Ok(Json(OperationStatusResponse {
            status,
            certificate: None,
            compact_certificate: None,
            operation_receipt: None,
        }))
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}

async fn uncertified_read_http(
    State(relay): State<OperationRelay>,
    Path((namespace_id, key)): Path<(String, String)>,
) -> Result<Json<CertifiedOperationResponse>, StatusCode> {
    tokio::task::spawn_blocking(move || {
        if !relay.owns_namespace_key(&namespace_id, &key) {
            let inbox = relay.inbox();
            let mut inbox = inbox.blocking_lock();
            return non_owner_certified_result(&mut inbox, &namespace_id, &key).map(Json);
        }
        let inbox = relay.inbox();
        let mut inbox = inbox.blocking_lock();
        inbox
            .recover_if_poisoned()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let read = inbox.certified_read(&[], &namespace_id, &key);
        Ok(Json(CertifiedOperationResponse {
            result: read.result,
            basis: Vec::new(),
            local_body_seen: read.local_body_seen,
        }))
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}

async fn status_http(
    State(state): State<OperationCertificateState>,
    Path((namespace_id, writer, counter)): Path<(String, String, u64)>,
) -> Result<Json<OperationStatusResponse>, StatusCode> {
    let subject = CrdtSubject {
        namespace_id,
        event_dot: EventDot {
            writer: NodeId(writer),
            counter,
        },
    };
    tokio::task::spawn_blocking(move || state.status_for_blocking(subject))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
}

async fn read_http(
    State(state): State<OperationCertificateState>,
    Path((namespace_id, key)): Path<(String, String)>,
) -> Result<Json<CertifiedOperationResponse>, StatusCode> {
    tokio::task::spawn_blocking(move || state.read_for_blocking(&namespace_id, &key).map(Json))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}

pub fn certificate_router(state: OperationCertificateState, token: String) -> Router {
    let link = state.link_auth.clone();
    Router::new()
        .route(
            CERTIFICATE_RECEIVE_PATH,
            post(receive_http).layer(DefaultBodyLimit::max(INTERNAL_OPERATION_BODY_LIMIT)),
        )
        .route(
            SUPPORT_RECEIVE_PATH,
            post(receive_support_http).layer(DefaultBodyLimit::max(INTERNAL_OPERATION_BODY_LIMIT)),
        )
        .route(CERTIFIED_READ_PATH, get(read_http))
        .route(OPERATION_STATUS_PATH, get(status_http))
        .layer(middleware::from_fn(move |request, next| {
            let link = link.clone();
            verify_evidence_link(request, next, link)
        }))
        .layer(middleware::from_fn(move |request, next| {
            let token = token.clone();
            require_bearer_token(request, next, token)
        }))
        .with_state(state)
}

async fn multi_receive_support_http(
    State(state): State<MultiOperationCertificateState>,
    Json(packet): Json<SupportPacket>,
) -> Result<Json<SupportReceipt>, StatusCode> {
    let bound = &packet.operation;
    let scope = select_scope_for_intake(
        &state,
        &bound.operation.subject.namespace_id,
        &bound.declaration.key,
    )
    .await?;
    receive_support_http(State(scope), Json(packet)).await
}

async fn multi_receive_http(
    State(state): State<MultiOperationCertificateState>,
    Json(certificate): Json<OperationMerkleCertificate>,
) -> Result<Json<CertificateReceipt>, StatusCode> {
    let bound = &certificate.operation;
    let scope = select_scope_for_intake(
        &state,
        &bound.operation.subject.namespace_id,
        &bound.declaration.key,
    )
    .await?;
    receive_http(State(scope), Json(certificate)).await
}

async fn multi_submit_certified_http(
    State((relay, state)): State<(OperationRelay, MultiOperationCertificateState)>,
    query: Query<CertifiedSubmitQuery>,
    Json(packet): Json<OperationPacket>,
) -> Result<(StatusCode, Json<CertifiedSubmitResponse>), StatusCode> {
    let bound = &packet.signed.bound;
    let scope = select_scope_for_intake(
        &state,
        &bound.operation.subject.namespace_id,
        &bound.declaration.key,
    )
    .await?;
    submit_certified_http(
        State(CertifiedSubmitState {
            relay,
            certificates: scope,
        }),
        query,
        Json(packet),
    )
    .await
}

async fn select_scope_for_intake(
    state: &MultiOperationCertificateState,
    namespace_id: &str,
    key: &str,
) -> Result<OperationCertificateState, StatusCode> {
    if let Some(scope) = state.select_for_key(namespace_id, key)? {
        return Ok(scope);
    }
    let state = state.clone();
    let namespace_id = namespace_id.to_owned();
    let key = key.to_owned();
    tokio::task::spawn_blocking(move || state.check_matching_trust_blocking(&namespace_id, &key))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;
    Err(StatusCode::UNPROCESSABLE_ENTITY)
}

async fn multi_status_http(
    State(state): State<MultiOperationCertificateState>,
    Path((namespace_id, writer, counter)): Path<(String, String, u64)>,
) -> Result<Json<OperationStatusResponse>, StatusCode> {
    let subject = CrdtSubject {
        namespace_id,
        event_dot: EventDot {
            writer: NodeId(writer),
            counter,
        },
    };
    let state_for_lookup = state.clone();
    let scope = tokio::task::spawn_blocking(move || -> Result<_, StatusCode> {
        let mut inbox = state_for_lookup.first().inbox.blocking_lock();
        inbox
            .recover_if_poisoned()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let mode_and_key = inbox
            .get(&subject)
            .map(|body| (body.declaration.mode, body.declaration.key))
            .or_else(|| {
                inbox
                    .receipt(&subject)
                    .map(|receipt| (receipt.declaration.mode, receipt.declaration.key))
            });
        let policy_known = inbox.creation_policy_known(&subject);
        drop(inbox);
        let Some((mode, key)) = mode_and_key else {
            return Ok(Err(OperationCertificationStatus::Unknown));
        };
        let checked = state_for_lookup.checked_scope_for_key(&subject.namespace_id, &key)?;
        if !state_for_lookup
            .first()
            .owned
            .owns_in_namespace(&subject.namespace_id, &key)
        {
            let status = if policy_known && mode == CertificationMode::Disabled {
                OperationCertificationStatus::NotTarget
            } else {
                OperationCertificationStatus::Unknown
            };
            drop(checked);
            return Ok(Err(status));
        }
        if mode == CertificationMode::Disabled {
            let status = if policy_known {
                OperationCertificationStatus::NotTarget
            } else {
                OperationCertificationStatus::PolicyUnknown
            };
            drop(checked);
            return Ok(Err(status));
        }
        let scope = checked.selected.clone();
        let result = scope
            .ok_or(if policy_known {
                OperationCertificationStatus::Pending
            } else {
                OperationCertificationStatus::PolicyUnknown
            })
            .map(|scope| (scope, subject));
        drop(checked);
        Ok(result)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;
    match scope {
        Ok((scope, subject)) => {
            tokio::task::spawn_blocking(move || scope.status_for_blocking(subject))
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .map(Json)
        }
        Err(status) => Ok(Json(OperationStatusResponse {
            status,
            certificate: None,
            compact_certificate: None,
            operation_receipt: None,
        })),
    }
}

async fn multi_read_http(
    State(state): State<MultiOperationCertificateState>,
    Path((namespace_id, key)): Path<(String, String)>,
) -> Result<Json<CertifiedOperationResponse>, StatusCode> {
    tokio::task::spawn_blocking(move || state.read_for_key_blocking(&namespace_id, &key).map(Json))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}

pub fn multi_certificate_router(state: MultiOperationCertificateState, token: String) -> Router {
    let link = state.first().link_auth.clone();
    Router::new()
        .route(
            CERTIFICATE_RECEIVE_PATH,
            post(multi_receive_http).layer(DefaultBodyLimit::max(INTERNAL_OPERATION_BODY_LIMIT)),
        )
        .route(
            SUPPORT_RECEIVE_PATH,
            post(multi_receive_support_http)
                .layer(DefaultBodyLimit::max(INTERNAL_OPERATION_BODY_LIMIT)),
        )
        .route(CERTIFIED_READ_PATH, get(multi_read_http))
        .route(OPERATION_STATUS_PATH, get(multi_status_http))
        .layer(middleware::from_fn(move |request, next| {
            let link = link.clone();
            verify_evidence_link(request, next, link)
        }))
        .layer(middleware::from_fn(move |request, next| {
            let token = token.clone();
            require_bearer_token(request, next, token)
        }))
        .with_state(state)
}

pub fn multi_public_operation_status_router(state: MultiOperationCertificateState) -> Router {
    Router::new()
        .route(PUBLIC_OPERATION_STATUS_PATH, get(multi_status_http))
        .route(PUBLIC_CERTIFIED_READ_PATH, get(multi_read_http))
        .with_state(state)
}

pub fn multi_certified_submit_router(
    relay: OperationRelay,
    state: MultiOperationCertificateState,
) -> Router {
    Router::new()
        .route(CERTIFIED_SUBMIT_PATH, post(multi_submit_certified_http))
        .with_state((relay, state))
}

async fn verify_evidence_link(
    request: axum::extract::Request,
    next: axum::middleware::Next,
    link: Option<OperationLinkAuth>,
) -> axum::response::Response {
    let Some(link) = link else {
        return next.run(request).await;
    };
    let path = request.uri().path().to_owned();
    if path != SUPPORT_RECEIVE_PATH && path != CERTIFICATE_RECEIVE_PATH {
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
    let Some(digest) = link.verify_request(&sender, &receiver, "POST", &path, &body, &signature)
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let response = next
        .run(axum::http::Request::from_parts(parts, Body::from(body)))
        .await;
    let (mut parts, body) = response.into_parts();
    let Ok(body) = to_bytes(body, INTERNAL_OPERATION_BODY_LIMIT).await else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let signature = link.sign_response(
        &sender,
        "POST",
        &path,
        &digest,
        parts.status.as_u16(),
        &body,
    );
    parts.headers.insert(
        LINK_SENDER,
        HeaderValue::from_str(&link.self_id().0).expect("validated link node ID"),
    );
    parts.headers.insert(
        LINK_RECEIVER,
        HeaderValue::from_str(&sender.0).expect("validated link node ID"),
    );
    parts.headers.insert(
        LINK_SIGNATURE,
        HeaderValue::from_str(&signature).expect("hex signature"),
    );
    parts.headers.insert(
        LINK_REQUEST_DIGEST,
        HeaderValue::from_str(&hex::encode(digest)).expect("hex digest"),
    );
    axum::http::Response::from_parts(parts, Body::from(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_trust_check_retries_io_but_stops_on_invalid_durable_state() {
        assert!(matches!(
            authority_evidence_error(StaticTrustError::Io(std::io::Error::other("temporary"))),
            AuthorityTrustCheckError::Retryable(_)
        ));
        assert!(matches!(
            authority_evidence_error(StaticTrustError::TrustChanged),
            AuthorityTrustCheckError::Permanent(_)
        ));
        assert!(matches!(
            authority_support_error(SupportStoreError::Io(std::io::Error::other("temporary"))),
            AuthorityTrustCheckError::Retryable(_)
        ));
        assert!(matches!(
            authority_support_error(SupportStoreError::Invalid("corrupt log".into())),
            AuthorityTrustCheckError::Permanent(_)
        ));
    }
    use crate::api::operation_view::OperationRead;
    use crate::authority::certificate::KeysetVersion;
    use crate::authority::operation_admission::{OperationAdmissionPolicy, admit_for_signing};
    use crate::authority::operation_merkle::{
        OperationMerkleTree, OperationRootSupport, SignedOperationRoot,
    };
    use crate::authority::writer_auth::{SignedBoundOperation, SignedCreation};
    use crate::control_plane::system_namespace::AuthorityDefinition;
    use crate::hlc::HlcTimestamp;
    use crate::network::operation_outbox::OperationOutbox;
    use crate::network::operation_transport::{
        OperationAdmission, OperationPacket, OperationViewResponse, submit_router,
        unified_operation_view_router,
    };
    use crate::operation::{
        BoundOperation, CandidateKvAction, CertificationMode, CrdtSubject, Declaration, EventDot,
        OperationLeaf, OperationLedger,
    };
    use crate::placement::PlacementPolicy;
    use crate::types::{KeyRange, NodeId, PolicyVersion, Tag};
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use ed25519_dalek::SigningKey;
    use tower::ServiceExt;

    #[tokio::test]
    async fn evidence_link_requires_signed_neighbor_and_signs_ack() {
        use ed25519_dalek::SigningKey;
        let a_key = SigningKey::from_bytes(&[1; 32]).verifying_key();
        let b_key = SigningKey::from_bytes(&[2; 32]).verifying_key();
        let a_neighbors = OperationNeighborManifest::from_json(
            r#"[{"node_id":"b","addr":"b:1234","key_prefixes":[""]}]"#,
            &NodeId("a".into()),
        )
        .unwrap();
        let b_neighbors = OperationNeighborManifest::from_json(
            r#"[{"node_id":"a","addr":"a:1234","key_prefixes":[""]}]"#,
            &NodeId("b".into()),
        )
        .unwrap();
        let a = OperationLinkAuth::from_manifest(
            NodeId("a".into()),
            &hex::encode([1; 32]),
            &serde_json::json!([{"node_id":"b","public_key":hex::encode(b_key.as_bytes())}])
                .to_string(),
            &a_neighbors,
        )
        .unwrap();
        let b = OperationLinkAuth::from_manifest(
            NodeId("b".into()),
            &hex::encode([2; 32]),
            &serde_json::json!([{"node_id":"a","public_key":hex::encode(a_key.as_bytes())}])
                .to_string(),
            &b_neighbors,
        )
        .unwrap();
        let app = Router::new()
            .route(SUPPORT_RECEIVE_PATH, post(|| async { "ack" }))
            .layer(middleware::from_fn(move |request, next| {
                verify_evidence_link(request, next, Some(b.clone()))
            }));
        let unsigned = axum::http::Request::post(SUPPORT_RECEIVE_PATH)
            .body(Body::from("request"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(unsigned).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let (digest, signature) = a.sign_request(
            &NodeId("b".into()),
            "POST",
            SUPPORT_RECEIVE_PATH,
            b"request",
        );
        let signed = axum::http::Request::post(SUPPORT_RECEIVE_PATH)
            .header(LINK_SENDER, "a")
            .header(LINK_RECEIVER, "b")
            .header(LINK_SIGNATURE, signature)
            .body(Body::from("request"))
            .unwrap();
        let response = app.oneshot(signed).await.unwrap();
        let signature = response
            .headers()
            .get(LINK_SIGNATURE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert!(a.verify_response(
            &NodeId("b".into()),
            a.self_id(),
            "POST",
            SUPPORT_RECEIVE_PATH,
            &digest,
            200,
            &body,
            &signature
        ));
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
    impl OperationAdmission for Allow {
        fn admit(&self, _: &SignedBoundOperation) -> Result<(), String> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn no_scope_public_status_and_disabled_read_use_durable_local_policy() {
        let temp = tempfile::tempdir().unwrap();
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(temp.path().join("inbox"), Arc::new(Allow)).unwrap(),
        ));
        let relay = OperationRelay::new(
            inbox.clone(),
            Arc::new(Mutex::new(
                OperationOutbox::open(temp.path().join("outbox")).unwrap(),
            )),
            OperationNeighborManifest::from_json("[]", &NodeId("self".into())).unwrap(),
            OperationOwnedPrefixes::from_json(r#"[{"namespace_id":"ns","key_prefix":"owned/"}]"#)
                .unwrap(),
        )
        .await
        .unwrap();
        let key = SigningKey::from_bytes(&[9; 32]);
        for (namespace_id, counter, item, mode) in [
            ("ns", 1, "owned/plain", CertificationMode::Disabled),
            ("ns", 2, "owned/waiting", CertificationMode::Enabled),
            ("ns", 3, "elsewhere/plain", CertificationMode::Disabled),
            ("other-ns", 4, "owned/plain", CertificationMode::Disabled),
            ("ns", 7, "elsewhere/waiting", CertificationMode::Enabled),
        ] {
            let subject = CrdtSubject {
                namespace_id: namespace_id.into(),
                event_dot: EventDot {
                    writer: NodeId("writer".into()),
                    counter,
                },
            };
            let bound = BoundOperation {
                operation: OperationLeaf {
                    subject: subject.clone(),
                    action: CandidateKvAction::Create {
                        key: item.into(),
                        value_bytes: b"value".to_vec(),
                        timestamp: HlcTimestamp {
                            physical: counter,
                            logical: 0,
                            node_id: "writer".into(),
                        },
                    },
                },
                declaration: Declaration {
                    creation_id: subject,
                    key: item.into(),
                    mode,
                },
            };
            let creation = SignedCreation::sign(&bound, &key).unwrap();
            let signed = SignedBoundOperation::sign(bound, &key, creation).unwrap();
            inbox
                .lock()
                .await
                .receive(OperationPacket { signed })
                .unwrap();
        }
        let witness_id = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 8,
            },
        };
        let witness_body = BoundOperation {
            operation: OperationLeaf {
                subject: witness_id.clone(),
                action: CandidateKvAction::Create {
                    key: "elsewhere/policy".into(),
                    value_bytes: b"policy-only value".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 8,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: witness_id.clone(),
                key: "elsewhere/policy".into(),
                mode: CertificationMode::Disabled,
            },
        };
        let creation = SignedCreation::sign(&witness_body, &key).unwrap();
        inbox
            .lock()
            .await
            .receive_policy_witness(OperationPacket {
                signed: SignedBoundOperation::sign(witness_body, &key, creation).unwrap(),
            })
            .unwrap();
        assert!(inbox.lock().await.get(&witness_id).is_none());
        let create_id = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 5,
            },
        };
        let create_body = BoundOperation {
            operation: OperationLeaf {
                subject: create_id.clone(),
                action: CandidateKvAction::Create {
                    key: "owned/late".into(),
                    value_bytes: b"first".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 5,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: create_id,
                key: "owned/late".into(),
                mode: CertificationMode::Disabled,
            },
        };
        let compact = SignedCreation::sign(&create_body, &key).unwrap();
        let mut later_body = create_body.clone();
        later_body.operation.subject.event_dot.counter = 6;
        later_body.operation.action = CandidateKvAction::Add {
            key: "owned/late".into(),
            value_bytes: b"second".to_vec(),
            timestamp: HlcTimestamp {
                physical: 6,
                logical: 0,
                node_id: "writer".into(),
            },
        };
        inbox
            .lock()
            .await
            .receive(OperationPacket {
                signed: SignedBoundOperation::sign(later_body, &key, compact.clone()).unwrap(),
            })
            .unwrap();
        let app = public_operation_uncertified_router(relay.clone());
        let view = crate::network::operation_transport::unified_operation_view_router(relay, None);
        for (counter, expected) in [
            (1, OperationCertificationStatus::NotTarget),
            (2, OperationCertificationStatus::Pending),
            (3, OperationCertificationStatus::NotTarget),
            (4, OperationCertificationStatus::Unknown),
            (7, OperationCertificationStatus::Unknown),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/api/operations/status/ns/writer/{counter}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), 4096).await.unwrap();
            let status: OperationStatusResponse = serde_json::from_slice(&body).unwrap();
            assert_eq!(status.status, expected);
            assert!(status.certificate.is_none());
        }
        let status_request = || {
            Request::builder()
                .uri("/api/operations/status/ns/writer/6")
                .body(Body::empty())
                .unwrap()
        };
        let read_request = || {
            Request::builder()
                .uri("/api/operations/certified-read/ns/owned/late")
                .body(Body::empty())
                .unwrap()
        };
        let response = app.clone().oneshot(status_request()).await.unwrap();
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let status: OperationStatusResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.status, OperationCertificationStatus::PolicyUnknown);
        let response = app.clone().oneshot(read_request()).await.unwrap();
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let read: CertifiedOperationResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(read.result, CertifiedReadResult::PolicyUnknown);
        inbox
            .lock()
            .await
            .receive(OperationPacket {
                signed: SignedBoundOperation::sign(create_body, &key, compact).unwrap(),
            })
            .unwrap();
        let response = app.clone().oneshot(status_request()).await.unwrap();
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let status: OperationStatusResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.status, OperationCertificationStatus::NotTarget);
        let response = app.clone().oneshot(read_request()).await.unwrap();
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let read: CertifiedOperationResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(read.result, CertifiedReadResult::NotTargeted);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/operations/certified-read/ns/owned/plain")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let read: CertifiedOperationResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(read.result, CertifiedReadResult::NotTargeted);
        assert!(read.basis.is_empty());
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/operations/certified-read/ns/owned/waiting")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let read: CertifiedOperationResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(read.result, CertifiedReadResult::NoCertifiedInformation);
        assert!(read.local_body_seen);
        assert!(read.basis.is_empty());
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/operations/certified-read/ns/elsewhere/plain")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let read: CertifiedOperationResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(read.result, CertifiedReadResult::NotTargeted);
        for (key, expected) in [
            ("elsewhere/policy", CertifiedReadResult::NotTargeted),
            ("elsewhere/waiting", CertifiedReadResult::Unknown),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/api/operations/certified-read/ns/{key}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), 4096).await.unwrap();
            let read: CertifiedOperationResponse = serde_json::from_slice(&body).unwrap();
            assert_eq!(read.result, expected);
            assert!(read.basis.is_empty());
            assert!(!read.local_body_seen);
        }
        let response = view
            .oneshot(
                Request::builder()
                    .uri("/api/operations/view/ns/elsewhere/policy?kind=certified")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let read: crate::network::operation_transport::OperationViewResponse =
            serde_json::from_slice(&body).unwrap();
        assert!(matches!(
            read,
            crate::network::operation_transport::OperationViewResponse::Certified(
                CertifiedOperationResponse {
                    result: CertifiedReadResult::NotTargeted,
                    ..
                }
            )
        ));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/operations/certified-read/other-ns/owned/plain")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let read: CertifiedOperationResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(read.result, CertifiedReadResult::NotTargeted);
    }

    fn fixture(replica_count: usize, reversed_tags: bool) -> (SystemNamespace, KeysetRegistry) {
        let mut namespace = SystemNamespace::new();
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "orders/".into(),
            },
            authority_nodes: vec![NodeId("auth".into())],
            auto_generated: false,
        });
        let mut policy = PlacementPolicy::new(
            PolicyVersion(1),
            KeyRange {
                prefix: "orders/".into(),
            },
            replica_count,
        )
        .with_certified(true);
        let tags = if reversed_tags {
            ["b", "a"]
        } else {
            ["a", "b"]
        };
        for tag in tags {
            policy.required_tags.insert(Tag(tag.into()));
        }
        namespace.set_placement_policy(policy).unwrap();
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![(
                    NodeId("auth".into()),
                    SigningKey::from_bytes(&[7; 32]).verifying_key(),
                )],
            )
            .unwrap();
        (namespace, registry)
    }

    #[tokio::test]
    async fn reconciliation_enrolls_durable_support_and_certificate_without_restart() {
        let temp = tempfile::tempdir().unwrap();
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "orders/".into(),
        };
        let (namespace, registry) = fixture(1, false);
        let config = EpochConfig::default();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 0,
        };
        let owned = OperationOwnedPrefixes::from_json("[]").unwrap();
        let digest = static_read_trust_digest(&namespace, &scope, &registry, &owned).unwrap();
        let state = OperationCertificateState {
            inbox: Arc::new(Mutex::new(
                OperationInbox::open(temp.path().join("inbox"), Arc::new(Allow)).unwrap(),
            )),
            store: Arc::new(Mutex::new(
                StaticOperationEvidenceStore::open(temp.path().join("evidence"), &verifier, digest)
                    .unwrap(),
            )),
            supports: Arc::new(Mutex::new(
                OperationSupportStore::open(
                    temp.path().join("supports"),
                    &registry,
                    &scope,
                    &config,
                )
                .unwrap(),
            )),
            support_outbox: Arc::new(Mutex::new(
                SupportOutbox::open(temp.path().join("support-outbox")).unwrap(),
            )),
            certificate_outbox: Arc::new(Mutex::new(
                CertificateOutbox::open(temp.path().join("certificate-outbox")).unwrap(),
            )),
            neighbors: OperationNeighborManifest::from_json(
                r#"[{"node_id":"next","addr":"next:1234","key_prefixes":["orders/"]}]"#,
                &NodeId("self".into()),
            )
            .unwrap(),
            owned,
            registry: Arc::new(RwLock::new(registry.clone())),
            namespace: Arc::new(RwLock::new(namespace)),
            scope: scope.clone(),
            epoch_config: config.clone(),
            faults: 0,
            link_auth: None,
        };
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
                    key: "orders/item".into(),
                    value_bytes: b"value".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 1,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject.clone(),
                key: "orders/item".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let mut ledger = OperationLedger::open(temp.path().join("admitted")).unwrap();
        let signable = admit_for_signing(&Allow, &mut ledger, bound.clone()).unwrap();
        let tree = OperationMerkleTree::from_signable(scope.clone(), &[signable]).unwrap();
        let support = OperationRootSupport {
            signed_root: SignedOperationRoot::sign(
                &tree,
                KeysetVersion(1),
                NodeId("auth".into()),
                &SigningKey::from_bytes(&[7; 32]),
            ),
            proof: tree.proof(&subject).unwrap(),
        };
        state
            .supports
            .lock()
            .await
            .record_and_sync(bound.clone(), support.clone())
            .unwrap();
        assert_eq!(state.support_outbox.lock().await.pending().count(), 0);
        assert_eq!(state.certificate_outbox.lock().await.pending().count(), 0);

        // Certificate persistence can fail for one subject after the support
        // itself is durable. Its packet still needs independent enrollment.
        let failed_reconcile = OperationCertificateState {
            store: Arc::new(Mutex::new(
                StaticOperationEvidenceStore::open(
                    temp.path().join("failed-reconcile-evidence"),
                    &verifier,
                    digest,
                )
                .unwrap(),
            )),
            support_outbox: Arc::new(Mutex::new(
                SupportOutbox::open(temp.path().join("failed-reconcile-support-outbox")).unwrap(),
            )),
            certificate_outbox: Arc::new(Mutex::new(
                CertificateOutbox::open(temp.path().join("failed-reconcile-certificate-outbox"))
                    .unwrap(),
            )),
            ..state.clone()
        };
        failed_reconcile
            .store
            .lock()
            .await
            .fail_next_append_for_test();
        let failed_attempt = failed_reconcile.clone();
        assert!(
            tokio::task::spawn_blocking(
                move || failed_attempt.reconcile_evidence_delivery_blocking()
            )
            .await
            .unwrap()
            .is_err()
        );
        assert_eq!(
            failed_reconcile
                .support_outbox
                .lock()
                .await
                .pending()
                .count(),
            1
        );

        let packet = SupportPacket {
            operation: bound.clone(),
            support: support.clone(),
        };
        state.store.lock().await.fail_next_append_for_test();
        let isolated = OperationCertificateState {
            support_outbox: Arc::new(Mutex::new(
                SupportOutbox::open(temp.path().join("isolated-support-outbox")).unwrap(),
            )),
            certificate_outbox: Arc::new(Mutex::new(
                CertificateOutbox::open(temp.path().join("isolated-certificate-outbox")).unwrap(),
            )),
            neighbors: OperationNeighborManifest::from_json("[]", &NodeId("self".into())).unwrap(),
            ..state.clone()
        };
        let injected_state = isolated.clone();
        let injected_packet = packet.clone();
        let pending_receipt = tokio::task::spawn_blocking(move || {
            injected_state.record_support_blocking(injected_packet)
        })
        .await
        .unwrap()
        .unwrap();
        assert!(!pending_receipt.certificate_ready);
        tokio::task::spawn_blocking(move || isolated.reconcile_evidence_delivery_blocking())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.certificate_outbox.lock().await.pending().count(), 0);

        let certificate = state
            .supports
            .lock()
            .await
            .assemble_certificate(&subject, 0)
            .unwrap()
            .unwrap();
        let certificate_outbox_path = temp.path().join("certificate-outbox");
        let displaced_certificate_path = temp.path().join("certificate-outbox-displaced");
        std::fs::rename(&certificate_outbox_path, &displaced_certificate_path).unwrap();
        std::fs::write(&certificate_outbox_path, b"temporary obstruction").unwrap();
        let certificate_receipt = receive_http(State(state.clone()), Json(certificate.clone()))
            .await
            .unwrap()
            .0;
        assert_eq!(
            certificate_receipt.commitment,
            commitment(&certificate).unwrap()
        );
        assert_eq!(state.certificate_outbox.lock().await.pending().count(), 0);
        std::fs::remove_file(&certificate_outbox_path).unwrap();
        std::fs::rename(&displaced_certificate_path, &certificate_outbox_path).unwrap();
        let empty_supports = {
            let registry = state.registry.read().unwrap();
            OperationSupportStore::open(
                temp.path().join("empty-supports"),
                &registry,
                &state.scope,
                &state.epoch_config,
            )
            .unwrap()
        };
        let certificate_state = OperationCertificateState {
            supports: Arc::new(Mutex::new(empty_supports)),
            ..state.clone()
        };
        tokio::task::spawn_blocking(move || {
            certificate_state.reconcile_evidence_delivery_blocking()
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(state.certificate_outbox.lock().await.pending().count(), 1);

        let outbox_path = temp.path().join("support-outbox");
        let displaced_path = temp.path().join("support-outbox-displaced");
        std::fs::rename(&outbox_path, &displaced_path).unwrap();
        std::fs::write(&outbox_path, b"temporary obstruction").unwrap();
        let public_state = state.clone();
        let public_packet = packet.clone();
        let public_receipt = tokio::task::spawn_blocking(move || {
            public_state.record_support_blocking(public_packet)
        })
        .await
        .unwrap()
        .unwrap();
        assert!(public_receipt.certificate_ready);
        let blocked_state = state.clone();
        let blocked_packet = packet.clone();
        let receipt = tokio::task::spawn_blocking(move || {
            blocked_state.record_authority_support_blocking(blocked_packet)
        })
        .await
        .unwrap()
        .unwrap();
        assert!(receipt.certificate_ready);
        std::fs::remove_file(&outbox_path).unwrap();
        std::fs::rename(&displaced_path, &outbox_path).unwrap();
        assert_eq!(state.support_outbox.lock().await.pending().count(), 0);

        let state_for_reconcile = state.clone();
        tokio::task::spawn_blocking(move || {
            state_for_reconcile.reconcile_evidence_delivery_blocking()
        })
        .await
        .unwrap()
        .unwrap();
        let sent_support = state
            .support_outbox
            .lock()
            .await
            .pending()
            .next()
            .cloned()
            .unwrap();
        assert_eq!(sent_support.peer_addr, "next:1234");
        assert_eq!(sent_support.packet, packet);
        let sent_certificate = state
            .certificate_outbox
            .lock()
            .await
            .pending()
            .next()
            .cloned()
            .unwrap();
        assert_eq!(sent_certificate.peer_addr, "next:1234");
        assert_eq!(sent_certificate.packet.operation, bound);
        assert_eq!(sent_certificate.packet.supports.len(), 1);

        // A certificate-only relay can reconstruct support sends from its
        // durable evidence, even when its local support store is empty.
        let support_size = serde_json::to_vec(&packet).unwrap().len();
        assert!(serde_json::to_vec(&sent_certificate.packet).unwrap().len() > support_size);
        assert!(split_certificate_at_limit(&sent_certificate.packet, support_size - 1).is_err());
        let fallback_state = OperationCertificateState {
            supports: Arc::new(Mutex::new(
                OperationSupportStore::open(
                    temp.path().join("certificate-only-supports"),
                    &state.registry.read().unwrap(),
                    &state.scope,
                    &state.epoch_config,
                )
                .unwrap(),
            )),
            support_outbox: Arc::new(Mutex::new(
                SupportOutbox::open(temp.path().join("certificate-only-support-outbox")).unwrap(),
            )),
            certificate_outbox: Arc::new(Mutex::new(
                CertificateOutbox::open(temp.path().join("certificate-only-certificate-outbox"))
                    .unwrap(),
            )),
            ..state.clone()
        };
        assert_eq!(fallback_state.supports.lock().await.packets().count(), 0);
        let recovery = fallback_state.clone();
        tokio::task::spawn_blocking(move || {
            recovery.reconcile_evidence_delivery_at_limit(support_size)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            fallback_state
                .certificate_outbox
                .lock()
                .await
                .pending()
                .count(),
            0
        );
        let fallback_entry = fallback_state
            .support_outbox
            .lock()
            .await
            .pending()
            .next()
            .cloned()
            .unwrap();
        assert_eq!(fallback_entry.packet, packet);
        assert_eq!(fallback_entry.peer_addr, "next:1234");
        assert!(!evidence_delivery_ready_at_limit(
            &sent_certificate.packet,
            &fallback_state.neighbors,
            &*fallback_state.certificate_outbox.lock().await,
            &*fallback_state.support_outbox.lock().await,
            support_size,
        ));
        fallback_state
            .support_outbox
            .lock()
            .await
            .record_receipt(
                fallback_entry.id,
                SupportReceipt {
                    commitment: fallback_entry.packet.commitment(),
                    already_present: false,
                    certificate_ready: true,
                },
            )
            .unwrap();
        assert!(evidence_delivery_ready_at_limit(
            &sent_certificate.packet,
            &fallback_state.neighbors,
            &*fallback_state.certificate_outbox.lock().await,
            &*fallback_state.support_outbox.lock().await,
            support_size,
        ));

        // One certificate that cannot fit the selected transport limit must
        // not prevent a later independent certificate from being enrolled.
        let make_certificate =
            |counter: u64, key: &str, value: Vec<u8>, ledger: &mut OperationLedger| {
                let subject = CrdtSubject {
                    namespace_id: "ns".into(),
                    event_dot: EventDot {
                        writer: NodeId("writer".into()),
                        counter,
                    },
                };
                let operation = BoundOperation {
                    operation: OperationLeaf {
                        subject: subject.clone(),
                        action: CandidateKvAction::Create {
                            key: key.into(),
                            value_bytes: value,
                            timestamp: HlcTimestamp {
                                physical: counter,
                                logical: 0,
                                node_id: "writer".into(),
                            },
                        },
                    },
                    declaration: Declaration {
                        creation_id: subject.clone(),
                        key: key.into(),
                        mode: CertificationMode::Enabled,
                    },
                };
                let signable = admit_for_signing(&Allow, ledger, operation.clone()).unwrap();
                let tree =
                    OperationMerkleTree::from_signable(state.scope.clone(), &[signable]).unwrap();
                let support = OperationRootSupport {
                    signed_root: SignedOperationRoot::sign(
                        &tree,
                        KeysetVersion(1),
                        NodeId("auth".into()),
                        &SigningKey::from_bytes(&[7; 32]),
                    ),
                    proof: tree.proof(&subject).unwrap(),
                };
                OperationMerkleCertificate {
                    operation,
                    supports: vec![support],
                }
            };
        let too_large = make_certificate(2, "orders/large", vec![b'x'; 100], &mut ledger);
        let later_small = make_certificate(3, "orders/x", Vec::new(), &mut ledger);
        let limit = serde_json::to_vec(&later_small).unwrap().len();
        assert!(
            serde_json::to_vec(&SupportPacket {
                operation: too_large.operation.clone(),
                support: too_large.supports[0].clone(),
            })
            .unwrap()
            .len()
                > limit
        );
        state
            .store
            .lock()
            .await
            .record_and_sync(too_large, &verifier, digest)
            .unwrap();
        state
            .store
            .lock()
            .await
            .record_and_sync(later_small.clone(), &verifier, digest)
            .unwrap();
        let independent = OperationCertificateState {
            supports: Arc::new(Mutex::new(
                OperationSupportStore::open(
                    temp.path().join("independent-supports"),
                    &state.registry.read().unwrap(),
                    &state.scope,
                    &state.epoch_config,
                )
                .unwrap(),
            )),
            support_outbox: Arc::new(Mutex::new(
                SupportOutbox::open(temp.path().join("independent-support-outbox")).unwrap(),
            )),
            certificate_outbox: Arc::new(Mutex::new(
                CertificateOutbox::open(temp.path().join("independent-certificate-outbox"))
                    .unwrap(),
            )),
            ..state.clone()
        };
        let recovered = independent.clone();
        assert!(
            tokio::task::spawn_blocking(
                move || recovered.reconcile_evidence_delivery_at_limit(limit)
            )
            .await
            .unwrap()
            .is_err()
        );
        assert!(
            independent
                .certificate_outbox
                .lock()
                .await
                .pending()
                .any(|entry| entry.packet == later_small)
        );
    }

    #[test]
    fn static_policy_digest_binds_effective_operation_policy_only() {
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "orders/".into(),
        };
        let (first, registry) = fixture(1, false);
        let (same, _) = fixture(1, true);
        let (changed, _) = fixture(2, false);
        assert_eq!(
            static_policy_digest(&first, &scope, &registry).unwrap(),
            static_policy_digest(&same, &scope, &registry).unwrap()
        );
        assert_eq!(
            static_policy_digest(&first, &scope, &registry).unwrap(),
            static_policy_digest(&changed, &scope, &registry).unwrap()
        );
        let mut disabled = first.clone();
        let mut policy = disabled.get_placement_policy("orders/").unwrap().clone();
        policy.certified = false;
        disabled.set_placement_policy(policy).unwrap();
        assert!(static_policy_digest(&disabled, &scope, &registry).is_err());
    }

    #[test]
    fn static_policy_digest_ignores_unrelated_and_noop_control_plane_updates() {
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "orders/".into(),
        };
        let (mut namespace, registry) = fixture(1, false);
        let before = static_policy_digest(&namespace, &scope, &registry).unwrap();
        namespace
            .set_placement_policy(
                PlacementPolicy::new(
                    PolicyVersion(1),
                    KeyRange {
                        prefix: "unrelated/".into(),
                    },
                    1,
                )
                .with_certified(false),
            )
            .unwrap();
        assert_eq!(
            static_policy_digest(&namespace, &scope, &registry).unwrap(),
            before
        );
        let same_policy = namespace.get_placement_policy("orders/").unwrap().clone();
        namespace.set_placement_policy(same_policy).unwrap();
        assert_eq!(
            static_policy_digest(&namespace, &scope, &registry).unwrap(),
            before
        );
        let mut same_authority = namespace
            .get_authority_definition("orders/")
            .unwrap()
            .clone();
        same_authority.auto_generated = !same_authority.auto_generated;
        namespace.set_authority_definition(same_authority);
        assert_eq!(
            static_policy_digest(&namespace, &scope, &registry).unwrap(),
            before
        );
    }

    #[test]
    fn static_policy_digest_ignores_nested_scoped_assignment_for_sibling_keys() {
        use crate::control_plane::system_namespace::{AuthorityDefinition, OperationAssignment};

        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "orders/".into(),
        };
        let (mut namespace, registry) = fixture(1, false);
        let before = static_policy_digest(&namespace, &scope, &registry).unwrap();
        namespace
            .set_operation_assignment(OperationAssignment {
                namespace_id: "ns".into(),
                policy: crate::placement::PlacementPolicy::new(
                    crate::types::PolicyVersion(1),
                    crate::types::KeyRange {
                        prefix: "orders/private/".into(),
                    },
                    1,
                )
                .with_certified(false),
                authority: AuthorityDefinition {
                    key_range: crate::types::KeyRange {
                        prefix: "orders/private/".into(),
                    },
                    authority_nodes: vec![NodeId("auth".into())],
                    auto_generated: false,
                },
            })
            .unwrap();
        assert_eq!(
            static_policy_digest(&namespace, &scope, &registry).unwrap(),
            before
        );
        assert!(scope_applies_to_key(
            &namespace,
            &scope,
            "orders/public/item"
        ));
        assert!(!scope_applies_to_key(
            &namespace,
            &scope,
            "orders/private/item"
        ));
    }

    #[test]
    fn scoped_operation_policy_digest_binds_namespace_without_changing_legacy_digest() {
        use crate::control_plane::system_namespace::OperationAssignment;

        let (mut namespace, registry) = fixture(1, false);
        let east = OperationScope {
            namespace_id: "east".into(),
            key_prefix: "orders/".into(),
        };
        let west = OperationScope {
            namespace_id: "west".into(),
            key_prefix: "orders/".into(),
        };
        let legacy_digest = static_policy_digest(&namespace, &east, &registry).unwrap();
        for scope in [&east, &west] {
            namespace
                .set_operation_assignment(OperationAssignment {
                    namespace_id: scope.namespace_id.clone(),
                    policy: namespace.get_placement_policy("orders/").unwrap().clone(),
                    authority: namespace
                        .get_authority_definition("orders/")
                        .unwrap()
                        .clone(),
                })
                .unwrap();
        }
        let east_digest = static_policy_digest(&namespace, &east, &registry).unwrap();
        let west_digest = static_policy_digest(&namespace, &west, &registry).unwrap();
        assert_ne!(east_digest, west_digest);
        assert_ne!(east_digest, legacy_digest);
        let legacy_scope = OperationScope {
            namespace_id: "legacy".into(),
            key_prefix: "orders/".into(),
        };
        assert_eq!(
            static_policy_digest(&namespace, &legacy_scope, &registry).unwrap(),
            legacy_digest
        );
    }

    #[test]
    fn static_policy_uses_effective_prefix_for_nested_ranges() {
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "orders/".into(),
        };
        let (mut namespace, registry) = fixture(1, false);
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "orders/private/".into(),
            },
            authority_nodes: vec![NodeId("auth".into())],
            auto_generated: false,
        });
        namespace
            .set_placement_policy(
                PlacementPolicy::new(
                    PolicyVersion(1),
                    KeyRange {
                        prefix: "orders/private/".into(),
                    },
                    1,
                )
                .with_certified(true),
            )
            .unwrap();
        assert!(static_policy_digest(&namespace, &scope, &registry).is_ok());
        assert!(scope_applies_to_key(
            &namespace,
            &scope,
            "orders/public/item"
        ));
        assert!(!scope_applies_to_key(
            &namespace,
            &scope,
            "orders/private/item"
        ));
        let child = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "orders/private/".into(),
        };
        assert!(scope_applies_to_key(
            &namespace,
            &child,
            "orders/private/item"
        ));
    }

    #[test]
    fn static_evidence_rejects_changed_local_ownership_after_restart() {
        let temp = tempfile::tempdir().unwrap();
        let (namespace, registry) = fixture(1, false);
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "orders/".into(),
        };
        let config = EpochConfig::default();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 0,
        };
        let owned = OperationOwnedPrefixes::from_json("[\"orders/\",\"users/\"]").unwrap();
        let reordered = OperationOwnedPrefixes::from_json("[\"users/\",\"orders/\"]").unwrap();
        let changed = OperationOwnedPrefixes::from_json("[]").unwrap();
        let pinned = static_read_trust_digest(&namespace, &scope, &registry, &owned).unwrap();
        assert_eq!(
            pinned,
            static_read_trust_digest(&namespace, &scope, &registry, &reordered).unwrap()
        );
        let store = StaticOperationEvidenceStore::open(temp.path(), &verifier, pinned).unwrap();
        drop(store);
        assert!(matches!(
            StaticOperationEvidenceStore::open(
                temp.path(),
                &verifier,
                static_read_trust_digest(&namespace, &scope, &registry, &changed).unwrap(),
            ),
            Err(StaticTrustError::TrustChanged)
        ));
    }

    #[tokio::test]
    async fn certificate_http_read_requires_exact_local_body_and_verified_proof() {
        let temp = tempfile::tempdir().unwrap();
        let (mut namespace, registry) = fixture(1, false);
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "users/".into(),
            },
            authority_nodes: vec![NodeId("auth".into())],
            auto_generated: false,
        });
        namespace
            .set_placement_policy(
                PlacementPolicy::new(
                    PolicyVersion(1),
                    KeyRange {
                        prefix: "users/".into(),
                    },
                    1,
                )
                .with_certified(true),
            )
            .unwrap();
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "orders/private/".into(),
            },
            authority_nodes: vec![NodeId("auth".into())],
            auto_generated: false,
        });
        namespace
            .set_placement_policy(
                PlacementPolicy::new(
                    PolicyVersion(1),
                    KeyRange {
                        prefix: "orders/private/".into(),
                    },
                    1,
                )
                .with_certified(true),
            )
            .unwrap();
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "orders/".into(),
        };
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 1,
            },
        };
        let timestamp = HlcTimestamp {
            physical: 1,
            logical: 0,
            node_id: "writer".into(),
        };
        let bound = BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: "orders/item".into(),
                    value_bytes: b"certified".to_vec(),
                    timestamp: timestamp.clone(),
                },
            },
            declaration: Declaration {
                creation_id: subject.clone(),
                key: "orders/item".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let mut admitted = OperationLedger::open(temp.path().join("admitted")).unwrap();
        let signable = admit_for_signing(&Allow, &mut admitted, bound.clone()).unwrap();
        let tree = OperationMerkleTree::from_signable(scope.clone(), &[signable]).unwrap();
        let cert = OperationMerkleCertificate {
            operation: bound.clone(),
            supports: vec![OperationRootSupport {
                signed_root: SignedOperationRoot::sign(
                    &tree,
                    KeysetVersion(1),
                    NodeId("auth".into()),
                    &SigningKey::from_bytes(&[7; 32]),
                ),
                proof: tree.proof(&subject).unwrap(),
            }],
        };
        let verified = cert
            .verify(
                &registry,
                &scope,
                KeysetVersion(1),
                0,
                &EpochConfig::default(),
                0,
            )
            .unwrap();
        let (full, compact) = status_certificates(Some(verified.clone()), Some(&bound), None);
        assert_eq!(full, Some(cert.clone()));
        assert!(compact.is_none());
        let receipt = crate::operation::OperationReceipt::from(&bound);
        let (full, compact) = status_certificates(Some(verified.clone()), None, Some(&receipt));
        assert!(full.is_none());
        let compact = compact.unwrap();
        assert_eq!(compact.subject, subject);
        assert_eq!(compact.commitment, receipt.commitment);
        let response = OperationStatusResponse {
            status: OperationCertificationStatus::Certified,
            certificate: None,
            compact_certificate: Some(compact),
            operation_receipt: Some(receipt.clone()),
        };
        let encoded = serde_json::to_vec(&response).unwrap();
        assert!(
            !encoded
                .windows(b"value_bytes".len())
                .any(|part| part == b"value_bytes")
        );
        let mut wrong = receipt.clone();
        wrong.commitment[0] ^= 1;
        assert_eq!(
            status_certificates(Some(verified), None, Some(&wrong)),
            (None, None)
        );
        let owned = OperationOwnedPrefixes::from_json("[\"\"]").unwrap();
        let transit_owned = OperationOwnedPrefixes::from_json("[]").unwrap();
        let policy_digest =
            static_read_trust_digest(&namespace, &scope, &registry, &owned).unwrap();
        let config = EpochConfig::default();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 0,
        };
        let store = StaticOperationEvidenceStore::open(
            temp.path().join("evidence"),
            &verifier,
            policy_digest,
        )
        .unwrap();
        let transit_store = StaticOperationEvidenceStore::open(
            temp.path().join("transit-evidence"),
            &verifier,
            static_read_trust_digest(&namespace, &scope, &registry, &transit_owned).unwrap(),
        )
        .unwrap();
        let users_scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "users/".into(),
        };
        let users_verifier = EvidenceVerifier {
            registry: &registry,
            scope: &users_scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 0,
        };
        let users_store = StaticOperationEvidenceStore::open(
            temp.path().join("users-evidence"),
            &users_verifier,
            static_read_trust_digest(&namespace, &users_scope, &registry, &owned).unwrap(),
        )
        .unwrap();
        let users_supports = OperationSupportStore::open(
            temp.path().join("users-supports"),
            &registry,
            &users_scope,
            &config,
        )
        .unwrap();
        let private_scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "orders/private/".into(),
        };
        let private_verifier = EvidenceVerifier {
            registry: &registry,
            scope: &private_scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 0,
        };
        let private_store = StaticOperationEvidenceStore::open(
            temp.path().join("private-evidence"),
            &private_verifier,
            static_read_trust_digest(&namespace, &private_scope, &registry, &owned).unwrap(),
        )
        .unwrap();
        let private_supports = OperationSupportStore::open(
            temp.path().join("private-supports"),
            &registry,
            &private_scope,
            &config,
        )
        .unwrap();
        let other_namespace_scope = OperationScope {
            namespace_id: "other-ns".into(),
            key_prefix: "orders/".into(),
        };
        let other_namespace_verifier = EvidenceVerifier {
            registry: &registry,
            scope: &other_namespace_scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 0,
        };
        let other_namespace_store = StaticOperationEvidenceStore::open(
            temp.path().join("other-namespace-evidence"),
            &other_namespace_verifier,
            static_read_trust_digest(&namespace, &other_namespace_scope, &registry, &owned)
                .unwrap(),
        )
        .unwrap();
        let other_namespace_supports = OperationSupportStore::open(
            temp.path().join("other-namespace-supports"),
            &registry,
            &other_namespace_scope,
            &config,
        )
        .unwrap();
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(temp.path().join("inbox"), Arc::new(Allow)).unwrap(),
        ));
        inbox
            .lock()
            .await
            .ensure_owned_manifest(owned.digest())
            .unwrap();
        let supports =
            OperationSupportStore::open(temp.path().join("supports"), &registry, &scope, &config)
                .unwrap();
        let support_outbox = SupportOutbox::open(temp.path().join("support-outbox")).unwrap();
        let state = OperationCertificateState {
            inbox: inbox.clone(),
            store: Arc::new(Mutex::new(store)),
            supports: Arc::new(Mutex::new(supports)),
            support_outbox: Arc::new(Mutex::new(support_outbox)),
            certificate_outbox: Arc::new(Mutex::new(
                CertificateOutbox::open(temp.path().join("certificate-outbox")).unwrap(),
            )),
            neighbors: OperationNeighborManifest::from_json("[]", &NodeId("self".into())).unwrap(),
            owned,
            registry: Arc::new(RwLock::new(registry)),
            namespace: Arc::new(RwLock::new(namespace)),
            scope: scope.clone(),
            epoch_config: config,
            faults: 0,
            link_auth: None,
        };
        let late_create_id = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 20,
            },
        };
        let late_create = BoundOperation {
            operation: OperationLeaf {
                subject: late_create_id.clone(),
                action: CandidateKvAction::Create {
                    key: "orders/late".into(),
                    value_bytes: b"initial".to_vec(),
                    timestamp: timestamp.clone(),
                },
            },
            declaration: Declaration {
                creation_id: late_create_id,
                key: "orders/late".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let writer_key = SigningKey::from_bytes(&[9; 32]);
        let compact_creation = SignedCreation::sign(&late_create, &writer_key).unwrap();
        let mut late_add = late_create.clone();
        late_add.operation.subject.event_dot.counter = 21;
        late_add.operation.action = CandidateKvAction::Add {
            key: "orders/late".into(),
            value_bytes: b"certified before create".to_vec(),
            timestamp: HlcTimestamp {
                physical: 2,
                logical: 0,
                node_id: "writer".into(),
            },
        };
        let late_add_id = late_add.operation.subject.clone();
        let late_signable = admit_for_signing(&Allow, &mut admitted, late_add.clone()).unwrap();
        let late_tree =
            OperationMerkleTree::from_signable(scope.clone(), &[late_signable]).unwrap();
        let late_certificate = OperationMerkleCertificate {
            operation: late_add.clone(),
            supports: vec![OperationRootSupport {
                signed_root: SignedOperationRoot::sign(
                    &late_tree,
                    KeysetVersion(1),
                    NodeId("auth".into()),
                    &SigningKey::from_bytes(&[7; 32]),
                ),
                proof: late_tree.proof(&late_add_id).unwrap(),
            }],
        };
        let response = certificate_router(state.clone(), "secret".into())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(CERTIFICATE_RECEIVE_PATH)
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&late_certificate).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // The proof can arrive before its body. Reopen both durable stores
        // before the body arrives to exercise the recovery boundary.
        let restart_registry = state.registry.clone();
        let restart_namespace = state.namespace.clone();
        let restart_scope = state.scope.clone();
        let restart_owned = state.owned.clone();
        let restart_neighbors = state.neighbors.clone();
        let restart_config = state.epoch_config.clone();
        drop(state);
        drop(inbox);
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(temp.path().join("inbox"), Arc::new(Allow)).unwrap(),
        ));
        let (store, supports) = {
            let registry = restart_registry.read().unwrap();
            let namespace = restart_namespace.read().unwrap();
            let digest =
                static_read_trust_digest(&namespace, &restart_scope, &registry, &restart_owned)
                    .unwrap();
            let verifier = EvidenceVerifier {
                registry: &registry,
                scope: &restart_scope,
                current_epoch: 0,
                epoch_config: &restart_config,
                faults: 0,
            };
            (
                StaticOperationEvidenceStore::open(temp.path().join("evidence"), &verifier, digest)
                    .unwrap(),
                OperationSupportStore::open(
                    temp.path().join("supports"),
                    &registry,
                    &restart_scope,
                    &restart_config,
                )
                .unwrap(),
            )
        };
        let state = OperationCertificateState {
            inbox: inbox.clone(),
            store: Arc::new(Mutex::new(store)),
            supports: Arc::new(Mutex::new(supports)),
            support_outbox: Arc::new(Mutex::new(
                SupportOutbox::open(temp.path().join("support-outbox")).unwrap(),
            )),
            certificate_outbox: Arc::new(Mutex::new(
                CertificateOutbox::open(temp.path().join("certificate-outbox")).unwrap(),
            )),
            neighbors: restart_neighbors,
            owned: restart_owned,
            registry: restart_registry,
            namespace: restart_namespace,
            scope: restart_scope,
            epoch_config: restart_config,
            faults: 0,
            link_auth: None,
        };
        let absent = tokio::task::spawn_blocking({
            let state = state.clone();
            let subject = late_add_id.clone();
            move || state.status_for_blocking(subject)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(absent.status, OperationCertificationStatus::Unknown);
        inbox
            .lock()
            .await
            .receive(OperationPacket {
                signed: SignedBoundOperation::sign(
                    late_add.clone(),
                    &writer_key,
                    compact_creation.clone(),
                )
                .unwrap(),
            })
            .unwrap();
        let before = tokio::task::spawn_blocking({
            let state = state.clone();
            let subject = late_add_id.clone();
            move || state.status_for_blocking(subject)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(before.status, OperationCertificationStatus::PolicyUnknown);
        assert!(before.certificate.is_some());
        let before = tokio::task::spawn_blocking({
            let state = state.clone();
            move || state.read_for_blocking("ns", "orders/late")
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(before.result, CertifiedReadResult::PolicyUnknown);
        assert_eq!(before.basis.len(), 1);
        inbox
            .lock()
            .await
            .receive(OperationPacket {
                signed: SignedBoundOperation::sign(late_create, &writer_key, compact_creation)
                    .unwrap(),
            })
            .unwrap();
        let late_add_for_unrelated = late_add_id.clone();
        let after = tokio::task::spawn_blocking({
            let state = state.clone();
            move || state.status_for_blocking(late_add_id)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(after.status, OperationCertificationStatus::Certified);
        let after = tokio::task::spawn_blocking({
            let state = state.clone();
            move || state.read_for_blocking("ns", "orders/late")
        })
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(after.result, CertifiedReadResult::Value { .. }));
        let mut unrelated_state = state.clone();
        unrelated_state.namespace = Arc::new(std::sync::RwLock::new(
            state.namespace.read().unwrap().clone(),
        ));
        unrelated_state
            .namespace
            .write()
            .unwrap()
            .set_placement_policy(
                PlacementPolicy::new(
                    PolicyVersion(1),
                    KeyRange {
                        prefix: "unrelated/".into(),
                    },
                    1,
                )
                .with_certified(false),
            )
            .unwrap();
        let after_unrelated_update = tokio::task::spawn_blocking(move || {
            let status = unrelated_state.status_for_blocking(late_add_for_unrelated)?;
            let read = unrelated_state.read_for_blocking("ns", "orders/late")?;
            Ok::<_, StatusCode>((status, read))
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            after_unrelated_update.0.status,
            OperationCertificationStatus::Certified
        );
        assert!(matches!(
            after_unrelated_update.1.result,
            CertifiedReadResult::Value { .. }
        ));
        let mut nested_state = state.clone();
        nested_state.namespace = Arc::new(RwLock::new(state.namespace.read().unwrap().clone()));
        nested_state
            .namespace
            .write()
            .unwrap()
            .set_operation_assignment(
                crate::control_plane::system_namespace::OperationAssignment {
                    namespace_id: "ns".into(),
                    policy: PlacementPolicy::new(
                        PolicyVersion(1),
                        KeyRange {
                            prefix: "orders/item".into(),
                        },
                        1,
                    )
                    .with_certified(false),
                    authority: AuthorityDefinition {
                        key_range: KeyRange {
                            prefix: "orders/item".into(),
                        },
                        authority_nodes: vec![NodeId("auth".into())],
                        auto_generated: false,
                    },
                },
            )
            .unwrap();
        let sibling = tokio::task::spawn_blocking({
            let state = nested_state.clone();
            let subject = late_add.operation.subject.clone();
            move || {
                Ok::<_, StatusCode>((
                    state.status_for_blocking(subject)?,
                    state.read_for_blocking("ns", "orders/late")?,
                ))
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(sibling.0.status, OperationCertificationStatus::Certified);
        assert!(matches!(
            sibling.1.result,
            CertifiedReadResult::Value { .. }
        ));
        assert_eq!(sibling.1.basis.len(), 1);
        let captured_parent_proof = certificate_router(nested_state, "secret".into())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(CERTIFICATE_RECEIVE_PATH)
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&cert).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            captured_parent_proof.status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        let mut captured_state = state.clone();
        captured_state.namespace = Arc::new(RwLock::new(state.namespace.read().unwrap().clone()));
        captured_state
            .namespace
            .write()
            .unwrap()
            .set_operation_assignment(
                crate::control_plane::system_namespace::OperationAssignment {
                    namespace_id: "ns".into(),
                    policy: PlacementPolicy::new(
                        PolicyVersion(1),
                        KeyRange {
                            prefix: "orders/late".into(),
                        },
                        1,
                    )
                    .with_certified(false),
                    authority: AuthorityDefinition {
                        key_range: KeyRange {
                            prefix: "orders/late".into(),
                        },
                        authority_nodes: vec![NodeId("auth".into())],
                        auto_generated: false,
                    },
                },
            )
            .unwrap();
        let captured = tokio::task::spawn_blocking(move || {
            let status = captured_state
                .status_for_blocking(late_add.operation.subject.clone())
                .unwrap();
            let read = captured_state
                .read_for_blocking("ns", "orders/late")
                .unwrap();
            let receipts = captured_state
                .verified_receipt_subjects_blocking(&[crate::operation::OperationReceipt::from(
                    &late_add,
                )])
                .unwrap();
            (status, read, receipts)
        })
        .await
        .unwrap();
        assert_eq!(captured.0.status, OperationCertificationStatus::Pending);
        assert!(captured.0.certificate.is_none());
        assert!(captured.1.basis.is_empty());
        assert!(captured.2.is_empty());
        let outside_subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 999,
            },
        };
        let outside_bound = BoundOperation {
            operation: OperationLeaf {
                subject: outside_subject.clone(),
                action: CandidateKvAction::Create {
                    key: "other/item".into(),
                    value_bytes: b"outside".to_vec(),
                    timestamp: timestamp.clone(),
                },
            },
            declaration: Declaration {
                creation_id: outside_subject.clone(),
                key: "other/item".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let outside_key = SigningKey::from_bytes(&[9; 32]);
        let outside_creation = SignedCreation::sign(&outside_bound, &outside_key).unwrap();
        inbox
            .lock()
            .await
            .receive(OperationPacket {
                signed: SignedBoundOperation::sign(outside_bound, &outside_key, outside_creation)
                    .unwrap(),
            })
            .unwrap();
        let users_subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 1000,
            },
        };
        let users_bound = BoundOperation {
            operation: OperationLeaf {
                subject: users_subject.clone(),
                action: CandidateKvAction::Create {
                    key: "users/item".into(),
                    value_bytes: b"users".to_vec(),
                    timestamp: timestamp.clone(),
                },
            },
            declaration: Declaration {
                creation_id: users_subject.clone(),
                key: "users/item".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let users_creation = SignedCreation::sign(&users_bound, &outside_key).unwrap();
        inbox
            .lock()
            .await
            .receive(OperationPacket {
                signed: SignedBoundOperation::sign(
                    users_bound.clone(),
                    &outside_key,
                    users_creation,
                )
                .unwrap(),
            })
            .unwrap();
        let users_signable = admit_for_signing(&Allow, &mut admitted, users_bound.clone()).unwrap();
        let users_tree =
            OperationMerkleTree::from_signable(users_scope.clone(), &[users_signable]).unwrap();
        let users_certificate = OperationMerkleCertificate {
            operation: users_bound,
            supports: vec![OperationRootSupport {
                signed_root: SignedOperationRoot::sign(
                    &users_tree,
                    KeysetVersion(1),
                    NodeId("auth".into()),
                    &SigningKey::from_bytes(&[7; 32]),
                ),
                proof: users_tree.proof(&users_subject).unwrap(),
            }],
        };
        let outside_status = tokio::task::spawn_blocking({
            let state = state.clone();
            let subject = outside_subject.clone();
            move || state.status_for_blocking(subject)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(outside_status.status, OperationCertificationStatus::Pending);
        assert!(outside_status.certificate.is_none());
        let transit_policy_id = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 1001,
            },
        };
        let transit_policy_body = BoundOperation {
            operation: OperationLeaf {
                subject: transit_policy_id.clone(),
                action: CandidateKvAction::Create {
                    key: "orders/nonowner/policy".into(),
                    value_bytes: b"private".to_vec(),
                    timestamp: timestamp.clone(),
                },
            },
            declaration: Declaration {
                creation_id: transit_policy_id.clone(),
                key: "orders/nonowner/policy".into(),
                mode: CertificationMode::Disabled,
            },
        };
        let transit_creation = SignedCreation::sign(&transit_policy_body, &outside_key).unwrap();
        let transit_packet = OperationPacket {
            signed: SignedBoundOperation::sign(transit_policy_body, &outside_key, transit_creation)
                .unwrap(),
        };
        inbox
            .lock()
            .await
            .receive_policy_witness(transit_packet.clone())
            .unwrap();
        assert!(inbox.lock().await.get(&transit_policy_id).is_none());
        let transit_state = OperationCertificateState {
            store: Arc::new(Mutex::new(transit_store)),
            owned: transit_owned,
            ..state.clone()
        };
        let transit_multi =
            MultiOperationCertificateState::new(vec![transit_state.clone()]).unwrap();
        let transit_public_multi_app = multi_public_operation_status_router(transit_multi.clone());
        let transit_app = certificate_router(transit_state.clone(), "secret".into());
        for read in [
            tokio::task::spawn_blocking({
                let state = transit_state.clone();
                move || state.read_for_blocking("ns", "orders/nonowner/policy")
            })
            .await
            .unwrap()
            .unwrap(),
            tokio::task::spawn_blocking(move || {
                transit_multi.read_for_key_blocking("ns", "orders/nonowner/policy")
            })
            .await
            .unwrap()
            .unwrap(),
        ] {
            assert_eq!(read.result, CertifiedReadResult::NotTargeted);
            assert!(read.basis.is_empty());
            assert!(!read.local_body_seen);
        }
        // Once this node also holds the exact body, status and key-level
        // certified read agree on the authenticated Disabled policy.
        inbox.lock().await.receive(transit_packet).unwrap();
        let status = tokio::task::spawn_blocking({
            let state = transit_state.clone();
            let subject = transit_policy_id.clone();
            move || state.status_for_blocking(subject)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(status.status, OperationCertificationStatus::NotTarget);
        for (counter, expected) in [
            (
                transit_policy_id.event_dot.counter,
                OperationCertificationStatus::NotTarget,
            ),
            (
                outside_subject.event_dot.counter,
                OperationCertificationStatus::Unknown,
            ),
        ] {
            let response = transit_public_multi_app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/api/operations/status/ns/writer/{counter}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let status: OperationStatusResponse =
                serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap())
                    .unwrap();
            assert_eq!(status.status, expected);
            assert!(status.certificate.is_none());
        }
        let response = transit_app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/internal/operations/certified-read/ns/orders/nonowner/policy")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let read: CertifiedOperationResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(read.result, CertifiedReadResult::NotTargeted);
        let stale_nonowner = OperationCertificateState {
            owned: OperationOwnedPrefixes::from_json("[\"users/\"]").unwrap(),
            ..transit_state.clone()
        };
        let stale_public = public_operation_status_router(stale_nonowner.clone());
        for (counter, expected_status, expected_result) in [
            (
                transit_policy_id.event_dot.counter,
                StatusCode::SERVICE_UNAVAILABLE,
                None,
            ),
            (
                outside_subject.event_dot.counter,
                StatusCode::OK,
                Some(OperationCertificationStatus::Unknown),
            ),
            (
                users_subject.event_dot.counter,
                StatusCode::OK,
                Some(OperationCertificationStatus::Pending),
            ),
            (
                424_242,
                StatusCode::OK,
                Some(OperationCertificationStatus::Unknown),
            ),
        ] {
            let response = stale_public
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/api/operations/status/ns/writer/{counter}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected_status);
            if let Some(expected) = expected_result {
                let status: OperationStatusResponse =
                    serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap())
                        .unwrap();
                assert_eq!(status.status, expected);
                assert!(status.certificate.is_none());
            }
        }
        let stale_app = certificate_router(stale_nonowner, "secret".into());
        let response = stale_app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/internal/operations/certified-read/ns/orders/nonowner/policy")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let unrelated = stale_app
            .oneshot(
                Request::builder()
                    .uri("/api/internal/operations/certified-read/ns/elsewhere")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unrelated.status(), StatusCode::OK);
        let unrelated: CertifiedOperationResponse =
            serde_json::from_slice(&to_bytes(unrelated.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(unrelated.result, CertifiedReadResult::Unknown);
        let submit_relay = OperationRelay::new(
            inbox.clone(),
            Arc::new(Mutex::new(
                OperationOutbox::open(temp.path().join("body-outbox")).unwrap(),
            )),
            OperationNeighborManifest::from_json("[]", &NodeId("self".into())).unwrap(),
            OperationOwnedPrefixes::from_json("[\"\"]").unwrap(),
        )
        .await
        .unwrap();
        let public_read_app = submit_router(submit_relay.clone());
        let unified_app = unified_operation_view_router(submit_relay.clone(), Some(state.clone()));
        let local_only_app = unified_operation_view_router(submit_relay.clone(), None);
        let submit_app = certified_submit_router(CertifiedSubmitState {
            relay: submit_relay,
            certificates: state.clone(),
        });
        let missing_create_id = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 2001,
            },
        };
        let missing_create = BoundOperation {
            operation: OperationLeaf {
                subject: missing_create_id.clone(),
                action: CandidateKvAction::Create {
                    key: "orders/submit-late-create".into(),
                    value_bytes: b"first".to_vec(),
                    timestamp: timestamp.clone(),
                },
            },
            declaration: Declaration {
                creation_id: missing_create_id,
                key: "orders/submit-late-create".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let writer_key = SigningKey::from_bytes(&[9; 32]);
        let compact_creation = SignedCreation::sign(&missing_create, &writer_key).unwrap();
        let mut add_before_create = missing_create.clone();
        add_before_create.operation.subject.event_dot.counter = 2002;
        add_before_create.operation.action = CandidateKvAction::Add {
            key: "orders/submit-late-create".into(),
            value_bytes: b"second".to_vec(),
            timestamp: HlcTimestamp {
                physical: 2,
                logical: 0,
                node_id: "writer".into(),
            },
        };
        let add_packet = OperationPacket {
            signed: SignedBoundOperation::sign(
                add_before_create,
                &writer_key,
                compact_creation.clone(),
            )
            .unwrap(),
        };
        let submit_request = |packet: &OperationPacket| {
            Request::builder()
                .method("POST")
                .uri("/api/operations/submit-certified?wait_ms=0")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(packet).unwrap()))
                .unwrap()
        };
        let mut shadowed = missing_create.clone();
        shadowed.operation.subject.event_dot.counter = 2003;
        shadowed.declaration.creation_id = shadowed.operation.subject.clone();
        if let CandidateKvAction::Create { key, .. } = &mut shadowed.operation.action {
            *key = "orders/private/shadow".into();
        }
        shadowed.declaration.key = "orders/private/shadow".into();
        let shadowed_creation = SignedCreation::sign(&shadowed, &writer_key).unwrap();
        let shadowed_packet = OperationPacket {
            signed: SignedBoundOperation::sign(shadowed.clone(), &writer_key, shadowed_creation)
                .unwrap(),
        };
        let response = submit_app
            .clone()
            .oneshot(submit_request(&shadowed_packet))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            inbox
                .lock()
                .await
                .get(&shadowed.operation.subject)
                .is_none(),
            "a parent verifier must reject a shadowed child key before saving it"
        );
        let response = submit_app
            .clone()
            .oneshot(submit_request(&add_packet))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let awaiting_create: CertifiedSubmitResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(
            awaiting_create.status,
            OperationCertificationStatus::PolicyUnknown
        );
        assert_eq!(awaiting_create.receipt.commitment, add_packet.commitment());
        let create_packet = OperationPacket {
            signed: SignedBoundOperation::sign(missing_create, &writer_key, compact_creation)
                .unwrap(),
        };
        let response = submit_app
            .clone()
            .oneshot(submit_request(&create_packet))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let response = tokio::task::spawn_blocking({
            let state = state.clone();
            let subject = add_packet.signed.bound.operation.subject.clone();
            move || state.status_for_blocking(subject)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.status, OperationCertificationStatus::Pending);
        let public_status_app = public_operation_status_router(state.clone());
        let public_certified_app = public_operation_status_router(state.clone());
        let mut second = state.clone();
        second.scope = users_scope;
        second.store = Arc::new(Mutex::new(users_store));
        second.supports = Arc::new(Mutex::new(users_supports));
        let mut private = state.clone();
        private.scope = private_scope;
        private.store = Arc::new(Mutex::new(private_store));
        private.supports = Arc::new(Mutex::new(private_supports));
        let mut other_namespace = state.clone();
        other_namespace.scope = other_namespace_scope;
        other_namespace.store = Arc::new(Mutex::new(other_namespace_store));
        other_namespace.supports = Arc::new(Mutex::new(other_namespace_supports));
        let multi = MultiOperationCertificateState::new(vec![
            state.clone(),
            second.clone(),
            private,
            other_namespace,
        ])
        .unwrap();
        let multi_for_guard = multi.clone();
        let state_for_guard = state.clone();
        tokio::task::spawn_blocking(move || {
            let checked = multi_for_guard
                .checked_scope_for_key("ns", "orders/private/item")
                .unwrap();
            assert_eq!(
                checked.selected.as_ref().unwrap().scope.key_prefix,
                "orders/private/"
            );
            assert!(state_for_guard.namespace.try_write().is_err());
            assert!(state_for_guard.registry.try_write().is_err());
            drop(checked);
            assert!(state_for_guard.namespace.try_write().is_ok());
        })
        .await
        .unwrap();
        assert!(multi.select_for_key("ns", "orders/item").unwrap().is_some());
        assert_eq!(
            multi
                .select_for_key("ns", "orders/private/item")
                .unwrap()
                .unwrap()
                .scope
                .key_prefix,
            "orders/private/"
        );
        assert!(multi.select_for_key("ns", "users/item").unwrap().is_some());
        assert_eq!(
            multi
                .select_for_key("other-ns", "orders/item")
                .unwrap()
                .unwrap()
                .scope
                .namespace_id,
            "other-ns"
        );
        assert!(multi.select_for_key("ns", "other/item").unwrap().is_none());
        assert!(
            multi
                .select_for_key("wrong", "orders/item")
                .unwrap()
                .is_none()
        );
        let private_subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 100,
            },
        };
        let private_bound = BoundOperation {
            operation: OperationLeaf {
                subject: private_subject.clone(),
                action: CandidateKvAction::Create {
                    key: "orders/private/item".into(),
                    value_bytes: b"private".to_vec(),
                    timestamp: timestamp.clone(),
                },
            },
            declaration: Declaration {
                creation_id: private_subject.clone(),
                key: "orders/private/item".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let private_signable =
            admit_for_signing(&Allow, &mut admitted, private_bound.clone()).unwrap();
        let private_tree = OperationMerkleTree::from_signable(
            multi
                .select_for_key("ns", "orders/private/item")
                .unwrap()
                .unwrap()
                .scope,
            &[private_signable],
        )
        .unwrap();
        let private_certificate = OperationMerkleCertificate {
            operation: private_bound,
            supports: vec![OperationRootSupport {
                signed_root: SignedOperationRoot::sign(
                    &private_tree,
                    KeysetVersion(1),
                    NodeId("auth".into()),
                    &SigningKey::from_bytes(&[7; 32]),
                ),
                proof: private_tree.proof(&private_subject).unwrap(),
            }],
        };
        let post_certificate = |certificate: &OperationMerkleCertificate| {
            Request::builder()
                .method("POST")
                .uri(CERTIFICATE_RECEIVE_PATH)
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(certificate).unwrap()))
                .unwrap()
        };
        let multi_receiver = multi_certificate_router(multi.clone(), "secret".into());
        assert_eq!(
            multi_receiver
                .clone()
                .oneshot(post_certificate(&private_certificate))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            certificate_router(state.clone(), "secret".into())
                .oneshot(post_certificate(&private_certificate))
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        let mut other_namespace_bound = bound.clone();
        other_namespace_bound.operation.subject.namespace_id = "other-ns".into();
        other_namespace_bound.declaration.creation_id =
            other_namespace_bound.operation.subject.clone();
        let other_namespace_subject = other_namespace_bound.operation.subject.clone();
        let other_namespace_signable =
            admit_for_signing(&Allow, &mut admitted, other_namespace_bound.clone()).unwrap();
        let other_namespace_tree = OperationMerkleTree::from_signable(
            multi
                .select_for_key("other-ns", "orders/item")
                .unwrap()
                .unwrap()
                .scope,
            &[other_namespace_signable],
        )
        .unwrap();
        let other_namespace_certificate = OperationMerkleCertificate {
            operation: other_namespace_bound,
            supports: vec![OperationRootSupport {
                signed_root: SignedOperationRoot::sign(
                    &other_namespace_tree,
                    KeysetVersion(1),
                    NodeId("auth".into()),
                    &SigningKey::from_bytes(&[7; 32]),
                ),
                proof: other_namespace_tree
                    .proof(&other_namespace_subject)
                    .unwrap(),
            }],
        };
        assert_eq!(
            multi_receiver
                .oneshot(post_certificate(&other_namespace_certificate))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            certificate_router(state.clone(), "secret".into())
                .oneshot(post_certificate(&other_namespace_certificate))
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        let multi_app = multi_public_operation_status_router(multi.clone());
        let response = multi_app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/operations/status/ns/writer/999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let status: OperationStatusResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(status.status, OperationCertificationStatus::Pending);
        let response = multi_app
            .oneshot(
                Request::builder()
                    .uri("/api/operations/certified-read/ns/other/item")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let read: CertifiedOperationResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(read.result, CertifiedReadResult::NoCertifiedInformation);
        assert!(read.local_body_seen);
        assert!(read.basis.is_empty());
        let mut duplicate = second;
        duplicate.scope.key_prefix = "orders/".into();
        assert!(MultiOperationCertificateState::new(vec![state.clone(), duplicate]).is_err());
        assert!(MultiOperationCertificateState::new(Vec::new()).is_err());
        let app = certificate_router(state.clone(), "secret".into());
        let support_post = Request::builder()
            .method("POST")
            .uri(SUPPORT_RECEIVE_PATH)
            .header("authorization", "Bearer secret")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&SupportPacket {
                    operation: cert.operation.clone(),
                    support: cert.supports[0].clone(),
                })
                .unwrap(),
            ))
            .unwrap();
        let support_response = app.clone().oneshot(support_post).await.unwrap();
        assert_eq!(support_response.status(), StatusCode::OK);
        let support_body = to_bytes(support_response.into_body(), 4096).await.unwrap();
        let support_receipt: SupportReceipt = serde_json::from_slice(&support_body).unwrap();
        assert!(support_receipt.certificate_ready);
        let post = Request::builder()
            .method("POST")
            .uri(CERTIFICATE_RECEIVE_PATH)
            .header("authorization", "Bearer secret")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&cert).unwrap()))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(post).await.unwrap().status(),
            StatusCode::OK
        );
        let multi_internal = multi_certificate_router(multi.clone(), "secret".into());
        let response = multi_internal
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/internal/operations/certified-read/ns/orders/item")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let read: CertifiedOperationResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await.unwrap())
                .unwrap();
        assert_eq!(read.result, CertifiedReadResult::Unknown);
        let response = multi_internal
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(CERTIFICATE_RECEIVE_PATH)
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&users_certificate).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = multi_internal
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/internal/operations/certified-read/ns/users/item")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let read: CertifiedOperationResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await.unwrap())
                .unwrap();
        assert!(matches!(read.result, CertifiedReadResult::Value { .. }));
        let response = multi_internal
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/internal/operations/status/ns/writer/1000")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let status: OperationStatusResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await.unwrap())
                .unwrap();
        assert_eq!(status.status, OperationCertificationStatus::Certified);
        let response = multi_internal
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(SUPPORT_RECEIVE_PATH)
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SupportPacket {
                            operation: users_certificate.operation.clone(),
                            support: users_certificate.supports[0].clone(),
                        })
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut wrong_scope_certificate = cert.clone();
        wrong_scope_certificate.operation.declaration.key = "else/item".into();
        let response = multi_internal
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(CERTIFICATE_RECEIVE_PATH)
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&wrong_scope_certificate).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let mut wrong_namespace_support = SupportPacket {
            operation: cert.operation.clone(),
            support: cert.supports[0].clone(),
        };
        wrong_namespace_support
            .operation
            .operation
            .subject
            .namespace_id = "else".into();
        let response = multi_internal
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(SUPPORT_RECEIVE_PATH)
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&wrong_namespace_support).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let read_request = || {
            Request::builder()
                .uri("/api/internal/operations/certified-read/ns/orders/item")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .unwrap()
        };
        let status_request = || {
            Request::builder()
                .uri("/api/internal/operations/status/ns/writer/1")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .unwrap()
        };
        let response = app.clone().oneshot(status_request()).await.unwrap();
        let status: OperationStatusResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(status.status, OperationCertificationStatus::Unknown);
        let response = app.clone().oneshot(read_request()).await.unwrap();
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let before: CertifiedOperationResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(before.result, CertifiedReadResult::Unknown);
        assert!(before.basis.is_empty());
        let early_view = Request::builder()
            .uri("/api/operations/view/ns/orders/item?kind=certified")
            .body(Body::empty())
            .unwrap();
        let response = unified_app.clone().oneshot(early_view).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let early: OperationViewResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(early, OperationViewResponse::Certified(before));
        let writer = SigningKey::from_bytes(&[9; 32]);
        let creation = SignedCreation::sign(&bound, &writer).unwrap();
        let packet = OperationPacket {
            signed: SignedBoundOperation::sign(bound.clone(), &writer, creation.clone()).unwrap(),
        };
        let submit = Request::builder()
            .method("POST")
            .uri("/api/operations/submit-certified?wait_ms=100")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&packet).unwrap()))
            .unwrap();
        let response = submit_app.clone().oneshot(submit).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let completed: CertifiedSubmitResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(completed.status, OperationCertificationStatus::Certified);
        assert_eq!(completed.certificate, Some(cert.clone()));
        let public_status = Request::builder()
            .uri("/api/operations/status/ns/writer/1")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            public_status_app
                .clone()
                .oneshot(public_status)
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let response = transit_app.clone().oneshot(read_request()).await.unwrap();
        let transit_read: CertifiedOperationResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(transit_read.result, CertifiedReadResult::Unknown);
        assert!(!transit_read.local_body_seen);
        let response = transit_app.oneshot(status_request()).await.unwrap();
        let transit_status: OperationStatusResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(transit_status.status, OperationCertificationStatus::Unknown);
        let response = app.clone().oneshot(status_request()).await.unwrap();
        let status: OperationStatusResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(status.status, OperationCertificationStatus::Certified);
        assert_eq!(status.certificate, Some(cert.clone()));
        let response = app.clone().oneshot(read_request()).await.unwrap();
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let after: CertifiedOperationResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            after.result,
            CertifiedReadResult::Value {
                value_bytes: b"certified".to_vec(),
                timestamp: timestamp.clone(),
            }
        );
        assert_eq!(after.basis, vec![cert.clone()]);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/internal/operations/certified-read/ns/orders/other")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let other: CertifiedOperationResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(other.result, CertifiedReadResult::Unknown);
        assert!(other.basis.is_empty());
        let view_request = |kind: &str| {
            Request::builder()
                .uri(format!("/api/operations/view/ns/orders/item?kind={kind}"))
                .body(Body::empty())
                .unwrap()
        };
        let response = unified_app
            .clone()
            .oneshot(view_request("local"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let local: OperationViewResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(
            local,
            OperationViewResponse::Local(OperationRead::Value {
                value_bytes: b"certified".to_vec(),
                timestamp: timestamp.clone(),
            })
        );
        let response = unified_app
            .clone()
            .oneshot(view_request("certified"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let certified: OperationViewResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(certified, OperationViewResponse::Certified(after.clone()));
        let missing_kind = Request::builder()
            .uri("/api/operations/view/ns/orders/item")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            unified_app
                .clone()
                .oneshot(missing_kind)
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        let foreign_key = Request::builder()
            .uri("/api/operations/view/ns/elsewhere?kind=certified")
            .body(Body::empty())
            .unwrap();
        let response = unified_app.oneshot(foreign_key).await.unwrap();
        let foreign: OperationViewResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(
            foreign,
            OperationViewResponse::Certified(CertifiedOperationResponse {
                result: CertifiedReadResult::Unknown,
                basis: Vec::new(),
                local_body_seen: false,
            })
        );
        let response = local_only_app
            .oneshot(view_request("certified"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let local: OperationViewResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(
            local,
            OperationViewResponse::Certified(CertifiedOperationResponse {
                result: CertifiedReadResult::NoCertifiedInformation,
                basis: Vec::new(),
                local_body_seen: true,
            })
        );
        let second = BoundOperation {
            operation: OperationLeaf {
                subject: CrdtSubject {
                    namespace_id: "ns".into(),
                    event_dot: EventDot {
                        writer: NodeId("writer".into()),
                        counter: 2,
                    },
                },
                action: CandidateKvAction::Add {
                    key: "orders/item".into(),
                    value_bytes: b"later".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 2,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: bound.declaration,
        };
        let pending_packet = OperationPacket {
            signed: SignedBoundOperation::sign(second.clone(), &writer, creation).unwrap(),
        };
        let submit = Request::builder()
            .method("POST")
            .uri("/api/operations/submit-certified?wait_ms=0")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&pending_packet).unwrap()))
            .unwrap();
        let response = submit_app.clone().oneshot(submit).await.unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let pending: CertifiedSubmitResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(pending.status, OperationCertificationStatus::Pending);
        assert_eq!(pending.receipt.commitment, pending_packet.commitment());
        assert!(pending.certificate.is_none());
        let public_status = Request::builder()
            .uri("/api/operations/status/ns/writer/2")
            .body(Body::empty())
            .unwrap();
        let response = public_status_app.oneshot(public_status).await.unwrap();
        let reported: OperationStatusResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(reported.status, OperationCertificationStatus::Pending);

        let response = public_read_app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/operations/read/ns/orders/item")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let ordinary: OperationRead =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(
            ordinary,
            OperationRead::Value {
                value_bytes: b"later".to_vec(),
                timestamp: HlcTimestamp {
                    physical: 2,
                    logical: 0,
                    node_id: "writer".into(),
                },
            }
        );
        let public_certified_request = || {
            Request::builder()
                .uri("/api/operations/certified-read/ns/orders/item")
                .body(Body::empty())
                .unwrap()
        };
        let response = public_certified_app
            .clone()
            .oneshot(public_certified_request())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let before_second_certificate: CertifiedOperationResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(before_second_certificate.basis.len(), 1);
        assert_eq!(
            before_second_certificate.result,
            CertifiedReadResult::Value {
                value_bytes: b"certified".to_vec(),
                timestamp: HlcTimestamp {
                    physical: 1,
                    logical: 0,
                    node_id: "writer".into(),
                },
            }
        );

        let second_signable = admit_for_signing(&Allow, &mut admitted, second.clone()).unwrap();
        let second_tree = OperationMerkleTree::from_signable(scope, &[second_signable]).unwrap();
        let second_certificate = OperationMerkleCertificate {
            operation: second.clone(),
            supports: vec![OperationRootSupport {
                signed_root: SignedOperationRoot::sign(
                    &second_tree,
                    KeysetVersion(1),
                    NodeId("auth".into()),
                    &SigningKey::from_bytes(&[7; 32]),
                ),
                proof: second_tree.proof(&second.operation.subject).unwrap(),
            }],
        };
        let certificate_app = app.clone();
        let certificate_to_deliver = second_certificate.clone();
        let delivery = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let post = Request::builder()
                .method("POST")
                .uri(CERTIFICATE_RECEIVE_PATH)
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&certificate_to_deliver).unwrap(),
                ))
                .unwrap();
            certificate_app.oneshot(post).await.unwrap().status()
        });
        let submit = Request::builder()
            .method("POST")
            .uri("/api/operations/submit-certified?wait_ms=1000")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&pending_packet).unwrap()))
            .unwrap();
        let response = submit_app.clone().oneshot(submit).await.unwrap();
        assert_eq!(delivery.await.unwrap(), StatusCode::OK);
        assert_eq!(response.status(), StatusCode::OK);
        let completed: CertifiedSubmitResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(completed.status, OperationCertificationStatus::Certified);
        assert_eq!(completed.certificate, Some(second_certificate));
        // A verified certificate already on disk permits an immediate
        // certified reply, even when the caller allows no waiting time.
        let immediate_retry = Request::builder()
            .method("POST")
            .uri("/api/operations/submit-certified?wait_ms=0")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&pending_packet).unwrap()))
            .unwrap();
        let response = submit_app.oneshot(immediate_retry).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let immediate: CertifiedSubmitResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(immediate.status, OperationCertificationStatus::Certified);
        assert_eq!(immediate.certificate, completed.certificate);
        assert_eq!(immediate.receipt, completed.receipt);
        let response = public_certified_app
            .oneshot(public_certified_request())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let after_second_certificate: CertifiedOperationResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(after_second_certificate.basis.len(), 2);
        assert_eq!(
            after_second_certificate.result,
            CertifiedReadResult::Value {
                value_bytes: b"later".to_vec(),
                timestamp: HlcTimestamp {
                    physical: 2,
                    logical: 0,
                    node_id: "writer".into(),
                },
            }
        );
        // A compact proof can still answer an exact-ID query if a body is
        // encountered again after its ledger receipt was collected.
        let retired_receipt = crate::operation::OperationReceipt::from(&cert.operation);
        let state_for_compaction = state.clone();
        let status = tokio::task::spawn_blocking(move || {
            {
                let registry = state_for_compaction.registry.read().unwrap();
                let namespace = state_for_compaction.namespace.read().unwrap();
                let digest = static_read_trust_digest(
                    &namespace,
                    &state_for_compaction.scope,
                    &registry,
                    &state_for_compaction.owned,
                )
                .unwrap();
                let verifier = EvidenceVerifier {
                    registry: &registry,
                    scope: &state_for_compaction.scope,
                    current_epoch: 0,
                    epoch_config: &state_for_compaction.epoch_config,
                    faults: state_for_compaction.faults,
                };
                assert_eq!(
                    state_for_compaction
                        .store
                        .blocking_lock()
                        .compact_receipted_and_sync(
                            std::slice::from_ref(&retired_receipt),
                            &verifier,
                            digest,
                        )
                        .unwrap(),
                    1
                );
            }
            state_for_compaction
                .supports
                .blocking_lock()
                .pin_neighbor_manifest(state_for_compaction.neighbors.digest())
                .unwrap();
            assert_eq!(
                state_for_compaction
                    .supports
                    .blocking_lock()
                    .compact_receipted_and_sync(std::slice::from_ref(&retired_receipt))
                    .unwrap(),
                1
            );
            state_for_compaction.status_for_blocking(retired_receipt.subject)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(status.status, OperationCertificationStatus::Certified);
        assert!(status.certificate.is_none());
        assert!(status.compact_certificate.is_some());
        assert_eq!(
            status.operation_receipt,
            Some(crate::operation::OperationReceipt::from(&cert.operation))
        );
        let repeated_support = SupportPacket {
            operation: cert.operation.clone(),
            support: cert.supports[0].clone(),
        };
        let support_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(SUPPORT_RECEIVE_PATH)
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&repeated_support).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(support_response.status(), StatusCode::OK);
        let support_response: SupportReceipt =
            serde_json::from_slice(&to_bytes(support_response.into_body(), 4096).await.unwrap())
                .unwrap();
        assert!(support_response.certificate_ready);
        assert_eq!(
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri(CERTIFICATE_RECEIVE_PATH)
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&cert).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status(),
            StatusCode::OK
        );

        // A live placement change must not turn a previously pinned scope
        // into a harmless no-verifier fallback with Pending status.
        state
            .namespace
            .write()
            .unwrap()
            .set_placement_policy(PlacementPolicy::new(
                PolicyVersion(2),
                KeyRange {
                    prefix: "orders/".into(),
                },
                1,
            ))
            .unwrap();
        let read_error = tokio::task::spawn_blocking({
            let multi = multi.clone();
            move || multi.read_for_key_blocking("ns", "orders/item").err()
        })
        .await
        .unwrap();
        assert_eq!(read_error, Some(StatusCode::SERVICE_UNAVAILABLE));
        let single_read_error = tokio::task::spawn_blocking({
            let state = state.clone();
            move || state.read_for_blocking("ns", "orders/item").err()
        })
        .await
        .unwrap();
        assert_eq!(single_read_error, Some(StatusCode::SERVICE_UNAVAILABLE));
        let response = multi_public_operation_status_router(multi.clone())
            .oneshot(
                Request::builder()
                    .uri("/api/operations/certified-read/ns/orders/item")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = certificate_router(state.clone(), "secret".into())
            .oneshot(post_certificate(&cert))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = certificate_router(state.clone(), "secret".into())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(SUPPORT_RECEIVE_PATH)
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&repeated_support).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = multi_public_operation_status_router(multi.clone())
            .oneshot(
                Request::builder()
                    .uri("/api/operations/status/ns/writer/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = multi_certificate_router(multi.clone(), "secret".into())
            .oneshot(post_certificate(&cert))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = multi_certificate_router(multi.clone(), "secret".into())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(SUPPORT_RECEIVE_PATH)
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&repeated_support).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        // A newly configured child verifier wins live selection, but it
        // cannot downgrade evidence saved under the old parent scope.
        let child_scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "orders/item".into(),
        };
        {
            let mut namespace = state.namespace.write().unwrap();
            namespace.set_authority_definition(AuthorityDefinition {
                key_range: KeyRange {
                    prefix: child_scope.key_prefix.clone(),
                },
                authority_nodes: vec![NodeId("auth".into())],
                auto_generated: false,
            });
            namespace
                .set_placement_policy(
                    PlacementPolicy::new(
                        PolicyVersion(2),
                        KeyRange {
                            prefix: child_scope.key_prefix.clone(),
                        },
                        1,
                    )
                    .with_certified(true),
                )
                .unwrap();
        }
        let mut child = state.clone();
        child.scope = child_scope.clone();
        {
            let registry = state.registry.read().unwrap();
            let namespace = state.namespace.read().unwrap();
            let child_digest =
                static_read_trust_digest(&namespace, &child_scope, &registry, &state.owned)
                    .unwrap();
            let child_verifier = EvidenceVerifier {
                registry: &registry,
                scope: &child_scope,
                current_epoch: 0,
                epoch_config: &state.epoch_config,
                faults: state.faults,
            };
            child.store = Arc::new(Mutex::new(
                StaticOperationEvidenceStore::open(
                    temp.path().join("new-child-evidence"),
                    &child_verifier,
                    child_digest,
                )
                .unwrap(),
            ));
            child.supports = Arc::new(Mutex::new(
                OperationSupportStore::open(
                    temp.path().join("new-child-supports"),
                    &registry,
                    &child_scope,
                    &state.epoch_config,
                )
                .unwrap(),
            ));
        }
        let with_child = MultiOperationCertificateState::new(vec![state.clone(), child]).unwrap();
        assert_eq!(
            with_child
                .select_for_key("ns", "orders/item")
                .unwrap()
                .unwrap()
                .scope,
            child_scope
        );
        let read_error = tokio::task::spawn_blocking({
            let with_child = with_child.clone();
            move || with_child.read_for_key_blocking("ns", "orders/item").err()
        })
        .await
        .unwrap();
        assert_eq!(read_error, Some(StatusCode::SERVICE_UNAVAILABLE));
        let response = multi_public_operation_status_router(with_child)
            .oneshot(
                Request::builder()
                    .uri("/api/operations/status/ns/writer/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let namespace_for_poison = state.namespace.clone();
        let _ = std::thread::spawn(move || {
            let _guard = namespace_for_poison.write().unwrap();
            panic!("poison namespace assignment");
        })
        .join();
        assert_eq!(
            multi.select_for_key("ns", "orders/item").err(),
            Some(StatusCode::INTERNAL_SERVER_ERROR)
        );
    }

    #[tokio::test]
    async fn support_receipts_report_previously_saved_full_certificate() {
        let temp = tempfile::tempdir().unwrap();
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "orders/".into(),
        };
        let keys: Vec<_> = (1..=4)
            .map(|byte| SigningKey::from_bytes(&[byte; 32]))
            .collect();
        let mut namespace = SystemNamespace::new();
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "orders/".into(),
            },
            authority_nodes: (0..4).map(|i| NodeId(format!("auth-{i}"))).collect(),
            auto_generated: false,
        });
        namespace
            .set_placement_policy(
                PlacementPolicy::new(
                    PolicyVersion(1),
                    KeyRange {
                        prefix: "orders/".into(),
                    },
                    4,
                )
                .with_certified(true),
            )
            .unwrap();
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                keys.iter()
                    .enumerate()
                    .map(|(i, key)| (NodeId(format!("auth-{i}")), key.verifying_key()))
                    .collect(),
            )
            .unwrap();
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
                    key: "orders/item".into(),
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
                key: "orders/item".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let mut ledger = OperationLedger::open(temp.path().join("admitted")).unwrap();
        let signable = admit_for_signing(&Allow, &mut ledger, bound.clone()).unwrap();
        let tree = OperationMerkleTree::from_signable(scope.clone(), &[signable]).unwrap();
        let supports: Vec<_> = keys
            .iter()
            .take(3)
            .enumerate()
            .map(|(i, key)| OperationRootSupport {
                signed_root: SignedOperationRoot::sign(
                    &tree,
                    KeysetVersion(1),
                    NodeId(format!("auth-{i}")),
                    key,
                ),
                proof: tree.proof(&bound.operation.subject).unwrap(),
            })
            .collect();
        let certificate = OperationMerkleCertificate {
            operation: bound.clone(),
            supports: supports.clone(),
        };
        let owned = OperationOwnedPrefixes::from_json("[\"\"]").unwrap();
        let digest = static_read_trust_digest(&namespace, &scope, &registry, &owned).unwrap();
        let config = EpochConfig::default();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        let mut evidence =
            StaticOperationEvidenceStore::open(temp.path().join("evidence"), &verifier, digest)
                .unwrap();
        evidence
            .record_and_sync(certificate.clone(), &verifier, digest)
            .unwrap();
        let state = OperationCertificateState {
            inbox: Arc::new(Mutex::new(
                OperationInbox::open(temp.path().join("inbox"), Arc::new(Allow)).unwrap(),
            )),
            store: Arc::new(Mutex::new(evidence)),
            supports: Arc::new(Mutex::new(
                OperationSupportStore::open(
                    temp.path().join("supports"),
                    &registry,
                    &scope,
                    &config,
                )
                .unwrap(),
            )),
            support_outbox: Arc::new(Mutex::new(
                SupportOutbox::open(temp.path().join("support-outbox")).unwrap(),
            )),
            certificate_outbox: Arc::new(Mutex::new(
                CertificateOutbox::open(temp.path().join("certificate-outbox")).unwrap(),
            )),
            neighbors: OperationNeighborManifest::from_json("[]", &NodeId("self".into())).unwrap(),
            owned,
            registry: Arc::new(RwLock::new(registry)),
            namespace: Arc::new(RwLock::new(namespace)),
            scope,
            epoch_config: config,
            faults: 1,
            link_auth: None,
        };
        // A receipt exists for every ingested body. It does not authorize
        // replacing the full proof while the ledger still retains that body
        // for Certified reads (for example, a live winner).
        let writer = SigningKey::from_bytes(&[9; 32]);
        let creation = SignedCreation::sign(&bound, &writer).unwrap();
        state
            .inbox
            .lock()
            .await
            .ensure_owned_manifest(state.owned.digest())
            .unwrap();
        state
            .inbox
            .lock()
            .await
            .receive(OperationPacket {
                signed: SignedBoundOperation::sign(bound.clone(), &writer, creation).unwrap(),
            })
            .unwrap();
        let expected_read = tokio::task::spawn_blocking({
            let state = state.clone();
            move || state.read_for_blocking("ns", "orders/item")
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(expected_read.basis, vec![certificate.clone()]);
        let relay = OperationRelay::new(
            state.inbox.clone(),
            Arc::new(Mutex::new(
                OperationOutbox::open(temp.path().join("outbox")).unwrap(),
            )),
            state.neighbors.clone(),
            state.owned.clone(),
        )
        .await
        .unwrap();
        let state_for_compaction = state.clone();
        tokio::task::spawn_blocking(move || {
            state_for_compaction.compact_ledger_bodies_blocking(&relay)
        })
        .await
        .unwrap()
        .unwrap();
        assert!(
            state
                .inbox
                .lock()
                .await
                .get(&bound.operation.subject)
                .is_some()
        );
        let after_read = tokio::task::spawn_blocking({
            let state = state.clone();
            move || state.read_for_blocking("ns", "orders/item")
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(after_read, expected_read);
        assert!(
            tokio::task::spawn_blocking({
                let state = state.clone();
                let subject = bound.operation.subject.clone();
                move || {
                    let registry = state.registry.read().unwrap();
                    let verifier = EvidenceVerifier {
                        registry: &registry,
                        scope: &state.scope,
                        current_epoch: 0,
                        epoch_config: &state.epoch_config,
                        faults: state.faults,
                    };
                    state
                        .store
                        .blocking_lock()
                        .get_verified(&subject, &verifier, digest)
                        .unwrap()
                        .is_some()
                }
            })
            .await
            .unwrap(),
            "a retained body must retain its full certified-read proof"
        );
        let mut fork = bound.clone();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut fork.operation.action {
            *value_bytes = b"minority fork".to_vec();
        }
        let mut fork_ledger = OperationLedger::open(temp.path().join("fork-admitted")).unwrap();
        let fork_signable = admit_for_signing(&Allow, &mut fork_ledger, fork.clone()).unwrap();
        let fork_tree =
            OperationMerkleTree::from_signable(state.scope.clone(), &[fork_signable]).unwrap();
        let fork_packet = SupportPacket {
            operation: fork.clone(),
            support: OperationRootSupport {
                signed_root: SignedOperationRoot::sign(
                    &fork_tree,
                    KeysetVersion(1),
                    NodeId("auth-0".into()),
                    &keys[0],
                ),
                proof: fork_tree.proof(&fork.operation.subject).unwrap(),
            },
        };
        let fork_first = fork_packet.clone();
        let first = state.clone();
        let fork_receipt =
            tokio::task::spawn_blocking(move || first.record_support_blocking(fork_packet))
                .await
                .unwrap()
                .unwrap();
        assert!(!fork_receipt.certificate_ready);
        let packet = SupportPacket {
            operation: bound,
            support: supports[0].clone(),
        };
        let honest_after_fork = packet.clone();
        let first = state.clone();
        let received = packet.clone();
        let receipt = tokio::task::spawn_blocking(move || first.record_support_blocking(received))
            .await
            .unwrap()
            .unwrap();
        assert!(receipt.certificate_ready);
        let second = state.clone();
        let receipt =
            tokio::task::spawn_blocking(move || second.record_authority_support_blocking(packet))
                .await
                .unwrap()
                .unwrap();
        assert!(receipt.certificate_ready);
        assert!(receipt.already_present);

        let mut reverse = state.clone();
        {
            let registry = reverse.registry.read().unwrap();
            let verifier = EvidenceVerifier {
                registry: &registry,
                scope: &reverse.scope,
                current_epoch: 0,
                epoch_config: &reverse.epoch_config,
                faults: reverse.faults,
            };
            reverse.store = Arc::new(Mutex::new(
                StaticOperationEvidenceStore::open(
                    temp.path().join("reverse-evidence"),
                    &verifier,
                    digest,
                )
                .unwrap(),
            ));
            reverse.supports = Arc::new(Mutex::new(
                OperationSupportStore::open(
                    temp.path().join("reverse-supports"),
                    &registry,
                    &reverse.scope,
                    &reverse.epoch_config,
                )
                .unwrap(),
            ));
        }
        let first = reverse.clone();
        let receipt =
            tokio::task::spawn_blocking(move || first.record_support_blocking(fork_first))
                .await
                .unwrap()
                .unwrap();
        assert!(!receipt.certificate_ready);
        let _ = receive_http(State(reverse.clone()), Json(certificate))
            .await
            .unwrap();
        let receipt =
            tokio::task::spawn_blocking(move || reverse.record_support_blocking(honest_after_fork))
                .await
                .unwrap()
                .unwrap();
        assert!(receipt.certificate_ready);
    }

    #[tokio::test]
    async fn support_http_relay_persists_each_hop_and_certifies_at_quorum() {
        use crate::network::operation_support_transport::SupportTransportClient;

        let temp = tempfile::tempdir().unwrap();
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "orders/".into(),
        };
        let keys: Vec<_> = (1..=4)
            .map(|byte| SigningKey::from_bytes(&[byte; 32]))
            .collect();
        let mut namespace = SystemNamespace::new();
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "orders/".into(),
            },
            authority_nodes: (0..4).map(|i| NodeId(format!("auth-{i}"))).collect(),
            auto_generated: false,
        });
        namespace
            .set_placement_policy(
                PlacementPolicy::new(
                    PolicyVersion(1),
                    KeyRange {
                        prefix: "orders/".into(),
                    },
                    4,
                )
                .with_certified(true),
            )
            .unwrap();
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                keys.iter()
                    .enumerate()
                    .map(|(i, key)| (NodeId(format!("auth-{i}")), key.verifying_key()))
                    .collect(),
            )
            .unwrap();
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 1,
            },
        };
        let timestamp = HlcTimestamp {
            physical: 1,
            logical: 0,
            node_id: "writer".into(),
        };
        let bound = BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: "orders/item".into(),
                    value_bytes: b"relayed".to_vec(),
                    timestamp: timestamp.clone(),
                },
            },
            declaration: Declaration {
                creation_id: subject.clone(),
                key: "orders/item".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let mut ledger = OperationLedger::open(temp.path().join("admitted")).unwrap();
        let signable = admit_for_signing(&Allow, &mut ledger, bound.clone()).unwrap();
        let tree =
            OperationMerkleTree::from_signable(scope.clone(), std::slice::from_ref(&signable))
                .unwrap();
        let mut other = bound.clone();
        other.operation.subject.event_dot.counter = 2;
        other.declaration.creation_id = other.operation.subject.clone();
        other.declaration.key = "orders/other".into();
        if let CandidateKvAction::Create { key, .. } = &mut other.operation.action {
            *key = "orders/other".into();
        }
        let extra = admit_for_signing(&Allow, &mut ledger, other).unwrap();
        let alternate_tree =
            OperationMerkleTree::from_signable(scope.clone(), &[signable, extra]).unwrap();
        let alternate = SupportPacket {
            operation: bound.clone(),
            support: OperationRootSupport {
                signed_root: SignedOperationRoot::sign(
                    &alternate_tree,
                    KeysetVersion(1),
                    NodeId("auth-0".into()),
                    &keys[0],
                ),
                proof: alternate_tree.proof(&subject).unwrap(),
            },
        };
        assert_ne!(alternate.support.signed_root.root, tree.root());
        let packets: Vec<SupportPacket> = keys
            .iter()
            .take(3)
            .enumerate()
            .map(|(i, key)| SupportPacket {
                operation: bound.clone(),
                support: OperationRootSupport {
                    signed_root: SignedOperationRoot::sign(
                        &tree,
                        KeysetVersion(1),
                        NodeId(format!("auth-{i}")),
                        key,
                    ),
                    proof: tree.proof(&subject).unwrap(),
                },
            })
            .collect();
        let listeners = [
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        ];
        let addresses: Vec<_> = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().to_string())
            .collect();
        let mut states = Vec::new();
        let mut servers = Vec::new();
        let config = EpochConfig::default();
        for (index, listener) in listeners.into_iter().enumerate() {
            let root = temp.path().join(format!("node-{index}"));
            let verifier = EvidenceVerifier {
                registry: &registry,
                scope: &scope,
                current_epoch: 0,
                epoch_config: &config,
                faults: 1,
            };
            let next = if index < 2 {
                serde_json::json!([{"node_id": format!("node-{}", index + 1),
                    "addr": addresses[index + 1], "key_prefixes": ["orders/"]}])
            } else {
                serde_json::json!([])
            };
            let owned =
                OperationOwnedPrefixes::from_json(if index == 2 { "[\"orders/\"]" } else { "[]" })
                    .unwrap();
            let digest = static_read_trust_digest(&namespace, &scope, &registry, &owned).unwrap();
            let state = OperationCertificateState {
                inbox: Arc::new(Mutex::new(
                    OperationInbox::open(root.join("inbox"), Arc::new(Allow)).unwrap(),
                )),
                store: Arc::new(Mutex::new(
                    StaticOperationEvidenceStore::open(root.join("evidence"), &verifier, digest)
                        .unwrap(),
                )),
                supports: Arc::new(Mutex::new(
                    OperationSupportStore::open(root.join("supports"), &registry, &scope, &config)
                        .unwrap(),
                )),
                support_outbox: Arc::new(Mutex::new(
                    SupportOutbox::open(root.join("outbox")).unwrap(),
                )),
                certificate_outbox: Arc::new(Mutex::new(
                    CertificateOutbox::open(root.join("certificate-outbox")).unwrap(),
                )),
                neighbors: OperationNeighborManifest::from_json(
                    &next.to_string(),
                    &NodeId(format!("node-{index}")),
                )
                .unwrap(),
                owned,
                registry: Arc::new(RwLock::new(registry.clone())),
                namespace: Arc::new(RwLock::new(namespace.clone())),
                scope: scope.clone(),
                epoch_config: config.clone(),
                faults: 1,
                link_auth: None,
            };
            let server_state = state.clone();
            servers.push(tokio::spawn(async move {
                axum::serve(listener, certificate_router(server_state, "secret".into()))
                    .await
                    .unwrap();
            }));
            states.push(state);
        }
        let client = SupportTransportClient::new("secret".into());
        for (count, packet) in packets.iter().enumerate() {
            let receipt = client.send(&addresses[0], packet).await.unwrap();
            assert_eq!(receipt.certificate_ready, count == 2);
            // A's ACK proves that the next-hop obligation is on disk.
            let a_outbox = states[0].support_outbox.lock().await;
            let a_entry = a_outbox
                .pending()
                .find(|entry| entry.packet == *packet)
                .unwrap();
            let a_id = a_entry.id;
            let a_record: serde_json::Value = serde_json::from_slice(
                &std::fs::read(temp.path().join(format!("node-0/outbox/{a_id}.send"))).unwrap(),
            )
            .unwrap();
            assert_eq!(a_record["packet"], serde_json::to_value(packet).unwrap());
            drop(a_outbox);
            states[0]
                .support_outbox
                .lock()
                .await
                .deliver(a_id, &client)
                .await
                .unwrap();
            let b_supports = states[1].supports.lock().await;
            assert_eq!(
                b_supports
                    .packets()
                    .filter(|(_, support)| support == &packet.support)
                    .count(),
                1
            );
            drop(b_supports);
            let log =
                std::fs::read(temp.path().join("node-1/supports/operation-supports.log")).unwrap();
            let encoded = serde_json::to_vec(&packet.support).unwrap();
            assert!(log.windows(encoded.len()).any(|window| window == encoded));
            let b_outbox = states[1].support_outbox.lock().await;
            let b_entry = b_outbox
                .pending()
                .find(|entry| entry.packet == *packet)
                .unwrap();
            let b_id = b_entry.id;
            let b_record: serde_json::Value = serde_json::from_slice(
                &std::fs::read(temp.path().join(format!("node-1/outbox/{b_id}.send"))).unwrap(),
            )
            .unwrap();
            assert_eq!(b_record["packet"], serde_json::to_value(packet).unwrap());
            drop(b_outbox);
            states[1]
                .support_outbox
                .lock()
                .await
                .deliver(b_id, &client)
                .await
                .unwrap();
            assert_eq!(states[2].support_outbox.lock().await.pending().count(), 0);
            if count == 0 {
                let second = client.send(&addresses[0], &alternate).await.unwrap();
                assert!(!second.already_present);
                assert!(!second.certificate_ready);
                for (index, state) in states.iter().enumerate().take(2) {
                    let entry = state
                        .support_outbox
                        .lock()
                        .await
                        .pending()
                        .find(|entry| entry.packet == alternate)
                        .cloned()
                        .unwrap();
                    if index == 1 {
                        let receipt = client.send(&addresses[2], &alternate).await.unwrap();
                        assert!(!receipt.already_present);
                        assert!(!receipt.certificate_ready);
                        state
                            .support_outbox
                            .lock()
                            .await
                            .record_receipt(entry.id, receipt)
                            .unwrap();
                    } else {
                        state
                            .support_outbox
                            .lock()
                            .await
                            .deliver(entry.id, &client)
                            .await
                            .unwrap();
                    }
                }
                let received = states[2].supports.lock().await;
                assert_eq!(received.support_count(&subject), 1);
                assert_eq!(received.packets().count(), 2);
                assert!(
                    received
                        .assemble_certificate(&subject, 1)
                        .unwrap()
                        .is_none()
                );
            }
        }
        assert_eq!(states[0].support_outbox.lock().await.pending().count(), 0);
        assert_eq!(states[1].support_outbox.lock().await.pending().count(), 0);
        // A complete certificate has its own durable A -> B -> C path.
        // C's body is deliberately absent until after this delivery.
        let cert_client =
            crate::network::operation_certificate_outbox::CertificateTransportClient::new(
                "secret".into(),
            );
        for index in 0..2 {
            let entry = states[index]
                .certificate_outbox
                .lock()
                .await
                .pending()
                .next()
                .cloned()
                .unwrap();
            states[index]
                .certificate_outbox
                .lock()
                .await
                .deliver(entry.id, &cert_client)
                .await
                .unwrap();
            assert_eq!(
                states[index]
                    .certificate_outbox
                    .lock()
                    .await
                    .pending()
                    .count(),
                0
            );
            assert!(!states[index + 1].store.lock().await.is_empty());
        }
        let c_supports = states[2].supports.lock().await;
        assert!(
            c_supports
                .assemble_certificate(&subject, 1)
                .unwrap()
                .is_some()
        );
        drop(c_supports);
        let app = certificate_router(states[2].clone(), "secret".into());
        let read = || {
            Request::builder()
                .uri("/api/internal/operations/certified-read/ns/orders/item")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .unwrap()
        };
        let response = app.clone().oneshot(read()).await.unwrap();
        let before: CertifiedOperationResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(before.result, CertifiedReadResult::Unknown);
        assert!(!before.local_body_seen);
        let writer = SigningKey::from_bytes(&[9; 32]);
        let creation = SignedCreation::sign(&bound, &writer).unwrap();
        states[2]
            .inbox
            .lock()
            .await
            .receive(OperationPacket {
                signed: SignedBoundOperation::sign(bound, &writer, creation).unwrap(),
            })
            .unwrap();
        let response = app.oneshot(read()).await.unwrap();
        let after: CertifiedOperationResponse =
            serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
        assert_eq!(
            after.result,
            CertifiedReadResult::Value {
                value_bytes: b"relayed".to_vec(),
                timestamp,
            }
        );
        assert_eq!(after.basis.len(), 1);
        assert_eq!(after.basis[0].supports.len(), 3);
        for server in servers {
            server.abort();
            let _ = server.await;
        }
        drop(states);
        let recovered = OperationSupportStore::open(
            temp.path().join("node-2/supports"),
            &registry,
            &scope,
            &config,
        )
        .unwrap();
        assert_eq!(recovered.packets().count(), 4);
        assert!(
            recovered
                .assemble_certificate(&subject, 1)
                .unwrap()
                .is_some()
        );
        for index in 0..2 {
            assert_eq!(
                CertificateOutbox::open(
                    temp.path().join(format!("node-{index}/certificate-outbox"))
                )
                .unwrap()
                .pending()
                .count(),
                0
            );
            assert_eq!(
                SupportOutbox::open(temp.path().join(format!("node-{index}/outbox")))
                    .unwrap()
                    .pending()
                    .count(),
                0
            );
        }
    }

    #[tokio::test]
    async fn support_gc_waits_for_required_ack_and_recovers_compact_identity() {
        use crate::network::operation_support_transport::SupportReceipt;

        let temp = tempfile::tempdir().unwrap();
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "orders/".into(),
        };
        let authority_key = SigningKey::from_bytes(&[7; 32]);
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![(NodeId("auth".into()), authority_key.verifying_key())],
            )
            .unwrap();
        let mut namespace = SystemNamespace::new();
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "orders/".into(),
            },
            authority_nodes: vec![NodeId("auth".into())],
            auto_generated: false,
        });
        namespace
            .set_placement_policy(
                PlacementPolicy::new(
                    PolicyVersion(1),
                    KeyRange {
                        prefix: "orders/".into(),
                    },
                    1,
                )
                .with_certified(true),
            )
            .unwrap();
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
                    key: "orders/item".into(),
                    value_bytes: b"support-gc-value".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 1,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject.clone(),
                key: "orders/item".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let inbox_dir = temp.path().join("inbox");
        let tree = {
            let mut ledger = OperationLedger::open(temp.path().join("admitted")).unwrap();
            let signable = admit_for_signing(&Allow, &mut ledger, bound.clone()).unwrap();
            OperationMerkleTree::from_signable(scope.clone(), &[signable]).unwrap()
        };
        let writer = SigningKey::from_bytes(&[9; 32]);
        let creation = SignedCreation::sign(&bound, &writer).unwrap();
        {
            let mut inbox = OperationInbox::open(&inbox_dir, Arc::new(Allow)).unwrap();
            inbox
                .ensure_owned_manifest(OperationOwnedPrefixes::from_json("[]").unwrap().digest())
                .unwrap();
            inbox
                .receive(OperationPacket {
                    signed: SignedBoundOperation::sign(bound.clone(), &writer, creation).unwrap(),
                })
                .unwrap();
        }
        {
            let mut ledger = OperationLedger::open(&inbox_dir).unwrap();
            ledger.compact_and_sync(&subject).unwrap();
            ledger.checkpoint_and_sync().unwrap();
        }
        let support = OperationRootSupport {
            signed_root: SignedOperationRoot::sign(
                &tree,
                KeysetVersion(1),
                NodeId("auth".into()),
                &authority_key,
            ),
            proof: tree.proof(&subject).unwrap(),
        };
        let packet = SupportPacket {
            operation: bound.clone(),
            support: support.clone(),
        };
        let certificate = OperationMerkleCertificate {
            operation: bound.clone(),
            supports: vec![support],
        };
        let owned = OperationOwnedPrefixes::from_json("[]").unwrap();
        let digest = static_read_trust_digest(&namespace, &scope, &registry, &owned).unwrap();
        let config = EpochConfig::default();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 0,
        };
        let evidence_dir = temp.path().join("evidence");
        let mut evidence =
            StaticOperationEvidenceStore::open(&evidence_dir, &verifier, digest).unwrap();
        evidence
            .record_and_sync(certificate.clone(), &verifier, digest)
            .unwrap();
        let receipt = crate::operation::OperationReceipt::from(&bound);
        evidence
            .compact_receipted_and_sync(std::slice::from_ref(&receipt), &verifier, digest)
            .unwrap();
        let support_dir = temp.path().join("supports");
        let mut supports =
            OperationSupportStore::open(&support_dir, &registry, &scope, &config).unwrap();
        supports
            .record_and_sync(bound.clone(), packet.support.clone())
            .unwrap();
        let peer_addr = "peer:1234";
        let support_outbox_dir = temp.path().join("support-outbox");
        let mut support_outbox = SupportOutbox::open(&support_outbox_dir).unwrap();
        let support_id = support_outbox
            .ensure_enqueued(peer_addr.into(), packet.clone())
            .unwrap();
        let certificate_outbox_dir = temp.path().join("certificate-outbox");
        let mut certificate_outbox = CertificateOutbox::open(&certificate_outbox_dir).unwrap();
        certificate_outbox
            .ensure_enqueued(peer_addr.into(), certificate)
            .unwrap();
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(&inbox_dir, Arc::new(Allow)).unwrap(),
        ));
        let relay = OperationRelay::new(
            inbox.clone(),
            Arc::new(Mutex::new(
                OperationOutbox::open(temp.path().join("outbox")).unwrap(),
            )),
            OperationNeighborManifest::from_json("[]", &NodeId("self".into())).unwrap(),
            owned.clone(),
        )
        .await
        .unwrap();
        let state = OperationCertificateState {
            inbox,
            store: Arc::new(Mutex::new(evidence)),
            supports: Arc::new(Mutex::new(supports)),
            support_outbox: Arc::new(Mutex::new(support_outbox)),
            certificate_outbox: Arc::new(Mutex::new(certificate_outbox)),
            neighbors: OperationNeighborManifest::from_json(
                r#"[{"node_id":"peer","addr":"peer:1234","key_prefixes":["orders/"]}]"#,
                &NodeId("self".into()),
            )
            .unwrap(),
            owned,
            registry: Arc::new(RwLock::new(registry.clone())),
            namespace: Arc::new(RwLock::new(namespace)),
            scope: scope.clone(),
            epoch_config: config.clone(),
            faults: 0,
            link_auth: None,
        };
        let support_log = support_dir.join("operation-supports.log");
        let encoded_value = serde_json::to_vec(&b"support-gc-value".to_vec()).unwrap();
        let has_value = || {
            std::fs::read(&support_log)
                .unwrap()
                .windows(encoded_value.len())
                .any(|window| window == encoded_value)
        };
        assert!(has_value());
        let gc_state = state.clone();
        let gc_relay = relay.clone();
        tokio::task::spawn_blocking(move || gc_state.compact_ledger_bodies_blocking(&gc_relay))
            .await
            .unwrap()
            .unwrap();
        assert!(has_value(), "a missing support ACK must retain the value");
        assert_eq!(state.supports.lock().await.packets().count(), 1);

        state
            .support_outbox
            .lock()
            .await
            .record_receipt(
                support_id,
                SupportReceipt {
                    commitment: packet.commitment(),
                    already_present: false,
                    certificate_ready: true,
                },
            )
            .unwrap();
        let gc_state = state.clone();
        tokio::task::spawn_blocking(move || gc_state.compact_ledger_bodies_blocking(&relay))
            .await
            .unwrap()
            .unwrap();
        assert!(
            !has_value(),
            "the exact support ACK permits value collection"
        );
        drop(state);
        let recovered =
            OperationSupportStore::open(&support_dir, &registry, &scope, &config).unwrap();
        assert_eq!(recovered.packets().count(), 0);
        assert_eq!(recovered.support_count(&subject), 1);
    }
}
