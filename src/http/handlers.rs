use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::Response;
use tokio::sync::Mutex;

use super::codec::{deserialize_internal, internal_response};

use crate::api::certified::{CertifiedApi, OnTimeout};
use crate::api::eventual::EventualApi;
use crate::authority::certificate::{EpochConfig, KeysetRegistry};
use crate::control_plane::consensus::ControlPlaneConsensus;
use crate::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use crate::crdt::pn_counter::PnCounter;
use crate::error::CrdtError;
use crate::ops::metrics::{MetricsSnapshot, RuntimeMetrics};
use crate::ops::slo::{SLO_CERTIFIED_READ_P99, SLO_EVENTUAL_READ_P99, SloSnapshot, SloTracker};
use crate::placement::PlacementPolicy;
use crate::placement::latency::LatencyModel;
use crate::placement::topology::TopologyView;
use crate::store::kv::CrdtValue;
use crate::types::{KeyRange, NodeId, PolicyVersion};

use crate::network::PeerRegistry;
use crate::network::sync::{
    DeltaEntry, DeltaSyncRequest, DeltaSyncResponse, KeyDumpResponse, SyncError, SyncRequest,
    SyncResponse,
};

use super::types::{
    AnnounceRequest, AnnounceResponse, ApiError, AuthorityDefinitionResponse,
    CertifiedReadResponse, CertifiedWriteRequest, CertifiedWriteResponse, CrdtValueJson,
    EventualReadResponse, EventualWriteRequest, FrontierJson, JoinRequest, JoinResponse,
    LeaveRequest, LeaveResponse, PeerInfo, PingRequest, PingResponse, PlacementPolicyResponse,
    ProofBundleJson, RemovePolicyRequest, SetAuthorityDefinitionRequest, SetPlacementPolicyRequest,
    StatusResponse, VerifyProofRequest, VerifyProofResponse, VersionHistoryResponse, WriteResponse,
};

/// Shared application state for HTTP handlers.
pub struct AppState {
    pub eventual: Arc<Mutex<EventualApi>>,
    pub certified: Arc<Mutex<CertifiedApi>>,
    pub namespace: Arc<RwLock<SystemNamespace>>,
    pub metrics: Arc<RuntimeMetrics>,
    /// Peer registry for node join/leave bootstrap.
    /// `None` when peer tracking is not needed (e.g. unit tests).
    pub peers: Option<Arc<Mutex<PeerRegistry>>>,
    /// File path to persist the peer registry to on join/leave.
    /// `None` disables persistence (e.g. unit tests).
    pub peer_persist_path: Option<PathBuf>,
    /// File path to persist the system namespace after control-plane mutations.
    /// `None` disables persistence (e.g. unit tests).
    pub namespace_persist_path: Option<PathBuf>,
    /// Control-plane consensus for gating namespace updates (FR-009).
    pub consensus: Arc<Mutex<ControlPlaneConsensus>>,
    /// Optional shared token for authenticating internal API requests.
    /// When `Some`, all `/api/internal/*` routes require a matching
    /// `Authorization: Bearer <token>` header.
    pub internal_token: Option<String>,
    /// This node's own ID, used to include the seed in join responses.
    pub self_node_id: Option<NodeId>,
    /// This node's own advertised address, used to include the seed in join responses.
    pub self_addr: Option<String>,
    /// Latency model for multi-region placement optimization.
    /// `None` when latency tracking is not configured.
    pub latency_model: Option<Arc<std::sync::RwLock<LatencyModel>>>,
    /// Known cluster nodes for topology view.
    /// `None` when topology tracking is not configured.
    pub cluster_nodes: Option<Arc<std::sync::RwLock<Vec<crate::node::Node>>>>,
    /// SLO tracker for budget monitoring.
    pub slo_tracker: Arc<SloTracker>,
    /// Keyset registry for registry-based proof verification.
    /// When `Some`, the `/api/certified/verify` endpoint uses
    /// registry-based verification instead of trusting caller-supplied keys.
    pub keyset_registry: Option<Arc<std::sync::RwLock<KeysetRegistry>>>,
    /// Epoch configuration for keyset expiry checks.
    pub epoch_config: EpochConfig,
    /// Current epoch, used for keyset expiry checks during verification.
    pub current_epoch: Arc<std::sync::atomic::AtomicU64>,
}

// ---------------------------------------------------------------
// Eventual handlers
// ---------------------------------------------------------------

/// `POST /api/eventual/write`
///
/// Accepts a typed CRDT operation and applies it to the eventual store.
pub async fn eventual_write(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EventualWriteRequest>,
) -> Result<Json<WriteResponse>, ApiError> {
    let mut api = state.eventual.lock().await;

    match req {
        EventualWriteRequest::CounterInc { key } => {
            api.eventual_counter_inc(&key)?;
        }
        EventualWriteRequest::CounterDec { key } => {
            api.eventual_counter_dec(&key)?;
        }
        EventualWriteRequest::SetAdd { key, element } => {
            api.eventual_set_add(&key, element)?;
        }
        EventualWriteRequest::SetRemove { key, element } => {
            api.eventual_set_remove(&key, &element)?;
        }
        EventualWriteRequest::MapSet {
            key,
            map_key,
            map_value,
        } => {
            api.eventual_map_set(&key, map_key, map_value)?;
        }
        EventualWriteRequest::MapDelete { key, map_key } => {
            api.eventual_map_delete(&key, &map_key)?;
        }
        EventualWriteRequest::RegisterSet { key, value } => {
            api.eventual_register_set(&key, value)?;
        }
    }

    state
        .metrics
        .write_ops_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    Ok(Json(WriteResponse { ok: true }))
}

/// `GET /api/eventual/:key`
///
/// Returns the local CRDT value for the given key.
pub async fn get_eventual(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Json<EventualReadResponse> {
    let key = key.strip_prefix('/').unwrap_or(&key).to_string();
    let start = Instant::now();
    let api = state.eventual.lock().await;
    let value = api.get_eventual(&key).map(CrdtValueJson::from_crdt_value);
    drop(api);

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    state
        .slo_tracker
        .record_observation(SLO_EVENTUAL_READ_P99, elapsed_ms);

    Json(EventualReadResponse { key, value })
}

// ---------------------------------------------------------------
// Certified handlers
// ---------------------------------------------------------------

/// `POST /api/certified/write`
///
/// Writes a value that requires Authority majority certification.
pub async fn certified_write(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CertifiedWriteRequest>,
) -> Result<Json<CertifiedWriteResponse>, ApiError> {
    let on_timeout = match req.on_timeout.as_str() {
        "error" => OnTimeout::Error,
        "pending" => OnTimeout::Pending,
        other => {
            return Err(ApiError(CrdtError::InvalidArgument(format!(
                "invalid on_timeout value: {other}; expected \"error\" or \"pending\""
            ))));
        }
    };

    let crdt_value = json_to_crdt_value(&req.value)?;

    let mut api = state.certified.lock().await;
    let status = api.certified_write(req.key, crdt_value, on_timeout)?;

    state
        .metrics
        .write_ops_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    Ok(Json(CertifiedWriteResponse { status }))
}

/// `GET /api/certified/:key`
///
/// Returns the value with certification status, frontier, and proof bundle.
pub async fn get_certified(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Json<CertifiedReadResponse> {
    let key = key.strip_prefix('/').unwrap_or(&key).to_string();
    let start = Instant::now();
    let api = state.certified.lock().await;
    let read = api.get_certified(&key);

    let value = read.value.map(CrdtValueJson::from_crdt_value);
    let frontier = read.frontier.map(|f| FrontierJson {
        physical: f.physical,
        logical: f.logical,
        node_id: f.node_id,
    });

    let proof = read.proof.map(|p| {
        let certificate = p.certificate.map(|cert| {
            use super::types::{AuthoritySignatureJson, CertificateJson};
            CertificateJson {
                keyset_version: cert.keyset_version.0,
                signatures: cert
                    .signatures
                    .iter()
                    .map(|s| {
                        let pk_hex: String = s
                            .public_key
                            .as_bytes()
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect();
                        let sig_hex: String = s
                            .signature
                            .to_bytes()
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect();
                        AuthoritySignatureJson {
                            authority_id: s.authority_id.0.clone(),
                            public_key: pk_hex,
                            signature: sig_hex,
                            keyset_version: s.keyset_version.0,
                        }
                    })
                    .collect(),
            }
        });
        ProofBundleJson {
            key_range_prefix: p.key_range.prefix,
            frontier: FrontierJson {
                physical: p.frontier_hlc.physical,
                logical: p.frontier_hlc.logical,
                node_id: p.frontier_hlc.node_id,
            },
            policy_version: p.policy_version.0,
            contributing_authorities: p
                .contributing_authorities
                .into_iter()
                .map(|n| n.0)
                .collect(),
            total_authorities: p.total_authorities,
            certificate,
        }
    });

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    state
        .slo_tracker
        .record_observation(SLO_CERTIFIED_READ_P99, elapsed_ms);

    Json(CertifiedReadResponse {
        key,
        value,
        status: read.status,
        frontier,
        proof,
    })
}

/// `GET /api/status/:key`
///
/// Returns the certification status of the latest write for the given key.
pub async fn get_certification_status(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Json<StatusResponse> {
    let key = key.strip_prefix('/').unwrap_or(&key).to_string();
    let api = state.certified.lock().await;
    let status = api.get_certification_status(&key);

    Json(StatusResponse { key, status })
}

// ---------------------------------------------------------------
// Internal frontier handlers
// ---------------------------------------------------------------

/// `POST /api/internal/frontiers`
///
/// Receives frontier updates from a peer and applies them to the local
/// `AckFrontierSet`. Monotonicity is enforced by `AckFrontierSet::update()`.
pub async fn post_internal_frontiers(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, super::codec::SerializationError> {
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());

    let req: crate::network::frontier_sync::FrontierPushRequest =
        deserialize_internal(&body, content_type)?;

    let mut api = state.certified.lock().await;
    let mut accepted = 0;
    for frontier in req.frontiers {
        if api.update_frontier(frontier) {
            accepted += 1;
        }
    }
    let resp = crate::network::frontier_sync::FrontierPushResponse { accepted };
    internal_response(&resp, accept)
}

/// `GET /api/internal/frontiers`
///
/// Returns all frontiers currently tracked by this node.
pub async fn get_internal_frontiers(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, super::codec::SerializationError> {
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());

    let api = state.certified.lock().await;
    let frontiers = api.all_frontiers().into_iter().cloned().collect();
    let resp = crate::network::frontier_sync::FrontierPullResponse { frontiers };
    internal_response(&resp, accept)
}

// ---------------------------------------------------------------
// Control-plane handlers
// ---------------------------------------------------------------

/// `GET /api/control-plane/authorities`
///
/// Returns all authority definitions from the system namespace.
pub async fn list_authorities(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<AuthorityDefinitionResponse>> {
    let ns = state.namespace.read().unwrap();
    let defs: Vec<AuthorityDefinitionResponse> = ns
        .all_authority_definitions()
        .into_iter()
        .map(|def| AuthorityDefinitionResponse {
            key_range_prefix: def.key_range.prefix.clone(),
            authority_nodes: def.authority_nodes.iter().map(|n| n.0.clone()).collect(),
        })
        .collect();
    Json(defs)
}

/// `GET /api/control-plane/authorities/{prefix}`
///
/// Returns the authority definition for the given key range prefix.
pub async fn get_authority_definition(
    State(state): State<Arc<AppState>>,
    Path(prefix): Path<String>,
) -> Result<Json<AuthorityDefinitionResponse>, ApiError> {
    let ns = state.namespace.read().unwrap();
    let def = ns.get_authority_definition(&prefix).ok_or_else(|| {
        ApiError(CrdtError::KeyNotFound(format!(
            "authority definition: {prefix}"
        )))
    })?;
    Ok(Json(AuthorityDefinitionResponse {
        key_range_prefix: def.key_range.prefix.clone(),
        authority_nodes: def.authority_nodes.iter().map(|n| n.0.clone()).collect(),
    }))
}

/// `PUT /api/control-plane/authorities`
///
/// Sets an authority definition in the system namespace.
/// Requires majority approval from authority nodes (FR-009).
pub async fn set_authority_definition(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SetAuthorityDefinitionRequest>,
) -> Result<Json<AuthorityDefinitionResponse>, ApiError> {
    let def = AuthorityDefinition {
        key_range: KeyRange {
            prefix: req.key_range_prefix.clone(),
        },
        authority_nodes: req
            .authority_nodes
            .iter()
            .map(|n| NodeId(n.clone()))
            .collect(),
        auto_generated: false,
    };
    let approvals: Vec<NodeId> = req.approvals.iter().map(|a| NodeId(a.clone())).collect();

    // Hold consensus lock across both validation and mutation to prevent
    // TOCTOU races where a concurrent request could invalidate the approval
    // between the check and the namespace write.
    {
        let consensus = state.consensus.lock().await;
        consensus.propose_authority_update(def.clone(), &approvals)?;
        let mut ns = state.namespace.write().unwrap();
        ns.set_authority_definition(def);
    }

    // Persist namespace after mutation.
    persist_namespace(&state).await;

    Ok(Json(AuthorityDefinitionResponse {
        key_range_prefix: req.key_range_prefix,
        authority_nodes: req.authority_nodes,
    }))
}

/// `GET /api/control-plane/policies`
///
/// Returns all placement policies from the system namespace.
pub async fn list_policies(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<PlacementPolicyResponse>> {
    let ns = state.namespace.read().unwrap();
    let policies: Vec<PlacementPolicyResponse> = ns
        .all_placement_policies()
        .into_iter()
        .map(|p| PlacementPolicyResponse {
            key_range_prefix: p.key_range.prefix.clone(),
            version: p.version.0,
            replica_count: p.replica_count,
            required_tags: p.required_tags.iter().map(|t| t.0.clone()).collect(),
            forbidden_tags: p.forbidden_tags.iter().map(|t| t.0.clone()).collect(),
            allow_local_write_on_partition: p.allow_local_write_on_partition,
            certified: p.certified,
        })
        .collect();
    Json(policies)
}

/// `GET /api/control-plane/policies/{prefix}`
///
/// Returns the placement policy for the given key range prefix.
pub async fn get_policy(
    State(state): State<Arc<AppState>>,
    Path(prefix): Path<String>,
) -> Result<Json<PlacementPolicyResponse>, ApiError> {
    let ns = state.namespace.read().unwrap();
    let p = ns.get_placement_policy(&prefix).ok_or_else(|| {
        ApiError(CrdtError::KeyNotFound(format!(
            "placement policy: {prefix}"
        )))
    })?;
    Ok(Json(PlacementPolicyResponse {
        key_range_prefix: p.key_range.prefix.clone(),
        version: p.version.0,
        replica_count: p.replica_count,
        required_tags: p.required_tags.iter().map(|t| t.0.clone()).collect(),
        forbidden_tags: p.forbidden_tags.iter().map(|t| t.0.clone()).collect(),
        allow_local_write_on_partition: p.allow_local_write_on_partition,
        certified: p.certified,
    }))
}

/// `PUT /api/control-plane/policies`
///
/// Sets a placement policy in the system namespace.
/// Requires majority approval from authority nodes (FR-009).
pub async fn set_placement_policy(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SetPlacementPolicyRequest>,
) -> Result<Json<PlacementPolicyResponse>, ApiError> {
    if req.replica_count < 1 {
        return Err(ApiError(CrdtError::InvalidArgument(
            "replica_count must be at least 1".to_string(),
        )));
    }

    let approvals: Vec<NodeId> = req.approvals.iter().map(|a| NodeId(a.clone())).collect();

    // Build the policy template without a version; the actual version is
    // assigned atomically inside the namespace write lock below to prevent
    // concurrent requests from stamping different policies with the same
    // version number.
    let build_policy = |version: PolicyVersion| {
        let mut policy = PlacementPolicy::new(
            version,
            KeyRange {
                prefix: req.key_range_prefix.clone(),
            },
            req.replica_count,
        );

        if !req.required_tags.is_empty() {
            policy = policy.with_required_tags(
                req.required_tags
                    .iter()
                    .map(|t| crate::types::Tag(t.clone()))
                    .collect(),
            );
        }
        if !req.forbidden_tags.is_empty() {
            policy = policy.with_forbidden_tags(
                req.forbidden_tags
                    .iter()
                    .map(|t| crate::types::Tag(t.clone()))
                    .collect(),
            );
        }
        policy = policy.with_local_write_on_partition(req.allow_local_write_on_partition);
        policy = policy.with_certified(req.certified);
        policy
    };

    // Hold consensus lock across both validation and mutation to prevent
    // TOCTOU races where a concurrent request could invalidate the approval
    // between the check and the namespace write.
    let policy = {
        let provisional = build_policy(PolicyVersion(0));
        let consensus = state.consensus.lock().await;
        consensus.propose_policy_update(provisional, &approvals)?;

        // Atomically read the current version, create the policy, and apply it
        // inside a single write-lock scope to prevent version collisions.
        let mut ns = state.namespace.write().unwrap();
        let current_version = ns.version().0;
        let policy = build_policy(PolicyVersion(current_version + 1));
        ns.set_placement_policy(policy.clone());
        policy
    };

    // Persist namespace after mutation.
    persist_namespace(&state).await;

    let resp = PlacementPolicyResponse {
        key_range_prefix: policy.key_range.prefix.clone(),
        version: policy.version.0,
        replica_count: policy.replica_count,
        required_tags: req.required_tags,
        forbidden_tags: req.forbidden_tags,
        allow_local_write_on_partition: policy.allow_local_write_on_partition,
        certified: policy.certified,
    };

    Ok(Json(resp))
}

/// `DELETE /api/control-plane/policies/{prefix}`
///
/// Removes the placement policy for the given key range prefix.
/// Requires majority approval from authority nodes (FR-009).
pub async fn remove_policy(
    State(state): State<Arc<AppState>>,
    Path(prefix): Path<String>,
    Json(req): Json<RemovePolicyRequest>,
) -> Result<Json<PlacementPolicyResponse>, ApiError> {
    let approvals: Vec<NodeId> = req.approvals.iter().map(|a| NodeId(a.clone())).collect();

    // Hold consensus lock across both validation and mutation to prevent
    // TOCTOU races where a concurrent request could invalidate the approval
    // between the check and the namespace write.
    let removed = {
        let consensus = state.consensus.lock().await;
        consensus.propose_policy_removal(&prefix, &approvals)?;
        let mut ns = state.namespace.write().unwrap();
        ns.remove_placement_policy(&prefix)
    };

    let removed = removed.ok_or_else(|| {
        ApiError(CrdtError::KeyNotFound(format!(
            "placement policy: {prefix}"
        )))
    })?;

    // Persist namespace after mutation.
    persist_namespace(&state).await;

    Ok(Json(PlacementPolicyResponse {
        key_range_prefix: removed.key_range.prefix.clone(),
        version: removed.version.0,
        replica_count: removed.replica_count,
        required_tags: removed.required_tags.iter().map(|t| t.0.clone()).collect(),
        forbidden_tags: removed.forbidden_tags.iter().map(|t| t.0.clone()).collect(),
        allow_local_write_on_partition: removed.allow_local_write_on_partition,
        certified: removed.certified,
    }))
}

/// `GET /api/control-plane/versions`
///
/// Returns the version history of the system namespace.
pub async fn get_version_history(
    State(state): State<Arc<AppState>>,
) -> Json<VersionHistoryResponse> {
    let ns = state.namespace.read().unwrap();
    Json(VersionHistoryResponse {
        current_version: ns.version().0,
        history: ns.version_history().iter().map(|v| v.0).collect(),
    })
}

// ---------------------------------------------------------------
// Verification handler
// ---------------------------------------------------------------

/// `POST /api/certified/verify`
///
/// Accepts a proof bundle and returns the verification result.
/// External clients can use this to independently verify that a
/// proof bundle represents genuine Authority consensus.
pub async fn verify_proof(
    State(state): State<Arc<AppState>>,
    Json(req): Json<VerifyProofRequest>,
) -> Result<Json<VerifyProofResponse>, (axum::http::StatusCode, String)> {
    use crate::api::certified::ProofBundle;
    use crate::authority::certificate::{AuthoritySignature, KeysetVersion, MajorityCertificate};
    use crate::authority::verifier;
    use crate::hlc::HlcTimestamp;
    use crate::types::{KeyRange, NodeId, PolicyVersion};

    // Registry-based verification is required; reject if no registry is configured.
    let registry_lock = state.keyset_registry.as_ref().ok_or((
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "keyset registry not configured; cannot verify proofs".to_string(),
    ))?;

    let key_range = KeyRange {
        prefix: req.key_range_prefix,
    };
    let frontier_hlc = HlcTimestamp {
        physical: req.frontier.physical,
        logical: req.frontier.logical,
        node_id: req.frontier.node_id,
    };
    let policy_version = PolicyVersion(req.policy_version);

    // Reconstruct the certificate from the HTTP payload, if provided.
    let certificate = req.certificate.and_then(|cert_json| {
        let mut cert = MajorityCertificate::new(
            key_range.clone(),
            frontier_hlc.clone(),
            policy_version,
            KeysetVersion(cert_json.keyset_version),
        );
        for sig_json in &cert_json.signatures {
            let pk_bytes = hex_to_bytes_32(&sig_json.public_key)?;
            let sig_bytes = hex_to_bytes_64(&sig_json.signature)?;
            let public_key = ed25519_dalek::VerifyingKey::from_bytes(&pk_bytes).ok()?;
            let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);
            cert.add_signature(AuthoritySignature {
                authority_id: NodeId(sig_json.authority_id.clone()),
                public_key,
                signature,
                keyset_version: KeysetVersion(sig_json.keyset_version),
            });
        }
        Some(cert)
    });

    let bundle = ProofBundle {
        key_range,
        frontier_hlc,
        policy_version,
        contributing_authorities: req
            .contributing_authorities
            .into_iter()
            .map(NodeId)
            .collect(),
        total_authorities: req.total_authorities,
        certificate,
    };

    let registry = registry_lock.read().unwrap();
    let current_epoch = state
        .current_epoch
        .load(std::sync::atomic::Ordering::Relaxed);
    let result = verifier::verify_proof_with_registry(
        &bundle,
        &registry,
        current_epoch,
        &state.epoch_config,
        None,
        0,
    );

    Ok(Json(VerifyProofResponse {
        valid: result.valid,
        has_majority: result.has_majority,
        contributing_count: result.contributing_count,
        required_count: result.required_count,
    }))
}

/// Decode a hex string into a 32-byte array. Returns `None` on failure.
fn hex_to_bytes_32(hex: &str) -> Option<[u8; 32]> {
    let bytes = hex_to_bytes(hex)?;
    bytes.try_into().ok()
}

/// Decode a hex string into a 64-byte array. Returns `None` on failure.
fn hex_to_bytes_64(hex: &str) -> Option<[u8; 64]> {
    let bytes = hex_to_bytes(hex)?;
    bytes.try_into().ok()
}

/// Decode a hex string into a byte vector. Returns `None` on failure.
fn hex_to_bytes(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

// ---------------------------------------------------------------
// Internal sync handlers
// ---------------------------------------------------------------

/// `POST /api/internal/sync`
///
/// Receives CRDT values from a remote peer and merges them into the
/// local eventual store using `merge_remote`.
pub async fn internal_sync(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, super::codec::SerializationError> {
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());

    let req: SyncRequest = deserialize_internal(&body, content_type)?;

    let mut api = state.eventual.lock().await;
    let mut merged = 0;
    let mut errors = Vec::new();

    for (key, value) in &req.entries {
        match api.merge_remote(key.clone(), value) {
            Ok(()) => merged += 1,
            Err(e) => {
                errors.push(SyncError {
                    key: key.clone(),
                    error: e.to_string(),
                });
            }
        }
    }

    let resp = SyncResponse { merged, errors };
    internal_response(&resp, accept)
}

/// `GET /api/internal/keys`
///
/// Returns all key-value pairs from the eventual store together with the
/// store's current frontier HLC. Used by remote peers for pull-based
/// anti-entropy sync. The frontier allows the requester to correctly
/// initialise its peer frontier tracking after a full sync.
pub async fn internal_keys(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, super::codec::SerializationError> {
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());

    let api = state.eventual.lock().await;
    let store = api.store();
    let mut entries = std::collections::HashMap::new();
    let mut timestamps = std::collections::HashMap::new();
    for (k, v, hlc) in store.all_entries_with_hlc() {
        entries.insert(k.clone(), v.clone());
        timestamps.insert(k.clone(), hlc.clone());
    }
    // Include entries without tracked timestamps (rare, but possible for
    // stores migrated from older versions). Only iterate entries not already
    // covered by `all_entries_with_hlc()` to avoid redundant cloning.
    for (k, v) in store.all_entries() {
        if !entries.contains_key(k) {
            entries.insert(k.clone(), v.clone());
        }
    }
    let frontier = store.current_frontier();

    let resp = KeyDumpResponse {
        entries,
        frontier,
        timestamps,
    };
    internal_response(&resp, accept)
}

/// `POST /api/internal/sync/delta`
///
/// Receives a delta sync request with a frontier timestamp and returns
/// all entries modified after that frontier. Used for incremental
/// anti-entropy sync.
pub async fn internal_delta_sync(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, super::codec::SerializationError> {
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());

    let req: DeltaSyncRequest = deserialize_internal(&body, content_type)?;

    let api = state.eventual.lock().await;
    let store = api.store();

    let entries: Vec<DeltaEntry> = store
        .delta_entries_since(&req.frontier)
        .into_iter()
        .map(|(key, value, hlc)| DeltaEntry { key, value, hlc })
        .collect();

    let sender_frontier = store.current_frontier();

    let resp = DeltaSyncResponse {
        entries,
        sender_frontier,
    };
    internal_response(&resp, accept)
}

// ---------------------------------------------------------------
// Internal join/leave handlers
// ---------------------------------------------------------------

/// `POST /api/internal/join`
///
/// A new node sends its configuration to this (seed) node to join the
/// cluster. The seed node adds the joining node to its peer registry
/// and returns the current peer list plus a system namespace snapshot.
pub async fn internal_join(
    State(state): State<Arc<AppState>>,
    Json(req): Json<JoinRequest>,
) -> Result<Json<JoinResponse>, ApiError> {
    use crate::network::PeerConfig;

    // Validate the caller-supplied address to prevent SSRF.
    validate_peer_address(&req.address).map_err(|e| ApiError(CrdtError::InvalidArgument(e)))?;

    let peers_registry = state.peers.as_ref().ok_or_else(|| {
        ApiError(CrdtError::Internal(
            "peer registry not configured".to_string(),
        ))
    })?;

    let joining_node_id = NodeId(req.node_id.clone());

    // Add the joining node (or update its address if already known) and
    // snapshot peer list under one lock acquisition, then release the lock
    // before performing blocking I/O.
    let (peer_list, persist_snapshot) = {
        let mut registry = peers_registry.lock().await;

        // If the peer already exists, update its address in case it restarted
        // with a new IP.
        if registry.get_peer(&joining_node_id).is_some() {
            registry.update_address(&joining_node_id, &req.address);
        } else {
            registry
                .add_peer(PeerConfig {
                    node_id: joining_node_id.clone(),
                    addr: req.address.clone(),
                })
                .map_err(|e| ApiError(CrdtError::InvalidArgument(e.to_string())))?;
        }

        // Snapshot the serialised state while we hold the lock.
        let snapshot = state
            .peer_persist_path
            .as_ref()
            .and_then(|_| serde_json::to_string_pretty(&*registry).ok());

        // Build the peer list response while the lock is held.
        let mut list: Vec<PeerInfo> = registry
            .all_peers_owned()
            .into_iter()
            .map(|p| PeerInfo {
                node_id: p.node_id.0,
                address: p.addr.clone(),
            })
            .collect();

        // Include the seed node (self) so the joiner has a complete peer view.
        if let (Some(self_id), Some(self_addr)) = (&state.self_node_id, &state.self_addr) {
            let already_present = list.iter().any(|p| p.node_id == self_id.0);
            if !already_present {
                list.push(PeerInfo {
                    node_id: self_id.0.clone(),
                    address: self_addr.clone(),
                });
            }
        }

        list.sort_by(|a, b| a.node_id.cmp(&b.node_id));

        (list, snapshot)
    };
    // Lock is released here — persist outside the critical section.

    if let Some(path) = &state.peer_persist_path
        && let Some(json) = persist_snapshot
    {
        let path = path.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || write_atomic(&path, json.as_bytes()))
            .await
            .unwrap_or_else(|e| Err(e.to_string()))
        {
            eprintln!("warning: failed to persist peer registry: {e}");
        }
    }

    let ns_snapshot = {
        let ns = state.namespace.read().unwrap();
        serde_json::to_value(&*ns).unwrap_or(serde_json::Value::Null)
    };

    Ok(Json(JoinResponse {
        peers: peer_list,
        namespace: ns_snapshot,
    }))
}

/// `POST /api/internal/leave`
///
/// A node sends its ID to gracefully depart the cluster. The receiving
/// node removes the departing node from its peer registry.
pub async fn internal_leave(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LeaveRequest>,
) -> Result<Json<LeaveResponse>, ApiError> {
    let peers_registry = state.peers.as_ref().ok_or_else(|| {
        ApiError(CrdtError::Internal(
            "peer registry not configured".to_string(),
        ))
    })?;

    let leaving_node_id = NodeId(req.node_id);

    // Remove peer and snapshot serialised state under the lock, then
    // release before performing blocking I/O.
    let (removed, persist_snapshot) = {
        let mut registry = peers_registry.lock().await;
        let removed = registry
            .remove_peer(&leaving_node_id)
            .map_err(|e| ApiError(CrdtError::InvalidArgument(e.to_string())))?;

        let snapshot = if removed.is_some() {
            state
                .peer_persist_path
                .as_ref()
                .and_then(|_| serde_json::to_string_pretty(&*registry).ok())
        } else {
            None
        };

        (removed, snapshot)
    };
    // Lock is released here.

    if let Some(path) = &state.peer_persist_path
        && let Some(json) = persist_snapshot
    {
        let path = path.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || write_atomic(&path, json.as_bytes()))
            .await
            .unwrap_or_else(|e| Err(e.to_string()))
        {
            eprintln!("warning: failed to persist peer registry: {e}");
        }
    }

    Ok(Json(LeaveResponse {
        success: removed.is_some(),
    }))
}

// ---------------------------------------------------------------
// Internal announce handler
// ---------------------------------------------------------------

/// `POST /api/internal/announce`
///
/// Receives a membership announcement from a peer. If the peer is
/// joining, it is added to the local peer registry. If leaving, it
/// is removed.
pub async fn internal_announce(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AnnounceRequest>,
) -> Result<Json<AnnounceResponse>, ApiError> {
    use crate::network::PeerConfig;

    // Validate the caller-supplied address to prevent SSRF.
    validate_peer_address(&req.address).map_err(|e| ApiError(CrdtError::InvalidArgument(e)))?;

    let peers_registry = state.peers.as_ref().ok_or_else(|| {
        ApiError(CrdtError::Internal(
            "peer registry not configured".to_string(),
        ))
    })?;

    let announcing_node_id = NodeId(req.node_id.clone());

    let (result, persist_snapshot) = if req.joining {
        let mut registry = peers_registry.lock().await;
        // If the peer is already known, update its address in case it
        // restarted with a new IP, then return success.
        if registry.get_peer(&announcing_node_id).is_some() {
            registry.update_address(&announcing_node_id, &req.address);
            let snapshot = state
                .peer_persist_path
                .as_ref()
                .and_then(|_| serde_json::to_string_pretty(&*registry).ok());
            drop(registry);
            // Persist outside the lock if the address changed.
            if let Some(path) = &state.peer_persist_path
                && let Some(json) = snapshot
            {
                let path = path.clone();
                let _ =
                    tokio::task::spawn_blocking(move || write_atomic(&path, json.as_bytes())).await;
            }
            return Ok(Json(AnnounceResponse { accepted: true }));
        }
        let result = match registry.add_peer(PeerConfig {
            node_id: announcing_node_id,
            addr: req.address,
        }) {
            Ok(()) => Ok(Json(AnnounceResponse { accepted: true })),
            // If it is our own ID, silently accept.
            Err(crate::network::PeerError::SelfInPeerList(_)) => {
                Ok(Json(AnnounceResponse { accepted: true }))
            }
            Err(e) => Err(ApiError(CrdtError::InvalidArgument(e.to_string()))),
        };
        let snapshot = state
            .peer_persist_path
            .as_ref()
            .and_then(|_| serde_json::to_string_pretty(&*registry).ok());
        (result, snapshot)
    } else {
        let mut registry = peers_registry.lock().await;
        let result = match registry.remove_peer(&announcing_node_id) {
            Ok(_) => Ok(Json(AnnounceResponse { accepted: true })),
            Err(crate::network::PeerError::SelfInPeerList(_)) => {
                Ok(Json(AnnounceResponse { accepted: true }))
            }
            Err(e) => Err(ApiError(CrdtError::InvalidArgument(e.to_string()))),
        };
        let snapshot = state
            .peer_persist_path
            .as_ref()
            .and_then(|_| serde_json::to_string_pretty(&*registry).ok());
        (result, snapshot)
    };

    // Persist outside the lock.
    if let Some(path) = &state.peer_persist_path
        && let Some(json) = persist_snapshot
    {
        let path = path.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || write_atomic(&path, json.as_bytes()))
            .await
            .unwrap_or_else(|e| Err(e.to_string()))
        {
            eprintln!("warning: failed to persist peer registry after announce: {e}");
        }
    }

    result
}

// ---------------------------------------------------------------
// Internal ping handler
// ---------------------------------------------------------------

/// `POST /api/internal/ping`
///
/// Lightweight gossip endpoint for peer list exchange. The sender
/// provides its known peers; the receiver reconciles and returns
/// its own known peers. Both sides can detect and fill gaps.
pub async fn internal_ping(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PingRequest>,
) -> Result<Json<PingResponse>, ApiError> {
    use crate::network::PeerConfig;

    // Validate the sender's address to prevent SSRF.
    validate_peer_address(&req.sender_addr).map_err(|e| ApiError(CrdtError::InvalidArgument(e)))?;

    // Validate all peer addresses in the known_peers list.
    for peer in &req.known_peers {
        validate_peer_address(&peer.address)
            .map_err(|e| ApiError(CrdtError::InvalidArgument(e)))?;
    }

    let peers_registry = state.peers.as_ref().ok_or_else(|| {
        ApiError(CrdtError::Internal(
            "peer registry not configured".to_string(),
        ))
    })?;

    // Maximum number of new peers that can be added from a single ping
    // exchange to limit peer-list poisoning.
    const MAX_NEW_PEERS_PER_PING: usize = 10;

    // Only reconcile peers from the sender's list if the sender is already
    // a known peer (or is self). This prevents unauthenticated nodes from
    // injecting arbitrary peers via ping.
    let sender_nid = NodeId(req.sender_id.clone());
    let peers_changed = {
        let mut registry = peers_registry.lock().await;
        let mut changed = false;

        let sender_is_known = registry.get_peer(&sender_nid).is_some()
            || state.self_node_id.as_ref() == Some(&sender_nid);

        // Update or add the sender. Only add an unknown sender if the
        // request passed bearer-token authentication (i.e. internal_token
        // is configured and was validated by the middleware). Without auth,
        // unknown nodes must not be able to inject themselves into the
        // registry via a bare ping.
        if registry.get_peer(&sender_nid).is_some() {
            if registry.update_address(&sender_nid, &req.sender_addr) {
                changed = true;
            }
        } else if state.internal_token.as_ref().is_some_and(|t| !t.is_empty()) {
            // The request reached us through the auth middleware, so the
            // sender has a valid token — safe to add as a new peer.
            if registry
                .add_peer(PeerConfig {
                    node_id: sender_nid.clone(),
                    addr: req.sender_addr.clone(),
                })
                .is_ok()
            {
                changed = true;
            }
        } else {
            tracing::warn!(
                sender = %req.sender_id,
                "ping from unknown sender rejected: no auth configured"
            );
        }

        // Only accept peer list from known senders.
        if sender_is_known {
            let mut new_peers_added: usize = 0;
            for peer_info in &req.known_peers {
                let peer_nid = NodeId(peer_info.node_id.clone());
                if registry.get_peer(&peer_nid).is_some() {
                    // Update address if it changed.
                    if registry.update_address(&peer_nid, &peer_info.address) {
                        changed = true;
                    }
                } else if new_peers_added < MAX_NEW_PEERS_PER_PING {
                    // Ignore errors (e.g. self-in-peer-list, duplicates).
                    if registry
                        .add_peer(PeerConfig {
                            node_id: peer_nid,
                            addr: peer_info.address.clone(),
                        })
                        .is_ok()
                    {
                        changed = true;
                        new_peers_added += 1;
                    }
                }
            }
        }

        changed
    };

    // Persist registry if it changed (Codex P2).
    if peers_changed && let Some(path) = &state.peer_persist_path {
        let snapshot = {
            let registry = peers_registry.lock().await;
            serde_json::to_string_pretty(&*registry).ok()
        };
        if let Some(json) = snapshot {
            let path = path.clone();
            if let Err(e) =
                tokio::task::spawn_blocking(move || write_atomic(&path, json.as_bytes()))
                    .await
                    .unwrap_or_else(|e| Err(e.to_string()))
            {
                eprintln!("warning: failed to persist peer registry after ping: {e}");
            }
        }
    }

    // Build our own peer list to return.
    let my_peers = {
        let registry = peers_registry.lock().await;
        let mut list: Vec<PeerInfo> = registry
            .all_peers_owned()
            .into_iter()
            .map(|p| PeerInfo {
                node_id: p.node_id.0,
                address: p.addr,
            })
            .collect();

        // Include self so the remote gets a complete view.
        if let (Some(self_id), Some(self_addr)) = (&state.self_node_id, &state.self_addr) {
            let already_present = list.iter().any(|p| p.node_id == self_id.0);
            if !already_present {
                list.push(PeerInfo {
                    node_id: self_id.0.clone(),
                    address: self_addr.clone(),
                });
            }
        }

        list.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        list
    };

    Ok(Json(PingResponse {
        known_peers: my_peers,
    }))
}

// ---------------------------------------------------------------
// Topology handler
// ---------------------------------------------------------------

/// `GET /api/topology`
///
/// Returns the cluster topology view grouped by region, including
/// inter-region latency information.
pub async fn get_topology(State(state): State<Arc<AppState>>) -> Json<TopologyView> {
    let nodes = state
        .cluster_nodes
        .as_ref()
        .map(|n| n.read().unwrap().clone())
        .unwrap_or_default();

    let latency_model = state
        .latency_model
        .as_ref()
        .map(|m| m.read().unwrap().clone())
        .unwrap_or_default();

    Json(TopologyView::build(&nodes, &latency_model))
}

// ---------------------------------------------------------------
// Metrics handler
// ---------------------------------------------------------------

/// `GET /api/metrics`
///
/// Returns a snapshot of runtime operational metrics (pending count,
/// certification latency, frontier skew, sync failure rate).
pub async fn get_metrics(State(state): State<Arc<AppState>>) -> Json<MetricsSnapshot> {
    Json(state.metrics.snapshot())
}

/// `GET /api/slo`
///
/// Returns a snapshot of all SLO budgets for operational monitoring.
pub async fn get_slo(State(state): State<Arc<AppState>>) -> Json<SloSnapshot> {
    Json(state.slo_tracker.snapshot())
}

/// `GET /healthz`
///
/// Simple health check endpoint for load balancers and orchestrators.
/// Returns 200 OK with a static JSON body. Placed outside auth middleware
/// so that unauthenticated probes succeed.
pub async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

/// Write `data` to `path` atomically: write to a uniquely-named sibling temp
/// file, fsync, then rename. The unique suffix (pid + counter) avoids temp
/// file contention when multiple handlers persist concurrently.
fn write_atomic(path: &std::path::Path, data: &[u8]) -> Result<(), String> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_name = format!(
        ".tmp.{}.{}.{}",
        path.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id(),
        seq,
    );
    let tmp_path = path.with_file_name(tmp_name);
    let mut file = std::fs::File::create(&tmp_path).map_err(|e| e.to_string())?;
    file.write_all(data).map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    drop(file);
    if let Err(e) = std::fs::rename(&tmp_path, path) {
        // Clean up stranded temp file on rename failure.
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e.to_string());
    }
    // Fsync the parent directory so the rename is durable.
    if let Some(parent) = path.parent()
        && let Ok(dir) = std::fs::File::open(parent)
    {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Validate that `addr` is a bare host:port address.
///
/// Rejects addresses containing schemes (`http://`, `ftp://`), paths, or query
/// strings. Only `host:port` or `ip:port` format is accepted. This prevents
/// SSRF via caller-supplied addresses in join/leave/announce/ping requests.
fn validate_peer_address(addr: &str) -> Result<(), String> {
    // Must not contain scheme indicators.
    if addr.contains("://") {
        return Err(format!("address must not contain a scheme: {addr}"));
    }
    // Must not contain path or query components.
    if addr.contains('/') || addr.contains('?') || addr.contains('#') {
        return Err(format!(
            "address must not contain path or query components: {addr}"
        ));
    }
    // Must contain at least one ':' separating host from port.
    // For IPv6, the port section follows the last colon after ']'.
    let port_sep = if addr.starts_with('[') {
        // IPv6 bracket notation: [::1]:3000
        addr.rfind("]:")
            .map(|i| i + 1)
            .ok_or_else(|| format!("invalid IPv6 bracket address: {addr}"))?
    } else {
        addr.rfind(':')
            .ok_or_else(|| format!("address must be host:port format: {addr}"))?
    };

    let port_str = &addr[port_sep + 1..];
    if port_str.is_empty() {
        return Err(format!("address must include a port number: {addr}"));
    }
    port_str
        .parse::<u16>()
        .map_err(|_| format!("invalid port number in address: {addr}"))?;

    // Host part must not be empty.
    let host = &addr[..port_sep];
    if host.is_empty() {
        return Err(format!("address must include a host: {addr}"));
    }
    // Must not contain whitespace.
    if addr.chars().any(|c| c.is_whitespace()) {
        return Err(format!("address must not contain whitespace: {addr}"));
    }

    Ok(())
}

/// Persist the system namespace to disk (if a path is configured).
///
/// Serialises the namespace under a read-lock, then writes atomically via
/// `write_atomic` on a blocking thread. Errors are logged but not propagated
/// because namespace persistence is best-effort.
async fn persist_namespace(state: &AppState) {
    if let Some(path) = &state.namespace_persist_path {
        let json = {
            let ns = state.namespace.read().unwrap();
            match serde_json::to_string_pretty(&*ns) {
                Ok(j) => j,
                Err(e) => {
                    eprintln!("warning: failed to serialise system namespace: {e}");
                    return;
                }
            }
        };
        let path = path.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || write_atomic(&path, json.as_bytes()))
            .await
            .unwrap_or_else(|e| Err(e.to_string()))
        {
            eprintln!("warning: failed to persist system namespace: {e}");
        }
    }
}

/// Maximum allowed absolute value for counter initialization via HTTP API.
///
/// Values exceeding this limit are rejected with `InvalidArgument` to prevent
/// resource exhaustion. This is a defense-in-depth measure; the O(1) constructor
/// already prevents CPU-based DoS.
const MAX_COUNTER_MAGNITUDE: i64 = 1_000_000_000;

/// Convert a JSON CRDT value representation into an internal `CrdtValue`.
///
/// For Counter, creates a PnCounter with the specified value using O(1)
/// `PnCounter::from_value` (not by looping). For Set/Map/Register,
/// constructs the appropriate CRDT type from the provided data.
fn json_to_crdt_value(json: &CrdtValueJson) -> Result<CrdtValue, CrdtError> {
    use crate::crdt::lww_register::LwwRegister;
    use crate::crdt::or_map::OrMap;
    use crate::crdt::or_set::OrSet;
    use crate::hlc::Hlc;
    use crate::types::NodeId;

    let writer = NodeId("http-writer".into());

    match json {
        CrdtValueJson::Counter { value } => {
            if value.unsigned_abs() > MAX_COUNTER_MAGNITUDE as u64 {
                return Err(CrdtError::InvalidArgument(format!(
                    "counter magnitude {} exceeds maximum allowed value {MAX_COUNTER_MAGNITUDE}",
                    value.unsigned_abs()
                )));
            }
            let counter = PnCounter::from_value(&writer, *value);
            Ok(CrdtValue::Counter(counter))
        }
        CrdtValueJson::Set { elements } => {
            let mut set = OrSet::new();
            for elem in elements {
                set.add(elem.clone(), &writer);
            }
            Ok(CrdtValue::Set(set))
        }
        CrdtValueJson::Map { entries } => {
            let mut map = OrMap::new();
            let mut clock = Hlc::new("http-writer".into());
            for (k, v) in entries {
                let ts = clock.now();
                map.set(k.clone(), v.clone(), ts, &writer);
            }
            Ok(CrdtValue::Map(map))
        }
        CrdtValueJson::Register { value } => {
            let mut reg = LwwRegister::new();
            if let Some(v) = value {
                let mut clock = Hlc::new("http-writer".into());
                let ts = clock.now();
                reg.set(v.clone(), ts);
            }
            Ok(CrdtValue::Register(reg))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_to_crdt_counter_positive() {
        let json = CrdtValueJson::Counter { value: 5 };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Counter(c) => assert_eq!(c.value(), 5),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_negative() {
        let json = CrdtValueJson::Counter { value: -3 };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Counter(c) => assert_eq!(c.value(), -3),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_zero() {
        let json = CrdtValueJson::Counter { value: 0 };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Counter(c) => assert_eq!(c.value(), 0),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_set() {
        let json = CrdtValueJson::Set {
            elements: vec!["a".into(), "b".into()],
        };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Set(s) => {
                assert!(s.contains(&"a".to_string()));
                assert!(s.contains(&"b".to_string()));
                assert_eq!(s.len(), 2);
            }
            other => panic!("expected Set, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_map() {
        let mut entries = std::collections::HashMap::new();
        entries.insert("name".into(), "db".into());
        let json = CrdtValueJson::Map { entries };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Map(m) => {
                assert_eq!(m.get(&"name".to_string()), Some(&"db".to_string()));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_register_with_value() {
        let json = CrdtValueJson::Register {
            value: Some("hello".into()),
        };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Register(r) => {
                assert_eq!(r.get(), Some(&"hello".to_string()));
            }
            other => panic!("expected Register, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_register_empty() {
        let json = CrdtValueJson::Register { value: None };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Register(r) => {
                assert_eq!(r.get(), None);
            }
            other => panic!("expected Register, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_large_value_is_o1() {
        // This would take O(999_999_999) iterations with the old loop-based
        // approach. With from_value it completes instantly.
        let json = CrdtValueJson::Counter { value: 999_999_999 };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Counter(c) => assert_eq!(c.value(), 999_999_999),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_large_negative_is_o1() {
        let json = CrdtValueJson::Counter {
            value: -999_999_999,
        };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Counter(c) => assert_eq!(c.value(), -999_999_999),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_exceeds_max_magnitude() {
        let json = CrdtValueJson::Counter {
            value: 1_000_000_001,
        };
        let err = json_to_crdt_value(&json).unwrap_err();
        match err {
            CrdtError::InvalidArgument(msg) => {
                assert!(msg.contains("exceeds maximum"), "unexpected message: {msg}");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_negative_exceeds_max_magnitude() {
        let json = CrdtValueJson::Counter {
            value: -1_000_000_001,
        };
        let err = json_to_crdt_value(&json).unwrap_err();
        match err {
            CrdtError::InvalidArgument(msg) => {
                assert!(msg.contains("exceeds maximum"), "unexpected message: {msg}");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_at_max_magnitude_is_ok() {
        // Exactly at the boundary should succeed.
        let json = CrdtValueJson::Counter {
            value: 1_000_000_000,
        };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Counter(c) => assert_eq!(c.value(), 1_000_000_000),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    // ---------------------------------------------------------------
    // validate_peer_address tests
    // ---------------------------------------------------------------

    #[test]
    fn validate_peer_address_accepts_docker_hostnames_with_hyphens() {
        // Docker container names use hyphens (e.g. asteroidb-node-2:3000).
        assert!(validate_peer_address("asteroidb-node-1:3000").is_ok());
        assert!(validate_peer_address("asteroidb-node-2:3000").is_ok());
        assert!(validate_peer_address("asteroidb-node-3:3000").is_ok());
    }

    #[test]
    fn validate_peer_address_accepts_ip_port() {
        assert!(validate_peer_address("127.0.0.1:3000").is_ok());
        assert!(validate_peer_address("0.0.0.0:3000").is_ok());
        assert!(validate_peer_address("192.168.1.1:8080").is_ok());
    }

    #[test]
    fn validate_peer_address_accepts_ipv6() {
        assert!(validate_peer_address("[::1]:3000").is_ok());
    }

    #[test]
    fn validate_peer_address_rejects_scheme() {
        assert!(validate_peer_address("http://localhost:3000").is_err());
        assert!(validate_peer_address("ftp://host:22").is_err());
    }

    #[test]
    fn validate_peer_address_rejects_path() {
        assert!(validate_peer_address("localhost:3000/secret").is_err());
    }

    #[test]
    fn validate_peer_address_rejects_missing_port() {
        assert!(validate_peer_address("localhost").is_err());
    }
}
