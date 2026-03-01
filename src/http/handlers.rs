use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use axum::Json;
use axum::extract::{Path, State};
use tokio::sync::Mutex;

use crate::api::certified::{CertifiedApi, OnTimeout};
use crate::api::eventual::EventualApi;
use crate::control_plane::consensus::ControlPlaneConsensus;
use crate::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use crate::crdt::pn_counter::PnCounter;
use crate::error::CrdtError;
use crate::ops::metrics::{MetricsSnapshot, RuntimeMetrics};
use crate::placement::PlacementPolicy;
use crate::store::kv::CrdtValue;
use crate::types::{KeyRange, NodeId, PolicyVersion};

use crate::network::PeerRegistry;
use crate::network::sync::{
    DeltaEntry, DeltaSyncRequest, DeltaSyncResponse, KeyDumpResponse, SyncError, SyncRequest,
    SyncResponse,
};

use super::types::{
    ApiError, AuthorityDefinitionResponse, CertifiedReadResponse, CertifiedWriteRequest,
    CertifiedWriteResponse, CrdtValueJson, EventualReadResponse, EventualWriteRequest,
    FrontierJson, JoinRequest, JoinResponse, LeaveRequest, LeaveResponse, PeerInfo,
    PlacementPolicyResponse, ProofBundleJson, SetAuthorityDefinitionRequest,
    SetPlacementPolicyRequest, StatusResponse, VerifyProofRequest, VerifyProofResponse,
    VersionHistoryResponse, WriteResponse,
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
    /// Control-plane consensus for gating namespace updates (FR-009).
    pub consensus: Arc<Mutex<ControlPlaneConsensus>>,
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

    Ok(Json(WriteResponse { ok: true }))
}

/// `GET /api/eventual/:key`
///
/// Returns the local CRDT value for the given key.
pub async fn get_eventual(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Json<EventualReadResponse> {
    let api = state.eventual.lock().await;
    let value = api.get_eventual(&key).map(CrdtValueJson::from_crdt_value);

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

    Ok(Json(CertifiedWriteResponse { status }))
}

/// `GET /api/certified/:key`
///
/// Returns the value with certification status, frontier, and proof bundle.
pub async fn get_certified(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Json<CertifiedReadResponse> {
    let api = state.certified.lock().await;
    let read = api.get_certified(&key);

    let value = read.value.map(CrdtValueJson::from_crdt_value);
    let frontier = read.frontier.map(|f| FrontierJson {
        physical: f.physical,
        logical: f.logical,
        node_id: f.node_id,
    });

    let proof = read.proof.map(|p| ProofBundleJson {
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
    });

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
    Json(req): Json<crate::network::frontier_sync::FrontierPushRequest>,
) -> Json<crate::network::frontier_sync::FrontierPushResponse> {
    let mut api = state.certified.lock().await;
    let mut accepted = 0;
    for frontier in req.frontiers {
        if api.update_frontier(frontier) {
            accepted += 1;
        }
    }
    Json(crate::network::frontier_sync::FrontierPushResponse { accepted })
}

/// `GET /api/internal/frontiers`
///
/// Returns all frontiers currently tracked by this node.
pub async fn get_internal_frontiers(
    State(state): State<Arc<AppState>>,
) -> Json<crate::network::frontier_sync::FrontierPullResponse> {
    let api = state.certified.lock().await;
    let frontiers = api.all_frontiers().into_iter().cloned().collect();
    Json(crate::network::frontier_sync::FrontierPullResponse { frontiers })
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
    };
    let approvals: Vec<NodeId> = req.approvals.iter().map(|a| NodeId(a.clone())).collect();

    // Validate majority consensus (FR-009).
    {
        let mut consensus = state.consensus.lock().await;
        consensus.propose_authority_update(def.clone(), &approvals)?;
    }

    // Apply to shared namespace for read handlers and CertifiedApi.
    {
        let mut ns = state.namespace.write().unwrap();
        ns.set_authority_definition(def);
    }

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
    let approvals: Vec<NodeId> = req.approvals.iter().map(|a| NodeId(a.clone())).collect();

    // Read current version from shared namespace.
    let policy = {
        let ns = state.namespace.read().unwrap();
        let current_version = ns.version().0;

        let mut policy = PlacementPolicy::new(
            PolicyVersion(current_version + 1),
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

    let resp = PlacementPolicyResponse {
        key_range_prefix: policy.key_range.prefix.clone(),
        version: policy.version.0,
        replica_count: policy.replica_count,
        required_tags: req.required_tags,
        forbidden_tags: req.forbidden_tags,
        allow_local_write_on_partition: policy.allow_local_write_on_partition,
        certified: policy.certified,
    };

    // Validate majority consensus (FR-009).
    {
        let mut consensus = state.consensus.lock().await;
        consensus.propose_policy_update(policy.clone(), &approvals)?;
    }

    // Apply to shared namespace for read handlers and CertifiedApi.
    {
        let mut ns = state.namespace.write().unwrap();
        ns.set_placement_policy(policy);
    }

    Ok(Json(resp))
}

/// `DELETE /api/control-plane/policies/{prefix}`
///
/// Removes the placement policy for the given key range prefix.
pub async fn remove_policy(
    State(state): State<Arc<AppState>>,
    Path(prefix): Path<String>,
) -> Result<Json<PlacementPolicyResponse>, ApiError> {
    let mut ns = state.namespace.write().unwrap();
    let removed = ns.remove_placement_policy(&prefix).ok_or_else(|| {
        ApiError(CrdtError::KeyNotFound(format!(
            "placement policy: {prefix}"
        )))
    })?;
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
pub async fn verify_proof(Json(req): Json<VerifyProofRequest>) -> Json<VerifyProofResponse> {
    use crate::api::certified::ProofBundle;
    use crate::authority::verifier;
    use crate::hlc::HlcTimestamp;
    use crate::types::{KeyRange, NodeId, PolicyVersion};

    let bundle = ProofBundle {
        key_range: KeyRange {
            prefix: req.key_range_prefix,
        },
        frontier_hlc: HlcTimestamp {
            physical: req.frontier.physical,
            logical: req.frontier.logical,
            node_id: req.frontier.node_id,
        },
        policy_version: PolicyVersion(req.policy_version),
        contributing_authorities: req
            .contributing_authorities
            .into_iter()
            .map(NodeId)
            .collect(),
        total_authorities: req.total_authorities,
        certificate: None,
    };

    let result = verifier::verify_proof(&bundle);

    Json(VerifyProofResponse {
        valid: result.valid,
        has_majority: result.has_majority,
        contributing_count: result.contributing_count,
        required_count: result.required_count,
    })
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
    Json(req): Json<SyncRequest>,
) -> Json<SyncResponse> {
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

    Json(SyncResponse { merged, errors })
}

/// `GET /api/internal/keys`
///
/// Returns all key-value pairs from the eventual store together with the
/// store's current frontier HLC. Used by remote peers for pull-based
/// anti-entropy sync. The frontier allows the requester to correctly
/// initialise its peer frontier tracking after a full sync.
pub async fn internal_keys(State(state): State<Arc<AppState>>) -> Json<KeyDumpResponse> {
    let api = state.eventual.lock().await;
    let store = api.store();
    let entries = store
        .all_entries()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let frontier = store.current_frontier();

    Json(KeyDumpResponse { entries, frontier })
}

/// `POST /api/internal/sync/delta`
///
/// Receives a delta sync request with a frontier timestamp and returns
/// all entries modified after that frontier. Used for incremental
/// anti-entropy sync.
pub async fn internal_delta_sync(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DeltaSyncRequest>,
) -> Json<DeltaSyncResponse> {
    let api = state.eventual.lock().await;
    let store = api.store();

    let entries: Vec<DeltaEntry> = store
        .entries_since(&req.frontier)
        .into_iter()
        .map(|(key, value, hlc)| DeltaEntry { key, value, hlc })
        .collect();

    let sender_frontier = store.current_frontier();

    Json(DeltaSyncResponse {
        entries,
        sender_frontier,
    })
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

    let peers_registry = state.peers.as_ref().ok_or_else(|| {
        ApiError(CrdtError::Internal(
            "peer registry not configured".to_string(),
        ))
    })?;

    let joining_node_id = NodeId(req.node_id.clone());

    // Add the joining node and snapshot peer list under one lock acquisition,
    // then release the lock before performing blocking I/O.
    let (peer_list, persist_snapshot) = {
        let mut registry = peers_registry.lock().await;
        registry
            .add_peer(PeerConfig {
                node_id: joining_node_id.clone(),
                addr: req.address.clone(),
            })
            .map_err(|e| ApiError(CrdtError::InvalidArgument(e.to_string())))?;

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
// Metrics handler
// ---------------------------------------------------------------

/// `GET /api/metrics`
///
/// Returns a snapshot of runtime operational metrics (pending count,
/// certification latency, frontier skew, sync failure rate).
pub async fn get_metrics(State(state): State<Arc<AppState>>) -> Json<MetricsSnapshot> {
    Json(state.metrics.snapshot())
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

/// Write `data` to `path` atomically: write to a sibling `.tmp` file, then
/// rename. Cleans up the temp file on failure.
fn write_atomic(path: &std::path::Path, data: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let tmp_path = path.with_extension("json.tmp");
    if let Err(e) = std::fs::write(&tmp_path, data) {
        return Err(e.to_string());
    }
    if let Err(e) = std::fs::rename(&tmp_path, path) {
        // Clean up stranded temp file on rename failure.
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e.to_string());
    }
    Ok(())
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
}
