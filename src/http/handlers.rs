use std::sync::{Arc, RwLock};

use axum::Json;
use axum::extract::{Path, State};
use tokio::sync::Mutex;

use crate::api::certified::{CertifiedApi, OnTimeout};
use crate::api::eventual::EventualApi;
use crate::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use crate::crdt::pn_counter::PnCounter;
use crate::error::CrdtError;
use crate::placement::PlacementPolicy;
use crate::store::kv::CrdtValue;
use crate::types::{KeyRange, NodeId, PolicyVersion};

use crate::network::sync::{KeyDumpResponse, SyncError, SyncRequest, SyncResponse};

use super::types::{
    ApiError, AuthorityDefinitionResponse, CertifiedReadResponse, CertifiedWriteRequest,
    CertifiedWriteResponse, CrdtValueJson, EventualReadResponse, EventualWriteRequest,
    FrontierJson, PlacementPolicyResponse, SetAuthorityDefinitionRequest,
    SetPlacementPolicyRequest, StatusResponse, VersionHistoryResponse, WriteResponse,
};

/// Shared application state for HTTP handlers.
pub struct AppState {
    pub eventual: Mutex<EventualApi>,
    pub certified: Mutex<CertifiedApi>,
    pub namespace: Arc<RwLock<SystemNamespace>>,
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
        _ => OnTimeout::Pending,
    };

    let crdt_value = json_to_crdt_value(&req.value)?;

    let mut api = state.certified.lock().await;
    let status = api.certified_write(req.key, crdt_value, on_timeout)?;

    Ok(Json(CertifiedWriteResponse { status }))
}

/// `GET /api/certified/:key`
///
/// Returns the value with certification status and frontier.
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

    Json(CertifiedReadResponse {
        key,
        value,
        status: read.status,
        frontier,
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
        api.update_frontier(frontier);
        accepted += 1;
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
pub async fn set_authority_definition(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SetAuthorityDefinitionRequest>,
) -> Json<AuthorityDefinitionResponse> {
    let mut ns = state.namespace.write().unwrap();
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
    ns.set_authority_definition(def);
    Json(AuthorityDefinitionResponse {
        key_range_prefix: req.key_range_prefix,
        authority_nodes: req.authority_nodes,
    })
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
pub async fn set_placement_policy(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SetPlacementPolicyRequest>,
) -> Json<PlacementPolicyResponse> {
    let mut ns = state.namespace.write().unwrap();
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

    let resp = PlacementPolicyResponse {
        key_range_prefix: policy.key_range.prefix.clone(),
        version: policy.version.0,
        replica_count: policy.replica_count,
        required_tags: req.required_tags,
        forbidden_tags: req.forbidden_tags,
        allow_local_write_on_partition: policy.allow_local_write_on_partition,
        certified: policy.certified,
    };

    ns.set_placement_policy(policy);
    Json(resp)
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
/// Returns all key-value pairs from the eventual store. Used by
/// remote peers for pull-based anti-entropy sync.
pub async fn internal_keys(State(state): State<Arc<AppState>>) -> Json<KeyDumpResponse> {
    let api = state.eventual.lock().await;
    let entries = api
        .store()
        .all_entries()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    Json(KeyDumpResponse { entries })
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

/// Convert a JSON CRDT value representation into an internal `CrdtValue`.
///
/// For Counter, creates a PnCounter with the specified value by incrementing
/// a synthetic writer node. For Set/Map/Register, constructs the appropriate
/// CRDT type from the provided data.
fn json_to_crdt_value(json: &CrdtValueJson) -> Result<CrdtValue, CrdtError> {
    use crate::crdt::lww_register::LwwRegister;
    use crate::crdt::or_map::OrMap;
    use crate::crdt::or_set::OrSet;
    use crate::hlc::Hlc;
    use crate::types::NodeId;

    let writer = NodeId("http-writer".into());

    match json {
        CrdtValueJson::Counter { value } => {
            let mut counter = PnCounter::new();
            if *value >= 0 {
                for _ in 0..*value {
                    counter.increment(&writer);
                }
            } else {
                for _ in 0..value.unsigned_abs() {
                    counter.decrement(&writer);
                }
            }
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
}
