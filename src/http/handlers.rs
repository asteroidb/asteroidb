use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use tokio::sync::Mutex;

use crate::api::certified::{CertifiedApi, OnTimeout};
use crate::api::eventual::EventualApi;
use crate::crdt::pn_counter::PnCounter;
use crate::error::CrdtError;
use crate::store::kv::CrdtValue;

use crate::network::sync::{KeyDumpResponse, SyncError, SyncRequest, SyncResponse};

use super::types::{
    ApiError, CertifiedReadResponse, CertifiedWriteRequest, CertifiedWriteResponse, CrdtValueJson,
    EventualReadResponse, EventualWriteRequest, FrontierJson, StatusResponse, WriteResponse,
};

/// Shared application state for HTTP handlers.
pub struct AppState {
    pub eventual: Mutex<EventualApi>,
    pub certified: Mutex<CertifiedApi>,
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
