use std::collections::HashMap;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::error::CrdtError;
use crate::store::kv::CrdtValue;
use crate::types::CertificationStatus;

/// JSON-friendly representation of a CRDT value for API responses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum CrdtValueJson {
    #[serde(rename = "counter")]
    Counter { value: i64 },
    #[serde(rename = "set")]
    Set { elements: Vec<String> },
    #[serde(rename = "map")]
    Map { entries: HashMap<String, String> },
    #[serde(rename = "register")]
    Register { value: Option<String> },
}

impl CrdtValueJson {
    /// Convert an internal `CrdtValue` to its JSON representation.
    pub fn from_crdt_value(value: &CrdtValue) -> Self {
        match value {
            CrdtValue::Counter(c) => CrdtValueJson::Counter { value: c.value() },
            CrdtValue::Set(s) => {
                let mut elements: Vec<String> = s.elements().into_iter().cloned().collect();
                elements.sort();
                CrdtValueJson::Set { elements }
            }
            CrdtValue::Map(m) => {
                let mut entries = HashMap::new();
                for key in m.keys() {
                    if let Some(val) = m.get(key) {
                        entries.insert(key.clone(), val.clone());
                    }
                }
                CrdtValueJson::Map { entries }
            }
            CrdtValue::Register(r) => CrdtValueJson::Register {
                value: r.get().cloned(),
            },
        }
    }
}

// ---------------------------------------------------------------
// Request types
// ---------------------------------------------------------------

/// Request body for `POST /api/eventual/write`.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum EventualWriteRequest {
    #[serde(rename = "counter_inc")]
    CounterInc { key: String },
    #[serde(rename = "counter_dec")]
    CounterDec { key: String },
    #[serde(rename = "set_add")]
    SetAdd { key: String, element: String },
    #[serde(rename = "set_remove")]
    SetRemove { key: String, element: String },
    #[serde(rename = "map_set")]
    MapSet {
        key: String,
        map_key: String,
        map_value: String,
    },
    #[serde(rename = "map_delete")]
    MapDelete { key: String, map_key: String },
    #[serde(rename = "register_set")]
    RegisterSet { key: String, value: String },
}

/// Request body for `POST /api/certified/write`.
#[derive(Debug, Deserialize)]
pub struct CertifiedWriteRequest {
    pub key: String,
    pub value: CrdtValueJson,
    #[serde(default = "default_on_timeout")]
    pub on_timeout: String,
}

fn default_on_timeout() -> String {
    "pending".to_string()
}

// ---------------------------------------------------------------
// Response types
// ---------------------------------------------------------------

/// Response for `GET /api/eventual/:key`.
#[derive(Debug, Serialize, Deserialize)]
pub struct EventualReadResponse {
    pub key: String,
    pub value: Option<CrdtValueJson>,
}

/// Response for a successful write.
#[derive(Debug, Serialize, Deserialize)]
pub struct WriteResponse {
    pub ok: bool,
}

/// Response for `GET /api/certified/:key`.
#[derive(Debug, Serialize, Deserialize)]
pub struct CertifiedReadResponse {
    pub key: String,
    pub value: Option<CrdtValueJson>,
    pub status: CertificationStatus,
    pub frontier: Option<FrontierJson>,
    pub proof: Option<ProofBundleJson>,
}

/// JSON-friendly frontier representation.
#[derive(Debug, Serialize, Deserialize)]
pub struct FrontierJson {
    pub physical: u64,
    pub logical: u32,
    pub node_id: String,
}

/// JSON-friendly representation of a verifiable proof bundle.
///
/// Included in certified read responses so that external clients can
/// independently verify that a majority of authorities acknowledged
/// the frontier.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProofBundleJson {
    /// Key range prefix this proof covers.
    pub key_range_prefix: String,
    /// The majority frontier at certification time.
    pub frontier: FrontierJson,
    /// The policy version in effect.
    pub policy_version: u64,
    /// Authority node IDs that contributed to this proof.
    pub contributing_authorities: Vec<String>,
    /// Total number of authorities in the set.
    pub total_authorities: usize,
}

/// Response for `POST /api/certified/write`.
#[derive(Debug, Serialize, Deserialize)]
pub struct CertifiedWriteResponse {
    pub status: CertificationStatus,
}

/// Request body for `POST /api/certified/verify`.
#[derive(Debug, Deserialize)]
pub struct VerifyProofRequest {
    /// Key range prefix this proof covers.
    pub key_range_prefix: String,
    /// The majority frontier at certification time.
    pub frontier: FrontierJson,
    /// The policy version in effect.
    pub policy_version: u64,
    /// Authority node IDs that contributed to this proof.
    pub contributing_authorities: Vec<String>,
    /// Total number of authorities in the set.
    pub total_authorities: usize,
}

/// Response for `POST /api/certified/verify`.
#[derive(Debug, Serialize, Deserialize)]
pub struct VerifyProofResponse {
    /// Overall validity of the proof.
    pub valid: bool,
    /// Whether a strict majority is present.
    pub has_majority: bool,
    /// Number of contributing authorities.
    pub contributing_count: usize,
    /// Number of authorities required for majority.
    pub required_count: usize,
}

/// Response for `GET /api/status/:key`.
#[derive(Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    pub key: String,
    pub status: CertificationStatus,
}

// ---------------------------------------------------------------
// Control-plane request types
// ---------------------------------------------------------------

/// Request body for `PUT /api/control-plane/authorities`.
///
/// Requires majority approval from authority nodes (FR-009).
#[derive(Debug, Deserialize)]
pub struct SetAuthorityDefinitionRequest {
    pub key_range_prefix: String,
    pub authority_nodes: Vec<String>,
    /// Node IDs that have approved this update.
    pub approvals: Vec<String>,
}

/// Request body for `PUT /api/control-plane/policies`.
///
/// Requires majority approval from authority nodes (FR-009).
#[derive(Debug, Deserialize)]
pub struct SetPlacementPolicyRequest {
    pub key_range_prefix: String,
    pub replica_count: usize,
    #[serde(default)]
    pub required_tags: Vec<String>,
    #[serde(default)]
    pub forbidden_tags: Vec<String>,
    #[serde(default)]
    pub allow_local_write_on_partition: bool,
    #[serde(default)]
    pub certified: bool,
    /// Node IDs that have approved this update.
    pub approvals: Vec<String>,
}

// ---------------------------------------------------------------
// Control-plane response types
// ---------------------------------------------------------------

/// Response for authority definition endpoints.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthorityDefinitionResponse {
    pub key_range_prefix: String,
    pub authority_nodes: Vec<String>,
}

/// Response for placement policy endpoints.
#[derive(Debug, Serialize, Deserialize)]
pub struct PlacementPolicyResponse {
    pub key_range_prefix: String,
    pub version: u64,
    pub replica_count: usize,
    pub required_tags: Vec<String>,
    pub forbidden_tags: Vec<String>,
    pub allow_local_write_on_partition: bool,
    pub certified: bool,
}

/// Response for `GET /api/control-plane/versions`.
#[derive(Debug, Serialize, Deserialize)]
pub struct VersionHistoryResponse {
    pub current_version: u64,
    pub history: Vec<u64>,
}

// ---------------------------------------------------------------
// Internal join/leave request/response types
// ---------------------------------------------------------------

/// Request body for `POST /api/internal/join`.
///
/// A new node sends this to a seed node to join the cluster.
#[derive(Debug, Serialize, Deserialize)]
pub struct JoinRequest {
    /// Unique identifier of the joining node.
    pub node_id: String,
    /// Socket address (ip:port) the joining node is listening on.
    pub address: String,
    /// Tags associated with the joining node.
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Response for `POST /api/internal/join`.
///
/// Returned by the seed node to the joining node. Contains the current
/// peer list and a snapshot of the system namespace for bootstrap.
#[derive(Debug, Serialize, Deserialize)]
pub struct JoinResponse {
    /// All peers currently known to the seed node (including the seed itself).
    pub peers: Vec<PeerInfo>,
    /// Serialised snapshot of the system namespace.
    pub namespace: serde_json::Value,
}

/// Minimal peer information returned in the join response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Unique identifier of the peer node.
    pub node_id: String,
    /// Socket address (ip:port) the peer is listening on.
    pub address: String,
}

/// Request body for `POST /api/internal/leave`.
///
/// Sent by a node that is gracefully departing the cluster.
#[derive(Debug, Serialize, Deserialize)]
pub struct LeaveRequest {
    /// Unique identifier of the departing node.
    pub node_id: String,
}

/// Response for `POST /api/internal/leave`.
#[derive(Debug, Serialize, Deserialize)]
pub struct LeaveResponse {
    /// Whether the leave operation was successful.
    pub success: bool,
}

// ---------------------------------------------------------------
// Error response
// ---------------------------------------------------------------

/// Structured error body returned as JSON.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error_code: String,
    pub message: String,
}

/// Map `CrdtError` to HTTP status code + JSON body.
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self.0 {
            CrdtError::InvalidArgument(msg) => {
                (StatusCode::BAD_REQUEST, "INVALID_ARGUMENT", msg.clone())
            }
            CrdtError::InvalidOp => (
                StatusCode::BAD_REQUEST,
                "INVALID_OP",
                "invalid operation for this CRDT type".to_string(),
            ),
            CrdtError::TypeMismatch { expected, actual } => (
                StatusCode::CONFLICT,
                "TYPE_MISMATCH",
                format!("expected {expected}, got {actual}"),
            ),
            CrdtError::KeyNotFound(key) => (
                StatusCode::NOT_FOUND,
                "KEY_NOT_FOUND",
                format!("key not found: {key}"),
            ),
            CrdtError::StaleVersion => (
                StatusCode::CONFLICT,
                "STALE_VERSION",
                "stale version".to_string(),
            ),
            CrdtError::PolicyDenied(msg) => (StatusCode::FORBIDDEN, "POLICY_DENIED", msg.clone()),
            CrdtError::Timeout => (
                StatusCode::GATEWAY_TIMEOUT,
                "TIMEOUT",
                "timeout".to_string(),
            ),
            CrdtError::Internal(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", msg.clone())
            }
        };

        let body = ErrorResponse {
            error_code: code.to_string(),
            message,
        };

        (status, axum::Json(body)).into_response()
    }
}

/// Newtype wrapper for `CrdtError` to implement `IntoResponse`.
#[derive(Debug)]
pub struct ApiError(pub CrdtError);

impl From<CrdtError> for ApiError {
    fn from(err: CrdtError) -> Self {
        ApiError(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::lww_register::LwwRegister;
    use crate::crdt::or_map::OrMap;
    use crate::crdt::or_set::OrSet;
    use crate::crdt::pn_counter::PnCounter;
    use crate::hlc::HlcTimestamp;
    use crate::types::NodeId;

    fn node(name: &str) -> NodeId {
        NodeId(name.into())
    }

    fn ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    #[test]
    fn counter_to_json() {
        let mut c = PnCounter::new();
        c.increment(&node("a"));
        c.increment(&node("a"));
        c.decrement(&node("a"));

        let json = CrdtValueJson::from_crdt_value(&CrdtValue::Counter(c));
        assert_eq!(json, CrdtValueJson::Counter { value: 1 });
    }

    #[test]
    fn set_to_json() {
        let mut s = OrSet::new();
        s.add("bob".to_string(), &node("a"));
        s.add("alice".to_string(), &node("a"));

        let json = CrdtValueJson::from_crdt_value(&CrdtValue::Set(s));
        match json {
            CrdtValueJson::Set { elements } => {
                assert_eq!(elements, vec!["alice", "bob"]);
            }
            other => panic!("expected Set, got {other:?}"),
        }
    }

    #[test]
    fn map_to_json() {
        let mut m = OrMap::new();
        m.set(
            "name".to_string(),
            "AsteroidDB".to_string(),
            ts(100, 0, "a"),
            &node("a"),
        );

        let json = CrdtValueJson::from_crdt_value(&CrdtValue::Map(m));
        match json {
            CrdtValueJson::Map { entries } => {
                assert_eq!(entries.get("name"), Some(&"AsteroidDB".to_string()));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn register_to_json() {
        let mut r = LwwRegister::new();
        r.set("hello".to_string(), ts(100, 0, "a"));

        let json = CrdtValueJson::from_crdt_value(&CrdtValue::Register(r));
        assert_eq!(
            json,
            CrdtValueJson::Register {
                value: Some("hello".into()),
            }
        );
    }

    #[test]
    fn empty_register_to_json() {
        let r = LwwRegister::<String>::new();
        let json = CrdtValueJson::from_crdt_value(&CrdtValue::Register(r));
        assert_eq!(json, CrdtValueJson::Register { value: None });
    }

    #[test]
    fn deserialize_eventual_write_counter_inc() {
        let json = r#"{"type":"counter_inc","key":"hits"}"#;
        let req: EventualWriteRequest = serde_json::from_str(json).unwrap();
        match req {
            EventualWriteRequest::CounterInc { key } => assert_eq!(key, "hits"),
            other => panic!("expected CounterInc, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_eventual_write_set_add() {
        let json = r#"{"type":"set_add","key":"users","element":"alice"}"#;
        let req: EventualWriteRequest = serde_json::from_str(json).unwrap();
        match req {
            EventualWriteRequest::SetAdd { key, element } => {
                assert_eq!(key, "users");
                assert_eq!(element, "alice");
            }
            other => panic!("expected SetAdd, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_certified_write_request() {
        let json = r#"{"key":"sensor","value":{"type":"counter","value":42},"on_timeout":"error"}"#;
        let req: CertifiedWriteRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.key, "sensor");
        assert_eq!(req.on_timeout, "error");
    }

    #[test]
    fn deserialize_certified_write_request_default_timeout() {
        let json = r#"{"key":"sensor","value":{"type":"counter","value":42}}"#;
        let req: CertifiedWriteRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.on_timeout, "pending");
    }
}
