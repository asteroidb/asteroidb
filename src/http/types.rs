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

impl EventualWriteRequest {
    /// Return the key being written, regardless of operation type.
    pub fn key(&self) -> &str {
        match self {
            Self::CounterInc { key }
            | Self::CounterDec { key }
            | Self::SetAdd { key, .. }
            | Self::SetRemove { key, .. }
            | Self::MapSet { key, .. }
            | Self::MapDelete { key, .. }
            | Self::RegisterSet { key, .. } => key,
        }
    }
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
    /// Session token covering this read's observed position (monotonic
    /// reads). Only present when the request carried a `session_token`
    /// query parameter (possibly empty); absent responses are
    /// byte-compatible with the pre-session wire format.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
}

/// Response for a successful write.
#[derive(Debug, Serialize, Deserialize)]
pub struct WriteResponse {
    pub ok: bool,
    /// Session token encoding this write's HLC position. Present the next
    /// eventual read as `?session_token=...` for read-your-writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
}

/// Query parameters for `GET /api/eventual/:key`.
///
/// All fields optional: a request without `session_token` behaves exactly
/// like the pre-session API (no checks, no token in the response).
#[derive(Debug, Default, Deserialize)]
pub struct EventualReadQuery {
    /// Session token from a previous write/read. Empty string means "no
    /// precondition, but start a session: return an observed-position
    /// token with the response".
    #[serde(default)]
    pub session_token: Option<String>,
    /// Maximum time (ms, capped at 5000) to wait for the local replica to
    /// catch up before answering 412. Only meaningful with a non-empty
    /// `session_token`.
    #[serde(default)]
    pub wait_ms: Option<u64>,
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

/// JSON-friendly representation of an individual authority signature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthoritySignatureJson {
    /// The authority node ID that produced this signature.
    pub authority_id: String,
    /// Hex-encoded Ed25519 public key (32 bytes).
    pub public_key: String,
    /// Hex-encoded Ed25519 signature (64 bytes).
    pub signature: String,
    /// The keyset version under which this signature was produced.
    pub keyset_version: u64,
}

/// JSON-friendly representation of a majority certificate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateJson {
    /// The keyset version used for signing.
    pub keyset_version: u64,
    /// Individual authority signatures.
    pub signatures: Vec<AuthoritySignatureJson>,
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
    /// The majority certificate with cryptographic signatures.
    /// Must be present for the proof to be considered valid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate: Option<CertificateJson>,
    /// Signature algorithm of the attached certificate material
    /// (`"Ed25519"` or `"Bls12_381"`). Mirrors `VerifyProofRequest` so a
    /// certified read response can round-trip into a verify request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature_algorithm: Option<String>,
    /// Keyset version of the attached certificate material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keyset_version: Option<u64>,
    /// BLS signer node IDs (same order as `bls_public_keys`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bls_signer_ids: Option<Vec<String>>,
    /// Hex-encoded BLS public keys (same order as `bls_signer_ids`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bls_public_keys: Option<Vec<String>>,
    /// Hex-encoded aggregated BLS signature over the certificate message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bls_aggregate_signature: Option<String>,
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
    ///
    /// Accepted for wire compatibility but **ignored by the server**: the
    /// majority denominator is derived from this node's authority definition
    /// for `key_range_prefix`, so callers cannot understate the quorum.
    #[serde(default)]
    pub total_authorities: usize,
    /// The majority certificate with cryptographic signatures.
    /// Must be present for the proof to be considered valid.
    #[serde(default)]
    pub certificate: Option<CertificateJson>,
    /// Optional certificate format version. When provided, the handler uses
    /// `FormatVersionConfig` to validate version compatibility.
    #[serde(default)]
    pub format_version: Option<u32>,
    /// Optional signature algorithm hint (`"Ed25519"` or `"Bls12_381"`).
    /// When `"Bls12_381"`, the handler sets the certificate's algorithm field
    /// so the verifier selects the correct verification path.
    #[serde(default)]
    pub signature_algorithm: Option<String>,
    /// Optional keyset version for BLS verification without a certificate
    /// JSON body. Falls back to `certificate.keyset_version`, then 1.
    #[serde(default)]
    pub keyset_version: Option<u64>,
    /// Optional hex-encoded BLS aggregated signature. When present together
    /// with `bls_signer_ids` and `bls_public_keys` and
    /// `signature_algorithm == "Bls12_381"`, the handler performs BLS
    /// aggregate verification against the keyset registry.
    #[serde(default)]
    pub bls_aggregate_signature: Option<String>,
    /// Optional BLS signer node IDs (same order as `bls_public_keys`).
    #[serde(default)]
    pub bls_signer_ids: Option<Vec<String>>,
    /// Optional hex-encoded BLS public keys (same order as `bls_signer_ids`).
    #[serde(default)]
    pub bls_public_keys: Option<Vec<String>>,
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

/// Response for `GET /api/authority/equivocations`.
///
/// The `evidence` entries carry both conflicting signed attestations
/// verbatim (hex-encoded signatures included), so a third party can
/// re-verify each pair against the registry keys — the report is a
/// portable proof-of-misbehaviour bundle, not just a status summary.
#[derive(Debug, Serialize, Deserialize)]
pub struct EquivocationReport {
    /// Authorities with at least one recorded equivocation.
    pub accused_authorities: Vec<String>,
    /// Number of stored evidence entries.
    pub evidence_count: usize,
    /// Conflicts detected beyond the per-authority storage cap (not stored).
    pub evidence_overflow_total: u64,
    /// The raw evidence pairs.
    pub evidence: Vec<crate::authority::equivocation::EquivocationEvidence>,
}

// ---------------------------------------------------------------
// Control-plane request types
// ---------------------------------------------------------------

/// Request body for `PUT /api/control-plane/authorities`.
///
/// The update is committed through the control-plane Raft log (FR-009);
/// the receiving node must be the current Raft leader.
#[derive(Debug, Deserialize)]
pub struct SetAuthorityDefinitionRequest {
    pub key_range_prefix: String,
    pub authority_nodes: Vec<String>,
    /// Deprecated: self-reported approvals from the pre-Raft consensus.
    /// Accepted for wire compatibility with old clients but ignored —
    /// agreement now comes from Raft log replication, not caller claims.
    #[serde(default)]
    pub approvals: Vec<String>,
}

/// Request body for `PUT /api/control-plane/policies`.
///
/// The update is committed through the control-plane Raft log (FR-009);
/// the receiving node must be the current Raft leader.
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
    /// Deprecated: self-reported approvals from the pre-Raft consensus.
    /// Accepted for wire compatibility with old clients but ignored.
    #[serde(default)]
    pub approvals: Vec<String>,
}

/// Request body for `DELETE /api/control-plane/policies/{prefix}`.
///
/// The removal is committed through the control-plane Raft log (FR-009);
/// the receiving node must be the current Raft leader.
#[derive(Debug, Deserialize)]
pub struct RemovePolicyRequest {
    /// Deprecated: self-reported approvals from the pre-Raft consensus.
    /// Accepted for wire compatibility with old clients but ignored.
    #[serde(default)]
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

/// Response for `GET /api/control-plane/raft/status`.
///
/// Public read-only observability endpoint for the control-plane Raft
/// consensus (same posture as `/api/metrics`). JSON-only external type, so
/// `Option` fields are plain serde (no bincode concerns).
#[derive(Debug, Serialize, Deserialize)]
pub struct RaftStatusResponse {
    /// This node's ID.
    pub node_id: String,
    /// Current role: `"leader"`, `"follower"`, `"candidate"`, or `"detached"`
    /// (no Raft consensus configured).
    pub role: String,
    /// Current Raft term.
    pub term: u64,
    /// Known leader hint, if any.
    pub leader_id: Option<String>,
    /// Resolved address of the leader hint, if known.
    pub leader_addr: Option<String>,
    /// Highest committed log index.
    pub commit_index: u64,
    /// Highest log index applied to the state machine.
    pub last_applied: u64,
    /// Last index in the local log.
    pub last_log_index: u64,
    /// Static voter set (`ASTEROIDB_CONTROL_PLANE_NODES`).
    pub voters: Vec<String>,
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
// Internal announce request/response types
// ---------------------------------------------------------------

/// Request body for `POST /api/internal/announce`.
///
/// Sent by a joining node to all peers to announce its presence.
/// Also used by a leaving node to announce its departure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnounceRequest {
    /// Unique identifier of the announcing node.
    pub node_id: String,
    /// Socket address (ip:port) the announcing node is listening on.
    pub address: String,
    /// Whether the node is joining (`true`) or leaving (`false`).
    pub joining: bool,
}

/// Response for `POST /api/internal/announce`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AnnounceResponse {
    /// Whether the announcement was accepted.
    pub accepted: bool,
}

// ---------------------------------------------------------------
// Internal ping request/response types
// ---------------------------------------------------------------

/// Request body for `POST /api/internal/ping`.
///
/// Used for lightweight gossip-based peer list exchange.
/// Sends a digest (sorted list of known peer node IDs) so the
/// receiver can detect differences without transferring full state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingRequest {
    /// Node ID of the sender.
    pub sender_id: String,
    /// Socket address (ip:port) the sender is listening on.
    pub sender_addr: String,
    /// Sorted list of known peer node IDs (digest).
    pub known_peers: Vec<PeerInfo>,
}

/// Response for `POST /api/internal/ping`.
///
/// Returns the receiver's known peers so the sender can reconcile.
#[derive(Debug, Serialize, Deserialize)]
pub struct PingResponse {
    /// Sorted list of known peer node IDs from the receiver.
    pub known_peers: Vec<PeerInfo>,
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
            CrdtError::IncompatibleVersion {
                data_version,
                code_version,
            } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "INCOMPATIBLE_VERSION",
                format!(
                    "data version {data_version} incompatible with code version {code_version}"
                ),
            ),
            CrdtError::MigrationFailed {
                from, to, reason, ..
            } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "MIGRATION_FAILED",
                format!("migration v{from} to v{to} failed: {reason}"),
            ),
            CrdtError::Internal(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", msg.clone())
            }
            CrdtError::CertificationTimeout => (
                StatusCode::GATEWAY_TIMEOUT,
                "CERTIFICATION_TIMEOUT",
                CrdtError::CertificationTimeout.to_string(),
            ),
            CrdtError::SessionNotSatisfied { key } => (
                StatusCode::PRECONDITION_FAILED,
                "SESSION_NOT_SATISFIED",
                format!(
                    "session token not satisfied for key {key}; \
                     retry, increase wait_ms, or try another replica"
                ),
            ),
            // WAL append failure: the write was applied in memory but its
            // durability could not be established (e.g. disk full). 503
            // signals a retryable service condition; reads keep working.
            CrdtError::Storage(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "STORAGE_UNAVAILABLE",
                msg.clone(),
            ),
            // Not the control-plane Raft leader: retryable service condition.
            // The response carries leader hint headers so the caller can
            // re-send the request to the leader directly.
            CrdtError::NotLeader {
                leader_id,
                leader_addr,
            } => (
                StatusCode::SERVICE_UNAVAILABLE,
                "NOT_LEADER",
                format!(
                    "this node is not the control-plane leader; retry against the leader \
                     (leader_id: {}, leader_addr: {})",
                    leader_id.as_deref().unwrap_or("unknown"),
                    leader_addr.as_deref().unwrap_or("unknown"),
                ),
            ),
        };
        let retry_after = matches!(
            &self.0,
            CrdtError::SessionNotSatisfied { .. } | CrdtError::NotLeader { .. }
        );

        let body = ErrorResponse {
            error_code: code.to_string(),
            message,
        };

        let mut resp = (status, axum::Json(body)).into_response();
        if retry_after {
            resp.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
        }
        if let CrdtError::NotLeader {
            leader_id,
            leader_addr,
        } = &self.0
        {
            if let Some(id) = leader_id
                && let Ok(v) = axum::http::HeaderValue::from_str(id)
            {
                resp.headers_mut().insert("x-asteroidb-leader-id", v);
            }
            if let Some(addr) = leader_addr
                && let Ok(v) = axum::http::HeaderValue::from_str(addr)
            {
                resp.headers_mut().insert("x-asteroidb-leader-addr", v);
            }
        }
        resp
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

    // ---------------------------------------------------------------
    // Session-token response compatibility and error mapping
    // ---------------------------------------------------------------

    /// Pre-session JSON (`{"ok":true}`) must still deserialize into
    /// `WriteResponse`.
    #[test]
    fn write_response_legacy_json_deserialises() {
        let resp: WriteResponse = serde_json::from_str(r#"{"ok":true}"#).unwrap();
        assert!(resp.ok);
        assert!(resp.session_token.is_none());
    }

    /// Pre-session eventual read JSON must still deserialize, and a
    /// response without a token must serialize WITHOUT the field (byte
    /// compatibility for token-less requests).
    #[test]
    fn eventual_read_response_token_field_is_omitted_when_none() {
        let resp: EventualReadResponse =
            serde_json::from_str(r#"{"key":"k","value":null}"#).unwrap();
        assert!(resp.session_token.is_none());

        let json = serde_json::to_string(&EventualReadResponse {
            key: "k".into(),
            value: None,
            session_token: None,
        })
        .unwrap();
        assert!(
            !json.contains("session_token"),
            "None token must not appear in the wire format: {json}"
        );

        let json = serde_json::to_string(&WriteResponse {
            ok: true,
            session_token: None,
        })
        .unwrap();
        assert_eq!(json, r#"{"ok":true}"#);
    }

    /// `SessionNotSatisfied` must map to 412 with a `Retry-After: 1`
    /// header and the `SESSION_NOT_SATISFIED` error code.
    #[tokio::test]
    async fn session_not_satisfied_maps_to_412_with_retry_after() {
        use http_body_util::BodyExt;

        let resp = ApiError(CrdtError::SessionNotSatisfied { key: "k1".into() }).into_response();
        assert_eq!(resp.status(), StatusCode::PRECONDITION_FAILED);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );

        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error_code"], "SESSION_NOT_SATISFIED");
        assert!(body["message"].as_str().unwrap().contains("k1"));
    }

    /// Other errors must not grow a Retry-After header.
    #[test]
    fn non_session_errors_have_no_retry_after() {
        let resp = ApiError(CrdtError::Timeout).into_response();
        assert!(
            resp.headers()
                .get(axum::http::header::RETRY_AFTER)
                .is_none()
        );
    }

    /// Query type: both fields optional with defaults.
    #[test]
    fn eventual_read_query_defaults() {
        let q: EventualReadQuery = serde_json::from_str("{}").unwrap();
        assert!(q.session_token.is_none());
        assert!(q.wait_ms.is_none());

        let q: EventualReadQuery =
            serde_json::from_str(r#"{"session_token":"","wait_ms":100}"#).unwrap();
        assert_eq!(q.session_token.as_deref(), Some(""));
        assert_eq!(q.wait_ms, Some(100));
    }
}
