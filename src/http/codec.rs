//! Content-Type based serialization/deserialization helpers for internal
//! node-to-node communication.
//!
//! Supports two wire formats:
//! - `application/octet-stream` — bincode (compact binary, default for internal traffic)
//! - `application/json` — JSON (backward compatible fallback)
//!
//! External client-facing APIs are not affected and continue to use JSON exclusively.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

/// MIME type for bincode-encoded payloads.
pub const CONTENT_TYPE_BINCODE: &str = "application/octet-stream";
/// MIME type for JSON-encoded payloads.
pub const CONTENT_TYPE_JSON: &str = "application/json";

/// Serialize `data` using the format requested by the `Accept` header.
///
/// If `accept` contains `application/octet-stream`, serializes as bincode.
/// Otherwise falls back to JSON for backward compatibility.
///
/// Returns the serialized bytes and the Content-Type to set on the response.
pub fn serialize_internal<T: Serialize>(
    data: &T,
    accept: Option<&str>,
) -> Result<(Vec<u8>, &'static str), SerializationError> {
    if accepts_bincode(accept) {
        let bytes = bincode::serde::encode_to_vec(data, bincode::config::standard())
            .map_err(|e| SerializationError(format!("bincode encode: {e}")))?;
        Ok((bytes, CONTENT_TYPE_BINCODE))
    } else {
        let bytes = serde_json::to_vec(data)
            .map_err(|e| SerializationError(format!("json encode: {e}")))?;
        Ok((bytes, CONTENT_TYPE_JSON))
    }
}

/// Deserialize `bytes` using the format indicated by the `Content-Type` header.
///
/// If `content_type` contains `application/octet-stream`, deserializes as bincode.
/// Otherwise falls back to JSON.
pub fn deserialize_internal<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    content_type: Option<&str>,
) -> Result<T, SerializationError> {
    if is_bincode_content_type(content_type) {
        let (val, _len) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|e| SerializationError(format!("bincode decode: {e}")))?;
        Ok(val)
    } else {
        serde_json::from_slice(bytes).map_err(|e| SerializationError(format!("json decode: {e}")))
    }
}

/// Check whether the `Accept` header value indicates bincode preference.
///
/// Properly parses the Accept header to handle:
/// - Case-insensitive MIME type matching
/// - Multiple comma-separated types (e.g., `application/octet-stream, application/json`)
/// - Quality values (e.g., `application/octet-stream;q=0.9, application/json;q=0.8`)
/// - Parameters after the media type (e.g., `application/octet-stream; charset=utf-8`)
///
/// Returns `true` if `application/octet-stream` appears with q > 0.
pub fn accepts_bincode(accept: Option<&str>) -> bool {
    let accept = match accept {
        Some(a) => a,
        None => return false,
    };

    for part in accept.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Split into media type and parameters (;key=value)
        let mut segments = part.split(';');
        let media_type = segments.next().unwrap_or("").trim();

        // Check if the media type matches (case-insensitive)
        if !media_type.eq_ignore_ascii_case(CONTENT_TYPE_BINCODE) {
            continue;
        }

        // Check for q=0 which means "not acceptable"
        let mut q_value = 1.0_f64;
        for segment in segments {
            let segment = segment.trim();
            if let Some(q_str) = segment
                .strip_prefix("q=")
                .or_else(|| segment.strip_prefix("Q="))
            {
                q_value = q_str.trim().parse::<f64>().unwrap_or(1.0);
            }
        }

        if q_value > 0.0 {
            return true;
        }
    }

    false
}

/// Check whether the `Content-Type` header value indicates bincode.
///
/// Extracts just the media type from the Content-Type header, ignoring
/// any parameters (e.g., `charset=utf-8`). Comparison is case-insensitive.
pub fn is_bincode_content_type(content_type: Option<&str>) -> bool {
    let ct = match content_type {
        Some(ct) => ct,
        None => return false,
    };

    // Content-Type is a single media type, possibly with parameters.
    // Extract just the media type part (before any semicolons).
    let media_type = ct.split(';').next().unwrap_or("").trim();
    media_type.eq_ignore_ascii_case(CONTENT_TYPE_BINCODE)
}

/// Error type for serialization/deserialization failures.
#[derive(Debug, Clone)]
pub struct SerializationError(pub String);

impl std::fmt::Display for SerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "serialization error: {}", self.0)
    }
}

impl std::error::Error for SerializationError {}

impl IntoResponse for SerializationError {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, self.0).into_response()
    }
}

/// Build an axum `Response` with the correct Content-Type for internal endpoints.
///
/// Checks the `Accept` header and serializes accordingly.
pub fn internal_response<T: Serialize>(
    data: &T,
    accept: Option<&str>,
) -> Result<Response, SerializationError> {
    let (bytes, content_type) = serialize_internal(data, accept)?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", content_type)
        .body(axum::body::Body::from(bytes))
        .unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::crdt::pn_counter::PnCounter;
    use crate::hlc::HlcTimestamp;
    use crate::network::sync::{
        DeltaEntry, DeltaSyncRequest, DeltaSyncResponse, KeyDumpResponse, SyncError, SyncRequest,
        SyncResponse,
    };
    use crate::store::kv::CrdtValue;
    use crate::types::NodeId;

    fn nid(s: &str) -> NodeId {
        NodeId(s.into())
    }

    fn hlc(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    // ---------------------------------------------------------------
    // Bincode round-trip tests for all sync message types
    // ---------------------------------------------------------------

    #[test]
    fn bincode_roundtrip_sync_request() {
        let mut entries = HashMap::new();
        let mut counter = PnCounter::new();
        counter.increment(&nid("node-1"));
        entries.insert("key1".to_string(), CrdtValue::Counter(counter));

        let req = SyncRequest {
            sender: "node-1".to_string(),
            entries,
        };

        let (bytes, ct) = serialize_internal(&req, Some(CONTENT_TYPE_BINCODE)).unwrap();
        assert_eq!(ct, CONTENT_TYPE_BINCODE);

        let decoded: SyncRequest =
            deserialize_internal(&bytes, Some(CONTENT_TYPE_BINCODE)).unwrap();
        assert_eq!(decoded.sender, "node-1");
        assert!(decoded.entries.contains_key("key1"));
    }

    #[test]
    fn bincode_roundtrip_sync_response() {
        let resp = SyncResponse {
            merged: 5,
            errors: vec![SyncError {
                key: "bad".into(),
                error: "type mismatch".into(),
            }],
        };

        let (bytes, ct) = serialize_internal(&resp, Some(CONTENT_TYPE_BINCODE)).unwrap();
        assert_eq!(ct, CONTENT_TYPE_BINCODE);

        let decoded: SyncResponse =
            deserialize_internal(&bytes, Some(CONTENT_TYPE_BINCODE)).unwrap();
        assert_eq!(decoded.merged, 5);
        assert_eq!(decoded.errors.len(), 1);
        assert_eq!(decoded.errors[0].key, "bad");
    }

    #[test]
    fn bincode_roundtrip_delta_sync_request() {
        let req = DeltaSyncRequest {
            sender: "node-2".to_string(),
            frontier: hlc(300, 1, "node-2"),
        };

        let (bytes, _) = serialize_internal(&req, Some(CONTENT_TYPE_BINCODE)).unwrap();
        let decoded: DeltaSyncRequest =
            deserialize_internal(&bytes, Some(CONTENT_TYPE_BINCODE)).unwrap();
        assert_eq!(decoded.sender, "node-2");
        assert_eq!(decoded.frontier.physical, 300);
        assert_eq!(decoded.frontier.logical, 1);
    }

    #[test]
    fn bincode_roundtrip_delta_sync_response() {
        let mut counter = PnCounter::new();
        counter.increment(&nid("node-1"));

        let resp = DeltaSyncResponse {
            entries: vec![DeltaEntry {
                key: "key1".into(),
                value: CrdtValue::Counter(counter),
                hlc: hlc(200, 0, "node-1"),
            }],
            sender_frontier: Some(hlc(200, 0, "node-1")),
        };

        let (bytes, _) = serialize_internal(&resp, Some(CONTENT_TYPE_BINCODE)).unwrap();
        let decoded: DeltaSyncResponse =
            deserialize_internal(&bytes, Some(CONTENT_TYPE_BINCODE)).unwrap();
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.entries[0].key, "key1");
        assert_eq!(decoded.sender_frontier.unwrap().physical, 200);
    }

    #[test]
    fn bincode_roundtrip_key_dump_response() {
        let mut entries = HashMap::new();
        let mut counter = PnCounter::new();
        counter.increment(&nid("node-1"));
        entries.insert("hits".to_string(), CrdtValue::Counter(counter));

        let mut timestamps = HashMap::new();
        timestamps.insert("hits".to_string(), hlc(500, 0, "node-1"));

        let resp = KeyDumpResponse {
            entries,
            frontier: Some(hlc(500, 0, "node-1")),
            timestamps,
        };

        let (bytes, _) = serialize_internal(&resp, Some(CONTENT_TYPE_BINCODE)).unwrap();
        let decoded: KeyDumpResponse =
            deserialize_internal(&bytes, Some(CONTENT_TYPE_BINCODE)).unwrap();
        assert!(decoded.entries.contains_key("hits"));
        assert_eq!(decoded.frontier.unwrap().physical, 500);
        assert_eq!(decoded.timestamps.get("hits").unwrap().physical, 500);
    }

    #[test]
    fn bincode_roundtrip_frontier_push_request() {
        use crate::authority::ack_frontier::AckFrontier;
        use crate::network::frontier_sync::{
            FrontierPullResponse, FrontierPushRequest, FrontierPushResponse,
        };
        use crate::types::{KeyRange, PolicyVersion};

        let req = FrontierPushRequest {
            frontiers: vec![AckFrontier {
                authority_id: nid("auth-1"),
                frontier_hlc: hlc(100, 0, "auth-1"),
                key_range: KeyRange {
                    prefix: "user/".into(),
                },
                policy_version: PolicyVersion(1),
                digest_hash: "hash-1".into(),
            }],
        };

        let (bytes, _) = serialize_internal(&req, Some(CONTENT_TYPE_BINCODE)).unwrap();
        let decoded: FrontierPushRequest =
            deserialize_internal(&bytes, Some(CONTENT_TYPE_BINCODE)).unwrap();
        assert_eq!(decoded.frontiers.len(), 1);
        assert_eq!(decoded.frontiers[0].authority_id, nid("auth-1"));

        // Also test FrontierPushResponse
        let resp = FrontierPushResponse { accepted: 3 };
        let (bytes, _) = serialize_internal(&resp, Some(CONTENT_TYPE_BINCODE)).unwrap();
        let decoded: FrontierPushResponse =
            deserialize_internal(&bytes, Some(CONTENT_TYPE_BINCODE)).unwrap();
        assert_eq!(decoded.accepted, 3);

        // Also test FrontierPullResponse
        let pull_resp = FrontierPullResponse {
            frontiers: vec![AckFrontier {
                authority_id: nid("auth-2"),
                frontier_hlc: hlc(200, 0, "auth-2"),
                key_range: KeyRange {
                    prefix: "order/".into(),
                },
                policy_version: PolicyVersion(2),
                digest_hash: "hash-2".into(),
            }],
        };
        let (bytes, _) = serialize_internal(&pull_resp, Some(CONTENT_TYPE_BINCODE)).unwrap();
        let decoded: FrontierPullResponse =
            deserialize_internal(&bytes, Some(CONTENT_TYPE_BINCODE)).unwrap();
        assert_eq!(decoded.frontiers.len(), 1);
        assert_eq!(decoded.frontiers[0].key_range.prefix, "order/");
    }

    // ---------------------------------------------------------------
    // JSON backward compatibility
    // ---------------------------------------------------------------

    #[test]
    fn json_fallback_when_no_accept_header() {
        let req = SyncRequest {
            sender: "node-1".to_string(),
            entries: HashMap::new(),
        };

        let (bytes, ct) = serialize_internal(&req, None).unwrap();
        assert_eq!(ct, CONTENT_TYPE_JSON);

        // Should be valid JSON
        let decoded: SyncRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.sender, "node-1");
    }

    #[test]
    fn json_fallback_when_accept_is_json() {
        let req = SyncRequest {
            sender: "node-1".to_string(),
            entries: HashMap::new(),
        };

        let (bytes, ct) = serialize_internal(&req, Some(CONTENT_TYPE_JSON)).unwrap();
        assert_eq!(ct, CONTENT_TYPE_JSON);

        let decoded: SyncRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.sender, "node-1");
    }

    #[test]
    fn json_deserialization_when_content_type_is_json() {
        let req = SyncRequest {
            sender: "node-1".to_string(),
            entries: HashMap::new(),
        };

        let json_bytes = serde_json::to_vec(&req).unwrap();
        let decoded: SyncRequest =
            deserialize_internal(&json_bytes, Some(CONTENT_TYPE_JSON)).unwrap();
        assert_eq!(decoded.sender, "node-1");
    }

    #[test]
    fn json_deserialization_when_content_type_is_none() {
        let req = SyncRequest {
            sender: "node-1".to_string(),
            entries: HashMap::new(),
        };

        let json_bytes = serde_json::to_vec(&req).unwrap();
        let decoded: SyncRequest = deserialize_internal(&json_bytes, None).unwrap();
        assert_eq!(decoded.sender, "node-1");
    }

    // ---------------------------------------------------------------
    // Content-Type negotiation helpers
    // ---------------------------------------------------------------

    #[test]
    fn accepts_bincode_detects_octet_stream() {
        assert!(accepts_bincode(Some("application/octet-stream")));
        assert!(accepts_bincode(Some(
            "application/octet-stream, application/json"
        )));
    }

    #[test]
    fn accepts_bincode_rejects_json() {
        assert!(!accepts_bincode(Some("application/json")));
        assert!(!accepts_bincode(None));
    }

    #[test]
    fn accepts_bincode_case_insensitive() {
        assert!(accepts_bincode(Some("Application/Octet-Stream")));
        assert!(accepts_bincode(Some("APPLICATION/OCTET-STREAM")));
        assert!(accepts_bincode(Some(
            "application/json, Application/Octet-Stream"
        )));
    }

    #[test]
    fn accepts_bincode_with_q_values() {
        // Bincode with explicit q=1 should be accepted
        assert!(accepts_bincode(Some("application/octet-stream;q=1")));
        assert!(accepts_bincode(Some("application/octet-stream; q=0.9")));

        // Bincode with q=0 means "not acceptable"
        assert!(!accepts_bincode(Some("application/octet-stream;q=0")));
        assert!(!accepts_bincode(Some("application/octet-stream; q=0")));
        assert!(!accepts_bincode(Some("application/octet-stream; q=0.0")));
    }

    #[test]
    fn accepts_bincode_with_parameters() {
        // Parameters after the media type should be ignored for matching
        assert!(accepts_bincode(Some(
            "application/octet-stream; charset=utf-8"
        )));
        assert!(accepts_bincode(Some(
            "application/octet-stream; charset=utf-8; q=0.8"
        )));
    }

    #[test]
    fn accepts_bincode_multiple_types_with_q_values() {
        assert!(accepts_bincode(Some(
            "application/json;q=0.8, application/octet-stream;q=0.9"
        )));
        // Bincode explicitly excluded but JSON present
        assert!(!accepts_bincode(Some(
            "application/json;q=0.8, application/octet-stream;q=0"
        )));
    }

    #[test]
    fn accepts_bincode_empty_and_whitespace() {
        assert!(!accepts_bincode(Some("")));
        assert!(!accepts_bincode(Some("  ")));
        assert!(!accepts_bincode(Some(",")));
    }

    #[test]
    fn is_bincode_content_type_detects_octet_stream() {
        assert!(is_bincode_content_type(Some("application/octet-stream")));
        assert!(!is_bincode_content_type(Some("application/json")));
        assert!(!is_bincode_content_type(None));
    }

    #[test]
    fn is_bincode_content_type_case_insensitive() {
        assert!(is_bincode_content_type(Some("Application/Octet-Stream")));
        assert!(is_bincode_content_type(Some("APPLICATION/OCTET-STREAM")));
    }

    #[test]
    fn is_bincode_content_type_ignores_parameters() {
        assert!(is_bincode_content_type(Some(
            "application/octet-stream; charset=utf-8"
        )));
        assert!(is_bincode_content_type(Some(
            "application/octet-stream;boundary=something"
        )));
    }

    #[test]
    fn is_bincode_content_type_rejects_partial_match() {
        // Should not match substrings
        assert!(!is_bincode_content_type(Some("x-application/octet-stream")));
        assert!(!is_bincode_content_type(Some(
            "application/octet-stream-extra"
        )));
    }

    // ---------------------------------------------------------------
    // Payload size comparison (bincode vs JSON)
    // ---------------------------------------------------------------

    #[test]
    fn bincode_is_smaller_than_json() {
        let mut entries = HashMap::new();
        for i in 0..10 {
            let mut counter = PnCounter::new();
            counter.increment(&nid("node-1"));
            entries.insert(format!("key-{i}"), CrdtValue::Counter(counter));
        }

        let req = SyncRequest {
            sender: "node-1".to_string(),
            entries,
        };

        let json_bytes = serde_json::to_vec(&req).unwrap();
        let (bincode_bytes, _) = serialize_internal(&req, Some(CONTENT_TYPE_BINCODE)).unwrap();

        assert!(
            bincode_bytes.len() < json_bytes.len(),
            "bincode ({} bytes) should be smaller than JSON ({} bytes)",
            bincode_bytes.len(),
            json_bytes.len()
        );
    }
}
