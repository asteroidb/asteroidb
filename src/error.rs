use thiserror::Error;

/// Error type for Hybrid Logical Clock operations.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum HlcError {
    #[error("HLC logical counter overflow: physical clock is not advancing fast enough")]
    Overflow,
    /// Received timestamp is too far ahead of local wall clock.
    ///
    /// Accepting it would set `self.physical` to a far-future value, causing
    /// `now()` to stop advancing and eventually fail with Overflow (DoS vector).
    #[error(
        "HLC clock skew too large: received physical={received_ms}, wall={wall_ms}, max_skew_ms={max_skew_ms}"
    )]
    ClockSkew {
        received_ms: u64,
        wall_ms: u64,
        max_skew_ms: u64,
    },
}

/// Common error type for CRDT operations.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum CrdtError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("invalid operation for this CRDT type")]
    InvalidOp,

    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("key not found: {0}")]
    KeyNotFound(String),

    #[error("stale version")]
    StaleVersion,

    #[error("policy denied: {0}")]
    PolicyDenied(String),

    #[error("timeout")]
    Timeout,

    /// The local write succeeded and is durably stored, but the authority
    /// majority could not be reached in time to certify it.
    ///
    /// Callers can distinguish this from a generic [`Timeout`] to know that
    /// the value IS present in the local store and will eventually propagate
    /// to other replicas.  Certification can be retried by calling
    /// `process_certifications` once more authorities report their frontiers,
    /// without re-issuing the write.
    #[error("certification timeout: local write committed but certification did not complete")]
    CertificationTimeout,

    /// A session-token-guarded read could not be satisfied: the local
    /// replica has not (provably) applied all writes covered by the token.
    ///
    /// This is a fail-closed refusal, never a stale answer: the client can
    /// retry, wait longer (`wait_ms`), or try another replica. Maps to
    /// HTTP 412 PRECONDITION_FAILED with a `Retry-After` header.
    #[error(
        "session token not satisfied for key {key}: local replica has not applied the requested writes yet"
    )]
    SessionNotSatisfied { key: String },

    #[error("incompatible format version: data={data_version}, code={code_version}")]
    IncompatibleVersion {
        data_version: u32,
        code_version: u32,
    },

    #[error("migration failed from v{from} to v{to}: {reason}")]
    MigrationFailed { from: u32, to: u32, reason: String },

    /// A durability-layer (WAL append) failure: the mutation was applied
    /// in memory but could NOT be recorded in the write-ahead log, so it
    /// must not be acknowledged as durable.
    ///
    /// This is a degrade signal (e.g. disk full): reads keep working and
    /// the un-acked in-memory effect will converge via anti-entropy. Maps
    /// to HTTP 503 SERVICE_UNAVAILABLE.
    #[error("storage error: {0}")]
    Storage(String),

    /// The control-plane consensus rejected the operation because this node
    /// is not the current Raft leader.
    ///
    /// Callers should retry the request against the hinted leader (when a
    /// hint is present). Maps to HTTP 503 SERVICE_UNAVAILABLE with the
    /// `NOT_LEADER` error code plus `x-asteroidb-leader-id` /
    /// `x-asteroidb-leader-addr` hint headers and `Retry-After: 1`.
    #[error(
        "not the control-plane leader (leader hint: {})",
        .leader_id.as_deref().unwrap_or("unknown")
    )]
    NotLeader {
        leader_id: Option<String>,
        leader_addr: Option<String>,
    },

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<HlcError> for CrdtError {
    fn from(e: HlcError) -> Self {
        CrdtError::Internal(format!("HLC error: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_invalid_argument() {
        let err = CrdtError::InvalidArgument("bad key".into());
        assert_eq!(err.to_string(), "invalid argument: bad key");
    }

    #[test]
    fn display_invalid_op() {
        let err = CrdtError::InvalidOp;
        assert_eq!(err.to_string(), "invalid operation for this CRDT type");
    }

    #[test]
    fn display_type_mismatch() {
        let err = CrdtError::TypeMismatch {
            expected: "PnCounter".into(),
            actual: "OrSet".into(),
        };
        assert_eq!(
            err.to_string(),
            "type mismatch: expected PnCounter, got OrSet"
        );
    }

    #[test]
    fn display_key_not_found() {
        let err = CrdtError::KeyNotFound("foo".into());
        assert_eq!(err.to_string(), "key not found: foo");
    }

    #[test]
    fn display_stale_version() {
        let err = CrdtError::StaleVersion;
        assert_eq!(err.to_string(), "stale version");
    }

    #[test]
    fn display_policy_denied() {
        let err = CrdtError::PolicyDenied("no authority".into());
        assert_eq!(err.to_string(), "policy denied: no authority");
    }

    #[test]
    fn display_timeout() {
        let err = CrdtError::Timeout;
        assert_eq!(err.to_string(), "timeout");
    }

    #[test]
    fn display_certification_timeout() {
        let err = CrdtError::CertificationTimeout;
        assert_eq!(
            err.to_string(),
            "certification timeout: local write committed but certification did not complete"
        );
    }

    #[test]
    fn display_internal() {
        let err = CrdtError::Internal("unexpected".into());
        assert_eq!(err.to_string(), "internal error: unexpected");
    }

    #[test]
    fn clone_and_eq() {
        let err = CrdtError::InvalidArgument("x".into());
        let err2 = err.clone();
        assert_eq!(err, err2);
    }
}
