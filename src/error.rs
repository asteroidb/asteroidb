use thiserror::Error;

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

    #[error("incompatible format version: data={data_version}, code={code_version}")]
    IncompatibleVersion {
        data_version: u32,
        code_version: u32,
    },

    #[error("migration failed from v{from} to v{to}: {reason}")]
    MigrationFailed { from: u32, to: u32, reason: String },

    #[error("internal error: {0}")]
    Internal(String),
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
