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

    #[error("internal error: {0}")]
    Internal(String),
}
