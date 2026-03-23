//! Stub BLS types for builds without the `native-crypto` feature.
//!
//! These types preserve the same serde wire format as the real BLS types
//! (hex-encoded byte strings) so that certificates can still be serialized
//! and deserialized. Verification always fails at runtime.

use serde::{Deserialize, Serialize};

/// Opaque stub for a BLS public key (hex-encoded bytes).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlsPublicKey(pub String);

/// Opaque stub for a BLS signature (hex-encoded bytes).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlsSignature(pub String);
