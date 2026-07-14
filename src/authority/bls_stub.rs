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

impl BlsPublicKey {
    /// Return the hex encoding of this public key (wire-compatible passthrough).
    pub fn to_hex(&self) -> String {
        self.0.clone()
    }

    /// Parse a public key from its hex encoding (passthrough; never fails).
    pub fn from_hex(hex: &str) -> Option<Self> {
        Some(BlsPublicKey(hex.to_string()))
    }
}

impl BlsSignature {
    /// Return the hex encoding of this signature (wire-compatible passthrough).
    pub fn to_hex(&self) -> String {
        self.0.clone()
    }

    /// Parse a signature from its hex encoding (passthrough; never fails).
    pub fn from_hex(hex: &str) -> Option<Self> {
        Some(BlsSignature(hex.to_string()))
    }

    /// Return deterministic bytes for this signature (the raw hex string
    /// bytes). Mirrors the real type's `to_bytes` for cache-key purposes;
    /// no cryptographic meaning in stub builds.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }
}
