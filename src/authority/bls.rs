//! BLS threshold signature support using the `blst` crate.
//!
//! Provides wrappers around BLS12-381 primitives for aggregate signatures.
//! Multiple authority signatures can be combined into a single compact
//! aggregated signature that verifies against the set of signers' public keys.

use serde::{Deserialize, Serialize};

use crate::error::CrdtError;

/// Domain separation tag for BLS signatures (required by blst).
const DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";

/// Wrapper around a BLS secret key.
#[derive(Clone)]
pub struct BlsKeypair {
    /// The secret key (not serializable for security).
    secret_key: blst::min_pk::SecretKey,
    /// The corresponding public key.
    pub public_key: BlsPublicKey,
}

/// Wrapper around a BLS public key with serde support (hex encoding).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlsPublicKey(pub blst::min_pk::PublicKey);

/// Wrapper around a BLS signature with serde support (hex encoding).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlsSignature(pub blst::min_pk::Signature);

// ---------------------------------------------------------------------------
// Serde: hex-encoded bytes
// ---------------------------------------------------------------------------

impl Serialize for BlsPublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = self.0.to_bytes();
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        serializer.serialize_str(&hex)
    }
}

impl<'de> Deserialize<'de> for BlsPublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let hex = String::deserialize(deserializer)?;
        let bytes = hex_to_bytes(&hex).map_err(serde::de::Error::custom)?;
        let pk = blst::min_pk::PublicKey::from_bytes(&bytes)
            .map_err(|e| serde::de::Error::custom(format!("invalid BLS public key: {e:?}")))?;
        Ok(BlsPublicKey(pk))
    }
}

impl Serialize for BlsSignature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = self.0.to_bytes();
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        serializer.serialize_str(&hex)
    }
}

impl<'de> Deserialize<'de> for BlsSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let hex = String::deserialize(deserializer)?;
        let bytes = hex_to_bytes(&hex).map_err(serde::de::Error::custom)?;
        let sig = blst::min_pk::Signature::from_bytes(&bytes)
            .map_err(|e| serde::de::Error::custom(format!("invalid BLS signature: {e:?}")))?;
        Ok(BlsSignature(sig))
    }
}

impl BlsSignature {
    /// Return the raw bytes of this BLS signature (96 bytes for min_pk).
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes().to_vec()
    }
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, String> {
    if !hex.len().is_multiple_of(2) {
        return Err("odd-length hex string".to_string());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|e| format!("invalid hex character: {e}"))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Key generation
// ---------------------------------------------------------------------------

impl BlsKeypair {
    /// Generate a new BLS keypair from random bytes.
    pub fn generate(ikm: &[u8; 32]) -> Self {
        let sk = blst::min_pk::SecretKey::key_gen(ikm, &[]).expect("key_gen with 32-byte IKM");
        let pk = sk.sk_to_pk();
        Self {
            secret_key: sk,
            public_key: BlsPublicKey(pk),
        }
    }

    /// Return a reference to the inner secret key.
    pub fn secret_key(&self) -> &blst::min_pk::SecretKey {
        &self.secret_key
    }
}

// ---------------------------------------------------------------------------
// Core operations
// ---------------------------------------------------------------------------

/// Sign a message with a BLS secret key.
pub fn sign_message(secret_key: &blst::min_pk::SecretKey, message: &[u8]) -> BlsSignature {
    let sig = secret_key.sign(message, DST, &[]);
    BlsSignature(sig)
}

/// Verify a single BLS signature against a public key and message.
pub fn verify_signature(
    public_key: &BlsPublicKey,
    message: &[u8],
    signature: &BlsSignature,
) -> bool {
    let result = signature
        .0
        .verify(true, message, DST, &[], &public_key.0, true);
    result == blst::BLST_ERROR::BLST_SUCCESS
}

/// Aggregate multiple BLS signatures into a single signature.
///
/// Returns an error if `signatures` is empty.
pub fn aggregate_signatures(signatures: &[BlsSignature]) -> Result<BlsSignature, CrdtError> {
    if signatures.is_empty() {
        return Err(CrdtError::InvalidArgument(
            "cannot aggregate zero signatures".into(),
        ));
    }

    let refs: Vec<&blst::min_pk::Signature> = signatures.iter().map(|s| &s.0).collect();
    let agg = blst::min_pk::AggregateSignature::aggregate(&refs, true)
        .map_err(|e| CrdtError::InvalidArgument(format!("BLS aggregate failed: {e:?}")))?;
    Ok(BlsSignature(agg.to_signature()))
}

/// Verify an aggregated BLS signature against the corresponding set of
/// public keys — all signers must have signed the **same** `message`.
///
/// Returns `true` if the aggregated signature is valid for all given
/// public keys over the shared message.
pub fn aggregate_verify(
    public_keys: &[BlsPublicKey],
    message: &[u8],
    aggregated_sig: &BlsSignature,
) -> bool {
    if public_keys.is_empty() {
        return false;
    }

    // For same-message aggregation we use `fast_aggregate_verify`.
    let pk_refs: Vec<&blst::min_pk::PublicKey> = public_keys.iter().map(|pk| &pk.0).collect();
    let result = aggregated_sig
        .0
        .fast_aggregate_verify(true, message, DST, &pk_refs);
    result == blst::BLST_ERROR::BLST_SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_keypair(seed: u8) -> BlsKeypair {
        let mut ikm = [0u8; 32];
        ikm[0] = seed;
        ikm[31] = seed.wrapping_add(42);
        BlsKeypair::generate(&ikm)
    }

    #[test]
    fn sign_and_verify_single() {
        let kp = make_keypair(1);
        let msg = b"hello BLS";
        let sig = sign_message(kp.secret_key(), msg);
        assert!(verify_signature(&kp.public_key, msg, &sig));
    }

    #[test]
    fn verify_wrong_message_fails() {
        let kp = make_keypair(2);
        let sig = sign_message(kp.secret_key(), b"correct message");
        assert!(!verify_signature(&kp.public_key, b"wrong message", &sig));
    }

    #[test]
    fn verify_wrong_key_fails() {
        let kp1 = make_keypair(3);
        let kp2 = make_keypair(4);
        let sig = sign_message(kp1.secret_key(), b"msg");
        assert!(!verify_signature(&kp2.public_key, b"msg", &sig));
    }

    #[test]
    fn aggregate_and_verify() {
        let msg = b"aggregate me";
        let keypairs: Vec<BlsKeypair> = (10..15).map(make_keypair).collect();

        let sigs: Vec<BlsSignature> = keypairs
            .iter()
            .map(|kp| sign_message(kp.secret_key(), msg))
            .collect();

        let agg = aggregate_signatures(&sigs).unwrap();

        let pks: Vec<BlsPublicKey> = keypairs.iter().map(|kp| kp.public_key.clone()).collect();
        assert!(aggregate_verify(&pks, msg, &agg));
    }

    #[test]
    fn aggregate_verify_wrong_message_fails() {
        let msg = b"original";
        let keypairs: Vec<BlsKeypair> = (20..23).map(make_keypair).collect();

        let sigs: Vec<BlsSignature> = keypairs
            .iter()
            .map(|kp| sign_message(kp.secret_key(), msg))
            .collect();

        let agg = aggregate_signatures(&sigs).unwrap();
        let pks: Vec<BlsPublicKey> = keypairs.iter().map(|kp| kp.public_key.clone()).collect();

        assert!(!aggregate_verify(&pks, b"tampered", &agg));
    }

    #[test]
    fn aggregate_verify_missing_signer_fails() {
        let msg = b"partial";
        let keypairs: Vec<BlsKeypair> = (30..34).map(make_keypair).collect();

        let sigs: Vec<BlsSignature> = keypairs
            .iter()
            .map(|kp| sign_message(kp.secret_key(), msg))
            .collect();

        let agg = aggregate_signatures(&sigs).unwrap();

        // Only use first 3 public keys (missing the 4th signer).
        let pks: Vec<BlsPublicKey> = keypairs[..3]
            .iter()
            .map(|kp| kp.public_key.clone())
            .collect();
        assert!(!aggregate_verify(&pks, msg, &agg));
    }

    #[test]
    fn aggregate_verify_empty_keys_returns_false() {
        let kp = make_keypair(40);
        let sig = sign_message(kp.secret_key(), b"x");
        let agg = aggregate_signatures(&[sig]).unwrap();
        assert!(!aggregate_verify(&[], b"x", &agg));
    }

    #[test]
    fn aggregate_empty_returns_error() {
        let result = aggregate_signatures(&[]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot aggregate zero signatures")
        );
    }

    #[test]
    fn serde_roundtrip_public_key() {
        let kp = make_keypair(50);
        let json = serde_json::to_string(&kp.public_key).unwrap();
        let restored: BlsPublicKey = serde_json::from_str(&json).unwrap();
        assert_eq!(kp.public_key, restored);
    }

    #[test]
    fn serde_roundtrip_signature() {
        let kp = make_keypair(51);
        let sig = sign_message(kp.secret_key(), b"serde test");
        let json = serde_json::to_string(&sig).unwrap();
        let restored: BlsSignature = serde_json::from_str(&json).unwrap();
        assert_eq!(sig, restored);
    }

    #[test]
    fn single_signer_aggregate_equals_direct() {
        let kp = make_keypair(60);
        let msg = b"single aggregate";
        let sig = sign_message(kp.secret_key(), msg);
        let agg = aggregate_signatures(std::slice::from_ref(&sig)).unwrap();

        // Aggregating a single signature should produce a valid aggregate.
        assert!(aggregate_verify(&[kp.public_key], msg, &agg));
    }
}
