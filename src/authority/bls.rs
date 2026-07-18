//! BLS threshold signature support using the `blst` crate.
//!
//! Provides wrappers around BLS12-381 primitives for aggregate signatures.
//! Multiple authority signatures can be combined into a single compact
//! aggregated signature that verifies against the set of signers' public keys.

use serde::{Deserialize, Serialize};

use crate::error::CrdtError;

/// Domain separation tag for BLS signatures (required by blst).
///
/// Note: this is the *basic scheme* (`NUL_`) tag even though aggregate
/// verification uses `fast_aggregate_verify`, which per
/// draft-irtf-cfrg-bls-signature belongs to the proof-of-possession
/// ciphersuite (`POP_` message tag). We keep `NUL_` for wire compatibility
/// with already-produced signatures; rogue-key soundness is provided by the
/// mandatory registration-time proof-of-possession check (see [`verify_pop`]
/// and `KeysetRegistry::register_bls_keys`), which uses the distinct
/// [`POP_DST`] tag, so domain separation between message signatures and
/// possession proofs still holds.
const DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";

/// Domain separation tag for proof-of-possession signatures
/// (draft-irtf-cfrg-bls-signature §4.2.3, proof-of-possession scheme).
///
/// Distinct from [`DST`] so a proof of possession can never be replayed as a
/// certificate signature and vice versa.
const POP_DST: &[u8] = b"BLS_POP_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";

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

/// Proof of possession over a BLS public key (G2 signature, 96 bytes /
/// 192 hex chars).
///
/// A PoP is the holder's signature over the compressed bytes of its own
/// public key, produced under the dedicated [`POP_DST`] domain separation
/// tag (draft-irtf-cfrg-bls-signature §3.3, PopProve). Verifying it before
/// accepting a key for aggregation blocks rogue-key attacks against
/// `fast_aggregate_verify`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlsProofOfPossession(pub blst::min_pk::Signature);

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

impl Serialize for BlsProofOfPossession {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = self.0.to_bytes();
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        serializer.serialize_str(&hex)
    }
}

impl<'de> Deserialize<'de> for BlsProofOfPossession {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let hex = String::deserialize(deserializer)?;
        let bytes = hex_to_bytes(&hex).map_err(serde::de::Error::custom)?;
        let sig = blst::min_pk::Signature::from_bytes(&bytes).map_err(|e| {
            serde::de::Error::custom(format!("invalid BLS proof of possession: {e:?}"))
        })?;
        Ok(BlsProofOfPossession(sig))
    }
}

impl BlsProofOfPossession {
    /// Return the raw bytes of this proof of possession (96 bytes for min_pk).
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes().to_vec()
    }

    /// Return the hex encoding of this proof of possession (192 hex characters).
    pub fn to_hex(&self) -> String {
        self.0
            .to_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    /// Parse a proof of possession from its hex encoding. Returns `None` on
    /// invalid input (the group membership check is performed by
    /// `Signature::from_bytes`).
    pub fn from_hex(hex: &str) -> Option<Self> {
        let bytes = hex_to_bytes(hex).ok()?;
        let sig = blst::min_pk::Signature::from_bytes(&bytes).ok()?;
        Some(BlsProofOfPossession(sig))
    }
}

impl BlsSignature {
    /// Return the raw bytes of this BLS signature (96 bytes for min_pk).
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes().to_vec()
    }

    /// Return the hex encoding of this signature (192 hex characters).
    pub fn to_hex(&self) -> String {
        self.0
            .to_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    /// Parse a signature from its hex encoding. Returns `None` on invalid input.
    pub fn from_hex(hex: &str) -> Option<Self> {
        let bytes = hex_to_bytes(hex).ok()?;
        let sig = blst::min_pk::Signature::from_bytes(&bytes).ok()?;
        Some(BlsSignature(sig))
    }
}

impl BlsPublicKey {
    /// Return the hex encoding of this public key (96 hex characters).
    pub fn to_hex(&self) -> String {
        self.0
            .to_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    /// Parse a public key from its hex encoding. Returns `None` on invalid input.
    pub fn from_hex(hex: &str) -> Option<Self> {
        let bytes = hex_to_bytes(hex).ok()?;
        let pk = blst::min_pk::PublicKey::from_bytes(&bytes).ok()?;
        Some(BlsPublicKey(pk))
    }
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, String> {
    // Operate on the raw bytes rather than string slices: slicing `&hex[i..i+2]`
    // panics when the offset lands inside a multi-byte UTF-8 code point, and this
    // runs on peer-supplied hex during Deserialize (remote-triggerable).
    let bytes = hex.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err("odd-length hex string".to_string());
    }
    fn nibble(b: u8) -> Result<u8, String> {
        match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            b'A'..=b'F' => Ok(b - b'A' + 10),
            _ => Err("invalid hex character".to_string()),
        }
    }
    bytes
        .chunks_exact(2)
        .map(|pair| Ok((nibble(pair[0])? << 4) | nibble(pair[1])?))
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

    /// Produce a proof of possession for this keypair's public key.
    ///
    /// See [`generate_pop`]. blst signatures are deterministic, so the same
    /// seed always yields the same PoP hex — convenient for static key
    /// distribution via `ASTEROIDB_AUTHORITY_KEYS`.
    pub fn proof_of_possession(&self) -> BlsProofOfPossession {
        generate_pop(&self.secret_key)
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

/// Generate a proof of possession: the holder's signature over its own
/// compressed public key bytes under [`POP_DST`]
/// (draft-irtf-cfrg-bls-signature §3.3, PopProve).
pub fn generate_pop(secret_key: &blst::min_pk::SecretKey) -> BlsProofOfPossession {
    let pk_bytes = secret_key.sk_to_pk().to_bytes();
    BlsProofOfPossession(secret_key.sign(&pk_bytes, POP_DST, &[]))
}

/// Verify a proof of possession against a BLS public key
/// (draft-irtf-cfrg-bls-signature §3.3, PopVerify).
///
/// **`pk_validate = true` is load-bearing here**: blst's
/// `fast_aggregate_verify` has no public-key validation parameter and
/// `PublicKey::from_bytes` performs no subgroup check, so this call is the
/// *only* subgroup / infinity-point check applied to keys entering the
/// keyset registry. Do not relax it as an optimisation.
pub fn verify_pop(public_key: &BlsPublicKey, pop: &BlsProofOfPossession) -> bool {
    let pk_bytes = public_key.0.to_bytes();
    let result = pop
        .0
        .verify(true, &pk_bytes, POP_DST, &[], &public_key.0, true);
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

    // ---------------------------------------------------------------
    // Proof of possession
    // ---------------------------------------------------------------

    #[test]
    fn pop_generate_and_verify() {
        let kp = make_keypair(70);
        let pop = kp.proof_of_possession();
        assert!(verify_pop(&kp.public_key, &pop));
    }

    #[test]
    fn pop_wrong_key_fails() {
        let kp1 = make_keypair(71);
        let kp2 = make_keypair(72);
        let pop1 = kp1.proof_of_possession();
        assert!(!verify_pop(&kp2.public_key, &pop1));
    }

    #[test]
    fn pop_serde_roundtrip() {
        let kp = make_keypair(73);
        let pop = kp.proof_of_possession();
        let json = serde_json::to_string(&pop).unwrap();
        let restored: BlsProofOfPossession = serde_json::from_str(&json).unwrap();
        assert_eq!(pop, restored);
    }

    #[test]
    fn pop_hex_roundtrip_is_192_chars() {
        let kp = make_keypair(74);
        let pop = kp.proof_of_possession();
        let hex = pop.to_hex();
        assert_eq!(hex.len(), 192, "min_pk PoP is a 96-byte G2 signature");
        let restored = BlsProofOfPossession::from_hex(&hex).unwrap();
        assert_eq!(pop, restored);
    }

    /// A message signature (NUL_ DST) over the public key bytes must not be
    /// accepted as a proof of possession (POP_ DST): domain separation.
    #[test]
    fn message_signature_is_not_valid_pop() {
        let kp = make_keypair(75);
        let pk_bytes = kp.public_key.0.to_bytes();
        let sig = sign_message(kp.secret_key(), &pk_bytes);
        let forged_pop = BlsProofOfPossession(sig.0);
        assert!(!verify_pop(&kp.public_key, &forged_pop));
    }

    /// A proof of possession must not verify as a message signature over the
    /// public key bytes: domain separation in the other direction.
    #[test]
    fn pop_is_not_valid_message_signature() {
        let kp = make_keypair(76);
        let pk_bytes = kp.public_key.0.to_bytes();
        let pop = kp.proof_of_possession();
        let as_sig = BlsSignature(pop.0);
        assert!(!verify_signature(&kp.public_key, &pk_bytes, &as_sig));
    }

    /// The G1 point at infinity must never pass PoP verification. Whether the
    /// rejection happens at deserialization or inside `verify_pop`
    /// (`pk_validate = true`), the invalid key must not be accepted.
    #[test]
    fn pop_rejects_infinity_public_key() {
        // Compressed G1 infinity: 0xc0 followed by 47 zero bytes.
        let mut inf = [0u8; 48];
        inf[0] = 0xc0;
        let hex: String = inf.iter().map(|b| format!("{b:02x}")).collect();
        match BlsPublicKey::from_hex(&hex) {
            Some(pk) => {
                let kp = make_keypair(77);
                let pop = kp.proof_of_possession();
                assert!(
                    !verify_pop(&pk, &pop),
                    "pk_validate must reject the infinity public key"
                );
            }
            None => {
                // Deserialization already rejects the infinity encoding; the
                // group check is enforced before `verify_pop` even runs.
            }
        }
    }
}
