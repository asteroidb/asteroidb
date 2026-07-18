//! Frontier report signing and verification (FR-008).
//!
//! Connects the signature generation pipeline end-to-end: an Authority node
//! signs each frontier report it produces, receivers verify the signature
//! against the keyset registry before accepting it, and the verified
//! attestations are later aggregated into a `MajorityCertificate` (Ed25519)
//! and, when BLS keys are available, a BLS `DualModeCertificate`.
//!
//! Two signatures are produced per report:
//!
//! 1. **Report signature** — an Ed25519 signature over a domain-separated
//!    canonical encoding of the *entire* `AckFrontier`. This binds the
//!    signature to the exact frontier HLC, digest hash, scope, and authority
//!    ID, preventing an attacker from re-attaching a genuine attestation to
//!    a forged frontier with an inflated timestamp.
//! 2. **Certificate signature** — an Ed25519 (and optionally BLS) signature
//!    over [`create_certificate_message`] computed for the *checkpoint* HLC,
//!    a floor-normalised form of the frontier HLC. Because all authorities
//!    normalise to the same checkpoint, their signatures cover the same
//!    message bytes, enabling `MajorityCertificate` single-message
//!    verification and BLS `fast_aggregate_verify`.

use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::authority::ack_frontier::AckFrontier;
#[cfg(feature = "native-crypto")]
use crate::authority::bls::{self, BlsKeypair, BlsProofOfPossession, BlsPublicKey, BlsSignature};
#[cfg(not(feature = "native-crypto"))]
use crate::authority::bls_stub::{BlsPublicKey, BlsSignature};
use crate::authority::certificate::{
    CertError, EpochConfig, KeysetRegistry, KeysetVersion, create_certificate_message, sign_message,
};
use crate::hlc::HlcTimestamp;
use crate::types::NodeId;

/// Width of a certificate checkpoint bucket in milliseconds.
///
/// Frontier HLCs are floor-normalised to this granularity before certificate
/// signing so that all authorities sign identical message bytes.
pub const CHECKPOINT_INTERVAL_MS: u64 = 1_000;

/// Domain separation tag for frontier report signatures.
///
/// Guarantees that a report signature can never be confused with a
/// certificate signature (which is not tagged), because the certificate
/// message starts with a length-prefixed key range prefix instead.
const FRONTIER_REPORT_DOMAIN: &[u8] = b"asteroidb/frontier-report/v1";

/// Floor-normalise a frontier HLC to its certificate checkpoint.
///
/// The checkpoint has `physical` rounded down to a multiple of
/// [`CHECKPOINT_INTERVAL_MS`], `logical = 0`, and an empty `node_id`.
/// The empty node ID is intentional: in `HlcTimestamp` lexicographic
/// ordering `checkpoint{p, 0, ""} < write{p, l, "n"}`, so a write on the
/// bucket boundary conservatively requires the *next* checkpoint.
pub fn checkpoint_hlc(ts: &HlcTimestamp) -> HlcTimestamp {
    HlcTimestamp {
        physical: ts.physical - ts.physical % CHECKPOINT_INTERVAL_MS,
        logical: 0,
        node_id: String::new(),
    }
}

/// Canonical, domain-separated message bytes for a frontier report signature.
///
/// Every variable-length field is length-prefixed (big-endian `u32`) so that
/// no two distinct frontiers can encode to the same byte sequence.
pub fn create_frontier_report_message(frontier: &AckFrontier) -> Vec<u8> {
    let mut buf = Vec::new();
    let push_bytes = |buf: &mut Vec<u8>, bytes: &[u8]| {
        buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(bytes);
    };
    push_bytes(&mut buf, FRONTIER_REPORT_DOMAIN);
    push_bytes(&mut buf, frontier.authority_id.0.as_bytes());
    push_bytes(&mut buf, frontier.key_range.prefix.as_bytes());
    buf.extend_from_slice(&frontier.frontier_hlc.physical.to_be_bytes());
    buf.extend_from_slice(&frontier.frontier_hlc.logical.to_be_bytes());
    push_bytes(&mut buf, frontier.frontier_hlc.node_id.as_bytes());
    buf.extend_from_slice(&frontier.policy_version.0.to_be_bytes());
    push_bytes(&mut buf, frontier.digest_hash.as_bytes());
    buf
}

/// Dual-mode signature attached alongside an `AckFrontier` in transport.
///
/// Carried in a parallel lane (`FrontierPushRequest::signatures`) so the
/// `AckFrontier` wire format itself stays unchanged for old nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrontierSignature {
    /// The keyset version under which the signatures were produced.
    pub keyset_version: KeysetVersion,
    /// Hex-encoded Ed25519 public key (32 bytes). Receivers only use this to
    /// cross-check against the registry key; the registry key is what verifies.
    pub ed25519_public_key: String,
    /// Hex-encoded Ed25519 signature (64 bytes) over
    /// [`create_frontier_report_message`].
    pub report_signature: String,
    /// The checkpoint HLC the certificate signature covers. Must equal
    /// `checkpoint_hlc(frontier.frontier_hlc)` exactly.
    pub checkpoint_hlc: HlcTimestamp,
    /// Hex-encoded Ed25519 signature (64 bytes) over
    /// `create_certificate_message(key_range, checkpoint_hlc, policy_version)`.
    pub cert_signature: String,
    /// Optional BLS public key (hex wire format, native and stub compatible).
    #[serde(default)]
    pub bls_public_key: Option<BlsPublicKey>,
    /// Optional BLS signature over the same certificate message.
    #[serde(default)]
    pub bls_cert_signature: Option<BlsSignature>,
}

/// An attestation that has passed signature verification.
///
/// Keys and signatures are already parsed; the Ed25519 key is the
/// registry-trusted value (never the embedded one).
#[derive(Debug, Clone)]
pub struct VerifiedAttestation {
    /// The authority that produced the attestation.
    pub authority_id: NodeId,
    /// The keyset version the signatures were produced under.
    pub keyset_version: KeysetVersion,
    /// The checkpoint the certificate signature covers.
    pub checkpoint_hlc: HlcTimestamp,
    /// Registry-trusted Ed25519 verifying key and the certificate signature.
    pub ed25519: (VerifyingKey, Signature),
    /// Optional BLS public key and certificate signature (native-crypto only).
    pub bls: Option<(BlsPublicKey, BlsSignature)>,
}

/// Holder of this node's signing key material.
///
/// Resolves the historical gap where the Ed25519 `SigningKey` was derived in
/// `main.rs` only to extract the verifying key and then dropped: the signer
/// owns the key for the lifetime of the node and produces frontier signatures.
pub struct NodeSigner {
    node_id: NodeId,
    signing_key: SigningKey,
    #[cfg(feature = "native-crypto")]
    bls: Option<BlsKeypair>,
}

impl std::fmt::Debug for NodeSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeSigner")
            .field("node_id", &self.node_id.0)
            .field("signing_key", &"[REDACTED]")
            .finish()
    }
}

impl NodeSigner {
    /// Derive a signer from a 32-byte seed.
    ///
    /// Uses the same derivation as the keyset registration path:
    /// `SigningKey::from_bytes(seed)` for Ed25519 and, when `enable_bls`
    /// is set, `BlsKeypair::generate(seed)` for BLS.
    #[cfg(feature = "native-crypto")]
    pub fn from_seed(node_id: NodeId, seed: &[u8; 32], enable_bls: bool) -> Self {
        Self {
            node_id,
            signing_key: SigningKey::from_bytes(seed),
            bls: enable_bls.then(|| BlsKeypair::generate(seed)),
        }
    }

    /// Derive a signer from a 32-byte seed (Ed25519 only; native-crypto disabled).
    #[cfg(not(feature = "native-crypto"))]
    pub fn from_seed(node_id: NodeId, seed: &[u8; 32]) -> Self {
        Self {
            node_id,
            signing_key: SigningKey::from_bytes(seed),
        }
    }

    /// Return this signer's node ID.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Return the Ed25519 verifying key for this signer.
    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    /// Return the BLS public key, if BLS signing is enabled.
    #[cfg(feature = "native-crypto")]
    pub fn bls_public_key(&self) -> Option<BlsPublicKey> {
        self.bls.as_ref().map(|kp| kp.public_key.clone())
    }

    /// Return a proof of possession over the BLS public key, if BLS signing
    /// is enabled.
    ///
    /// Required alongside the public key when registering into a
    /// `KeysetRegistry` and when distributing this node's keys via
    /// `ASTEROIDB_AUTHORITY_KEYS`. Deterministic for a given seed.
    #[cfg(feature = "native-crypto")]
    pub fn bls_proof_of_possession(&self) -> Option<BlsProofOfPossession> {
        self.bls.as_ref().map(|kp| kp.proof_of_possession())
    }

    /// Sign a frontier report.
    ///
    /// Produces the report signature (over the whole frontier) and the
    /// checkpoint certificate signature. When a BLS keypair is held, a BLS
    /// signature over the same certificate message is attached as well.
    /// `keyset_version` must be the version under which this node's public
    /// keys are registered (resolved by the caller from the shared registry).
    pub fn sign_frontier(
        &self,
        frontier: &AckFrontier,
        keyset_version: KeysetVersion,
    ) -> FrontierSignature {
        let report_message = create_frontier_report_message(frontier);
        let report_sig = sign_message(&self.signing_key, &report_message);

        let checkpoint = checkpoint_hlc(&frontier.frontier_hlc);
        let cert_message =
            create_certificate_message(&frontier.key_range, &checkpoint, &frontier.policy_version);
        let cert_sig = sign_message(&self.signing_key, &cert_message);

        #[cfg(feature = "native-crypto")]
        let (bls_public_key, bls_cert_signature) = match &self.bls {
            Some(kp) => (
                Some(kp.public_key.clone()),
                Some(bls::sign_message(kp.secret_key(), &cert_message)),
            ),
            None => (None, None),
        };
        #[cfg(not(feature = "native-crypto"))]
        let (bls_public_key, bls_cert_signature) = (None, None);

        FrontierSignature {
            keyset_version,
            ed25519_public_key: hex::encode(self.verifying_key().as_bytes()),
            report_signature: hex::encode(report_sig.to_bytes()),
            checkpoint_hlc: checkpoint,
            cert_signature: hex::encode(cert_sig.to_bytes()),
            bls_public_key,
            bls_cert_signature,
        }
    }

    /// Build a `VerifiedAttestation` from this node's own signature without
    /// re-verifying it (local reporting path — the key material is our own).
    pub fn self_verified(
        &self,
        frontier: &AckFrontier,
        sig: &FrontierSignature,
    ) -> VerifiedAttestation {
        let cert_sig = decode_signature(&sig.cert_signature)
            .expect("self-produced certificate signature must be well-formed");
        VerifiedAttestation {
            authority_id: frontier.authority_id.clone(),
            keyset_version: sig.keyset_version.clone(),
            checkpoint_hlc: sig.checkpoint_hlc.clone(),
            ed25519: (self.verifying_key(), cert_sig),
            bls: sig
                .bls_public_key
                .clone()
                .zip(sig.bls_cert_signature.clone()),
        }
    }
}

/// Decode a hex-encoded 64-byte Ed25519 signature.
fn decode_signature(hex_str: &str) -> Option<Signature> {
    let bytes: [u8; 64] = hex::decode(hex_str).ok()?.try_into().ok()?;
    Some(Signature::from_bytes(&bytes))
}

/// Verify a frontier signature against the keyset registry.
///
/// Only registry keys are trusted for verification; the embedded public key
/// is used solely for a consistency cross-check. Checks, in order:
///
/// 1. `keyset_version` is known and within the epoch grace period.
/// 2. The authority is present in the registry for that keyset version.
/// 3. The embedded Ed25519 key matches the registry key.
/// 4. The report signature verifies over [`create_frontier_report_message`].
/// 5. `checkpoint_hlc` equals `checkpoint_hlc(frontier.frontier_hlc)` exactly.
/// 6. The certificate signature verifies over the checkpoint message.
/// 7. When BLS fields are present (native-crypto builds) and the registry
///    holds a BLS key for the authority, the embedded BLS key must match the
///    registry BLS key and the BLS signature must verify. When the registry
///    has no BLS key for the authority (e.g. `ASTEROIDB_AUTHORITY_KEYS`
///    distributed Ed25519 keys only), the BLS lane is ignored and the
///    attestation degrades to Ed25519-only — rejecting it outright would
///    let a documented-valid key distribution halt all frontier exchange.
///    A half-populated BLS pair is rejected. Non-native builds ignore the
///    BLS fields entirely.
pub fn verify_frontier_signature(
    frontier: &AckFrontier,
    sig: &FrontierSignature,
    registry: &KeysetRegistry,
    current_epoch: u64,
    epoch_config: &EpochConfig,
) -> Result<VerifiedAttestation, CertError> {
    // (1) Keyset version must be known and within grace.
    if !registry.is_version_valid(&sig.keyset_version, current_epoch, epoch_config) {
        if registry.get_keys(&sig.keyset_version).is_none() {
            return Err(CertError::UnknownKeyset(sig.keyset_version.0));
        }
        let keyset_epoch = registry.registered_epoch(&sig.keyset_version).unwrap_or(0);
        return Err(CertError::ExpiredKeyset {
            version: sig.keyset_version.0,
            keyset_epoch,
            current_epoch,
        });
    }

    // (2) The authority must be registered.
    let registry_key = registry
        .get_key_for_authority(&sig.keyset_version, &frontier.authority_id)
        .ok_or_else(|| CertError::AuthorityNotInRegistry(frontier.authority_id.0.clone()))?;

    // (3) Embedded key must match the registry key.
    let embedded_bytes = hex::decode(&sig.ed25519_public_key)
        .map_err(|_| CertError::InvalidSignature(frontier.authority_id.0.clone()))?;
    if embedded_bytes.as_slice() != registry_key.as_bytes() {
        return Err(CertError::InvalidSignature(format!(
            "embedded Ed25519 key mismatch for {}",
            frontier.authority_id.0
        )));
    }

    // (4) Report signature binds the full frontier contents.
    let report_sig = decode_signature(&sig.report_signature)
        .ok_or_else(|| CertError::InvalidSignature(frontier.authority_id.0.clone()))?;
    let report_message = create_frontier_report_message(frontier);
    registry_key
        .verify(&report_message, &report_sig)
        .map_err(|_| CertError::InvalidSignature(frontier.authority_id.0.clone()))?;

    // (5) Checkpoint must be the exact normalisation of the frontier HLC.
    let expected_checkpoint = checkpoint_hlc(&frontier.frontier_hlc);
    if sig.checkpoint_hlc != expected_checkpoint {
        return Err(CertError::InvalidSignature(format!(
            "checkpoint mismatch for {}",
            frontier.authority_id.0
        )));
    }

    // (6) Certificate signature over the checkpoint message.
    let cert_sig = decode_signature(&sig.cert_signature)
        .ok_or_else(|| CertError::InvalidSignature(frontier.authority_id.0.clone()))?;
    let cert_message = create_certificate_message(
        &frontier.key_range,
        &expected_checkpoint,
        &frontier.policy_version,
    );
    registry_key
        .verify(&cert_message, &cert_sig)
        .map_err(|_| CertError::InvalidSignature(frontier.authority_id.0.clone()))?;

    // (7) Optional BLS lane.
    #[cfg(feature = "native-crypto")]
    let bls = match (&sig.bls_public_key, &sig.bls_cert_signature) {
        (Some(embedded_pk), Some(bls_sig)) => {
            let Some(registry_bls) =
                registry.get_bls_key(&sig.keyset_version, &frontier.authority_id.0)
            else {
                // The registry holds no BLS key for this authority (BLS keys
                // are optional in ASTEROIDB_AUTHORITY_KEYS). The Ed25519
                // report and certificate signatures already verified above,
                // so accept the attestation without its BLS lane instead of
                // rejecting the whole frontier.
                return Ok(VerifiedAttestation {
                    authority_id: frontier.authority_id.clone(),
                    keyset_version: sig.keyset_version.clone(),
                    checkpoint_hlc: expected_checkpoint,
                    ed25519: (*registry_key, cert_sig),
                    bls: None,
                });
            };
            if embedded_pk != registry_bls {
                return Err(CertError::InvalidSignature(format!(
                    "embedded BLS key mismatch for {}",
                    frontier.authority_id.0
                )));
            }
            if !bls::verify_signature(registry_bls, &cert_message, bls_sig) {
                return Err(CertError::InvalidSignature(format!(
                    "BLS certificate signature invalid for {}",
                    frontier.authority_id.0
                )));
            }
            Some((registry_bls.clone(), bls_sig.clone()))
        }
        (None, None) => None,
        _ => {
            return Err(CertError::InvalidSignature(format!(
                "half-populated BLS signature fields for {}",
                frontier.authority_id.0
            )));
        }
    };
    #[cfg(not(feature = "native-crypto"))]
    let bls = None;

    Ok(VerifiedAttestation {
        authority_id: frontier.authority_id.clone(),
        keyset_version: sig.keyset_version.clone(),
        checkpoint_hlc: expected_checkpoint,
        ed25519: (*registry_key, cert_sig),
        bls,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{KeyRange, PolicyVersion};

    fn node(name: &str) -> NodeId {
        NodeId(name.into())
    }

    fn seed(byte: u8) -> [u8; 32] {
        let mut s = [0u8; 32];
        s[0] = byte;
        s[31] = byte.wrapping_add(7);
        s
    }

    #[cfg(feature = "native-crypto")]
    fn make_signer(name: &str, byte: u8, bls: bool) -> NodeSigner {
        NodeSigner::from_seed(node(name), &seed(byte), bls)
    }

    #[cfg(not(feature = "native-crypto"))]
    fn make_signer(name: &str, byte: u8, _bls: bool) -> NodeSigner {
        NodeSigner::from_seed(node(name), &seed(byte))
    }

    fn make_frontier(authority: &str, physical: u64, logical: u32) -> AckFrontier {
        AckFrontier {
            authority_id: node(authority),
            frontier_hlc: HlcTimestamp {
                physical,
                logical,
                node_id: authority.into(),
            },
            key_range: KeyRange {
                prefix: "user/".into(),
            },
            policy_version: PolicyVersion(1),
            digest_hash: format!("{authority}-{physical}-{logical}"),
        }
    }

    /// Build a registry containing the signer's keys at keyset version 1.
    fn registry_with(signers: &[&NodeSigner]) -> KeysetRegistry {
        let mut registry = KeysetRegistry::new();
        let keys = signers
            .iter()
            .map(|s| (s.node_id().clone(), s.verifying_key()))
            .collect();
        registry.register_keyset(KeysetVersion(1), 0, keys).unwrap();
        #[cfg(feature = "native-crypto")]
        {
            let bls_keys: Vec<(String, BlsPublicKey, BlsProofOfPossession)> = signers
                .iter()
                .filter_map(|s| {
                    s.bls_public_key()
                        .zip(s.bls_proof_of_possession())
                        .map(|(pk, pop)| (s.node_id().0.clone(), pk, pop))
                })
                .collect();
            if !bls_keys.is_empty() {
                registry
                    .register_bls_keys(&KeysetVersion(1), bls_keys)
                    .unwrap();
            }
        }
        registry
    }

    // ---------------------------------------------------------------
    // checkpoint_hlc
    // ---------------------------------------------------------------

    #[test]
    fn checkpoint_hlc_floors_physical_and_clears_logical_and_node_id() {
        let exact = HlcTimestamp {
            physical: 5_000,
            logical: 3,
            node_id: "auth-1".into(),
        };
        let cp = checkpoint_hlc(&exact);
        assert_eq!(cp.physical, 5_000);
        assert_eq!(cp.logical, 0);
        assert_eq!(cp.node_id, "");

        let plus_one = HlcTimestamp {
            physical: 5_001,
            logical: 0,
            node_id: "auth-1".into(),
        };
        assert_eq!(checkpoint_hlc(&plus_one).physical, 5_000);

        let just_below = HlcTimestamp {
            physical: 5_999,
            logical: 42,
            node_id: "x".into(),
        };
        assert_eq!(checkpoint_hlc(&just_below).physical, 5_000);
    }

    // ---------------------------------------------------------------
    // Report message encoding
    // ---------------------------------------------------------------

    #[test]
    fn report_message_is_domain_separated() {
        let frontier = make_frontier("auth-1", 5_000, 0);
        let report_msg = create_frontier_report_message(&frontier);
        let cert_msg = create_certificate_message(
            &frontier.key_range,
            &frontier.frontier_hlc,
            &frontier.policy_version,
        );
        // The report message starts with the length-prefixed domain tag; the
        // certificate message starts with the length-prefixed key range prefix.
        assert_ne!(report_msg[..8], cert_msg[..8]);
        assert!(report_msg.len() > cert_msg.len());
    }

    #[test]
    fn report_message_changes_with_every_field() {
        let base = make_frontier("auth-1", 5_000, 0);
        let base_msg = create_frontier_report_message(&base);

        let mut f = base.clone();
        f.authority_id = node("auth-2");
        assert_ne!(create_frontier_report_message(&f), base_msg);

        let mut f = base.clone();
        f.frontier_hlc.physical += 1;
        assert_ne!(create_frontier_report_message(&f), base_msg);

        let mut f = base.clone();
        f.frontier_hlc.logical += 1;
        assert_ne!(create_frontier_report_message(&f), base_msg);

        let mut f = base.clone();
        f.key_range.prefix = "order/".into();
        assert_ne!(create_frontier_report_message(&f), base_msg);

        let mut f = base.clone();
        f.policy_version = PolicyVersion(2);
        assert_ne!(create_frontier_report_message(&f), base_msg);

        let mut f = base.clone();
        f.digest_hash = "tampered".into();
        assert_ne!(create_frontier_report_message(&f), base_msg);
    }

    // ---------------------------------------------------------------
    // Sign / verify roundtrip
    // ---------------------------------------------------------------

    #[test]
    fn sign_and_verify_roundtrip() {
        let signer = make_signer("auth-1", 1, false);
        let registry = registry_with(&[&signer]);
        let frontier = make_frontier("auth-1", 5_432, 7);

        let sig = signer.sign_frontier(&frontier, KeysetVersion(1));
        let att = verify_frontier_signature(&frontier, &sig, &registry, 0, &EpochConfig::default())
            .expect("valid signature must verify");

        assert_eq!(att.authority_id, node("auth-1"));
        assert_eq!(att.keyset_version, KeysetVersion(1));
        assert_eq!(att.checkpoint_hlc.physical, 5_000);
        assert_eq!(att.checkpoint_hlc.logical, 0);
        assert_eq!(att.ed25519.0, signer.verifying_key());
    }

    #[test]
    fn self_verified_matches_registry_verification() {
        let signer = make_signer("auth-1", 2, false);
        let registry = registry_with(&[&signer]);
        let frontier = make_frontier("auth-1", 9_876, 1);
        let sig = signer.sign_frontier(&frontier, KeysetVersion(1));

        let self_att = signer.self_verified(&frontier, &sig);
        let reg_att =
            verify_frontier_signature(&frontier, &sig, &registry, 0, &EpochConfig::default())
                .unwrap();

        assert_eq!(self_att.authority_id, reg_att.authority_id);
        assert_eq!(self_att.keyset_version, reg_att.keyset_version);
        assert_eq!(self_att.checkpoint_hlc, reg_att.checkpoint_hlc);
        assert_eq!(self_att.ed25519.0, reg_att.ed25519.0);
        assert_eq!(self_att.ed25519.1.to_bytes(), reg_att.ed25519.1.to_bytes());
    }

    // ---------------------------------------------------------------
    // Rejection cases
    // ---------------------------------------------------------------

    #[test]
    fn tampered_frontier_hlc_is_rejected() {
        let signer = make_signer("auth-1", 3, false);
        let registry = registry_with(&[&signer]);
        let frontier = make_frontier("auth-1", 5_000, 0);
        let sig = signer.sign_frontier(&frontier, KeysetVersion(1));

        // An attacker re-attaches the genuine signature to an inflated frontier.
        let mut forged = frontier.clone();
        forged.frontier_hlc.physical += 60_000;
        let result =
            verify_frontier_signature(&forged, &sig, &registry, 0, &EpochConfig::default());
        assert!(result.is_err(), "inflated frontier must be rejected");
    }

    #[test]
    fn tampered_digest_hash_is_rejected() {
        let signer = make_signer("auth-1", 4, false);
        let registry = registry_with(&[&signer]);
        let frontier = make_frontier("auth-1", 5_000, 0);
        let sig = signer.sign_frontier(&frontier, KeysetVersion(1));

        let mut forged = frontier.clone();
        forged.digest_hash = "forged-digest".into();
        let result =
            verify_frontier_signature(&forged, &sig, &registry, 0, &EpochConfig::default());
        assert!(result.is_err());
    }

    #[test]
    fn checkpoint_mismatch_is_rejected() {
        let signer = make_signer("auth-1", 5, false);
        let registry = registry_with(&[&signer]);
        let frontier = make_frontier("auth-1", 5_000, 0);
        let mut sig = signer.sign_frontier(&frontier, KeysetVersion(1));
        sig.checkpoint_hlc.physical += CHECKPOINT_INTERVAL_MS;

        let result =
            verify_frontier_signature(&frontier, &sig, &registry, 0, &EpochConfig::default());
        assert!(matches!(result, Err(CertError::InvalidSignature(_))));
    }

    #[test]
    fn embedded_key_mismatch_is_rejected() {
        let signer = make_signer("auth-1", 6, false);
        let other = make_signer("auth-1", 7, false);
        let registry = registry_with(&[&signer]);
        let frontier = make_frontier("auth-1", 5_000, 0);
        let mut sig = signer.sign_frontier(&frontier, KeysetVersion(1));
        sig.ed25519_public_key = hex::encode(other.verifying_key().as_bytes());

        let result =
            verify_frontier_signature(&frontier, &sig, &registry, 0, &EpochConfig::default());
        assert!(matches!(result, Err(CertError::InvalidSignature(_))));
    }

    #[test]
    fn impersonation_with_other_authoritys_signature_is_rejected() {
        // auth-2 signs its own frontier; an attacker presents the signature
        // under auth-1's identity.
        let signer1 = make_signer("auth-1", 8, false);
        let signer2 = make_signer("auth-2", 9, false);
        let registry = registry_with(&[&signer1, &signer2]);

        let frontier2 = make_frontier("auth-2", 5_000, 0);
        let sig2 = signer2.sign_frontier(&frontier2, KeysetVersion(1));

        let mut forged = frontier2.clone();
        forged.authority_id = node("auth-1");
        let result =
            verify_frontier_signature(&forged, &sig2, &registry, 0, &EpochConfig::default());
        assert!(result.is_err(), "impersonated attestation must be rejected");
    }

    #[test]
    fn unknown_keyset_is_rejected() {
        let signer = make_signer("auth-1", 10, false);
        let registry = registry_with(&[&signer]);
        let frontier = make_frontier("auth-1", 5_000, 0);
        let sig = signer.sign_frontier(&frontier, KeysetVersion(99));

        let result =
            verify_frontier_signature(&frontier, &sig, &registry, 0, &EpochConfig::default());
        assert!(matches!(result, Err(CertError::UnknownKeyset(99))));
    }

    #[test]
    fn expired_keyset_is_rejected_but_grace_is_accepted() {
        let signer = make_signer("auth-1", 11, false);
        let mut registry = registry_with(&[&signer]);
        // Register a newer keyset so version 1 is no longer current.
        let newer = make_signer("auth-1", 12, false);
        registry
            .register_keyset(
                KeysetVersion(2),
                5,
                vec![(node("auth-1"), newer.verifying_key())],
            )
            .unwrap();

        let config = EpochConfig {
            duration_secs: 86400,
            grace_epochs: 3,
        };
        let frontier = make_frontier("auth-1", 5_000, 0);
        let sig = signer.sign_frontier(&frontier, KeysetVersion(1));

        // Within grace (epoch 3 <= 0 + 3): accepted.
        let ok = verify_frontier_signature(&frontier, &sig, &registry, 3, &config);
        assert!(ok.is_ok(), "keyset within grace must be accepted");

        // Beyond grace (epoch 4 > 0 + 3): rejected.
        let result = verify_frontier_signature(&frontier, &sig, &registry, 4, &config);
        assert!(matches!(
            result,
            Err(CertError::ExpiredKeyset { version: 1, .. })
        ));
    }

    #[test]
    fn authority_not_in_registry_is_rejected() {
        let signer = make_signer("auth-1", 13, false);
        let other = make_signer("auth-2", 14, false);
        let registry = registry_with(&[&other]);
        let frontier = make_frontier("auth-1", 5_000, 0);
        let sig = signer.sign_frontier(&frontier, KeysetVersion(1));

        let result =
            verify_frontier_signature(&frontier, &sig, &registry, 0, &EpochConfig::default());
        assert!(matches!(result, Err(CertError::AuthorityNotInRegistry(_))));
    }

    // ---------------------------------------------------------------
    // BLS lane
    // ---------------------------------------------------------------

    #[cfg(feature = "native-crypto")]
    #[test]
    fn bls_roundtrip_produces_bls_attestation() {
        let signer = make_signer("auth-1", 20, true);
        let registry = registry_with(&[&signer]);
        let frontier = make_frontier("auth-1", 5_000, 0);
        let sig = signer.sign_frontier(&frontier, KeysetVersion(1));
        assert!(sig.bls_public_key.is_some());
        assert!(sig.bls_cert_signature.is_some());

        let att = verify_frontier_signature(&frontier, &sig, &registry, 0, &EpochConfig::default())
            .unwrap();
        assert!(
            att.bls.is_some(),
            "BLS attestation must survive verification"
        );
    }

    #[cfg(feature = "native-crypto")]
    #[test]
    fn bls_fields_without_registry_bls_key_degrade_to_ed25519() {
        // A BLS-seeded signer always attaches BLS fields, but the receiver's
        // registry only has the Ed25519 key (ASTEROIDB_AUTHORITY_KEYS with
        // the optional BLS part omitted). The frontier must still verify.
        let signer = make_signer("auth-1", 26, true);
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![(node("auth-1"), signer.verifying_key())],
            )
            .unwrap();

        let frontier = make_frontier("auth-1", 5_000, 0);
        let sig = signer.sign_frontier(&frontier, KeysetVersion(1));
        assert!(sig.bls_public_key.is_some());

        let att = verify_frontier_signature(&frontier, &sig, &registry, 0, &EpochConfig::default())
            .expect("Ed25519-only registry must accept BLS-signed frontiers");
        assert!(
            att.bls.is_none(),
            "attestation must degrade to Ed25519-only when the registry has no BLS key"
        );
    }

    #[cfg(feature = "native-crypto")]
    #[test]
    fn bls_registry_mismatch_is_rejected() {
        let signer = make_signer("auth-1", 21, true);
        let other = make_signer("auth-1", 22, true);
        let registry = registry_with(&[&signer]);
        let frontier = make_frontier("auth-1", 5_000, 0);
        let mut sig = signer.sign_frontier(&frontier, KeysetVersion(1));
        sig.bls_public_key = other.bls_public_key();

        let result =
            verify_frontier_signature(&frontier, &sig, &registry, 0, &EpochConfig::default());
        assert!(matches!(result, Err(CertError::InvalidSignature(_))));
    }

    #[cfg(feature = "native-crypto")]
    #[test]
    fn half_populated_bls_fields_are_rejected() {
        let signer = make_signer("auth-1", 23, true);
        let registry = registry_with(&[&signer]);
        let frontier = make_frontier("auth-1", 5_000, 0);
        let mut sig = signer.sign_frontier(&frontier, KeysetVersion(1));
        sig.bls_cert_signature = None;

        let result =
            verify_frontier_signature(&frontier, &sig, &registry, 0, &EpochConfig::default());
        assert!(matches!(result, Err(CertError::InvalidSignature(_))));
    }

    #[cfg(not(feature = "native-crypto"))]
    #[test]
    fn non_native_build_ignores_bls_fields() {
        let signer = make_signer("auth-1", 24, false);
        let registry = registry_with(&[&signer]);
        let frontier = make_frontier("auth-1", 5_000, 0);
        let mut sig = signer.sign_frontier(&frontier, KeysetVersion(1));
        // Simulate a native peer's signature carrying BLS fields.
        sig.bls_public_key = Some(BlsPublicKey("aa".repeat(48)));
        sig.bls_cert_signature = Some(BlsSignature("bb".repeat(96)));

        let att = verify_frontier_signature(&frontier, &sig, &registry, 0, &EpochConfig::default())
            .expect("Ed25519 portion must still verify");
        assert!(
            att.bls.is_none(),
            "non-native builds must ignore BLS fields"
        );
    }

    #[test]
    fn frontier_signature_serde_roundtrip() {
        let signer = make_signer("auth-1", 25, false);
        let frontier = make_frontier("auth-1", 5_000, 0);
        let sig = signer.sign_frontier(&frontier, KeysetVersion(1));

        let json = serde_json::to_string(&sig).unwrap();
        let back: FrontierSignature = serde_json::from_str(&json).unwrap();
        assert_eq!(back, sig);

        // Old-format JSON without the BLS fields still decodes.
        let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let obj = value.as_object_mut().unwrap();
        obj.remove("bls_public_key");
        obj.remove("bls_cert_signature");
        let back: FrontierSignature = serde_json::from_value(value).unwrap();
        assert!(back.bls_public_key.is_none());
    }
}
