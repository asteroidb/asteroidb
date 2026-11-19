//! Operation-level Merkle certificates, corresponding to `MerkleCertificate`
//! and `MerkleFlow` in aste-lean.
//!
//! Cryptographic verification proves an operation is included in roots signed
//! by a quorum. Honest Authority signing additionally depends on the durable
//! admission lock and the deployment's authentication/creation rules.

use std::collections::HashSet;

use ed25519_dalek::{Signature, SigningKey, Verifier};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::authority::certificate::{EpochConfig, KeysetRegistry, KeysetVersion};
use crate::authority::operation_admission::SignableCommitment;
use crate::operation::{
    BoundOperation, CertificationMode, CrdtSubject, OperationReceipt, ReceiptActionKind,
};
use crate::types::NodeId;

const LEAF_DOMAIN: &[u8] = b"asteroidb/operation-merkle-leaf/v1";
const BRANCH_DOMAIN: &[u8] = b"asteroidb/operation-merkle-branch/v1";
const ROOT_DOMAIN: &[u8] = b"asteroidb/operation-merkle-root/v1";

type Hash = [u8; 32];

fn hash_parts(domain: &[u8], parts: &[&[u8]]) -> Hash {
    let mut h = Sha256::new();
    h.update((domain.len() as u64).to_be_bytes());
    h.update(domain);
    for part in parts {
        h.update((part.len() as u64).to_be_bytes());
        h.update(part);
    }
    h.finalize().into()
}

fn leaf_hash(subject: &CrdtSubject, digest: &Hash) -> Hash {
    hash_parts(
        LEAF_DOMAIN,
        &[
            subject.namespace_id.as_bytes(),
            subject.event_dot.writer.0.as_bytes(),
            &subject.event_dot.counter.to_be_bytes(),
            digest,
        ],
    )
}

fn branch_hash(left: &Hash, right: &Hash) -> Hash {
    hash_parts(BRANCH_DOMAIN, &[left, right])
}

/// Binary tree in caller supplied order. The final leaf at an odd level is
/// duplicated; the proof contains that duplicate and verifies its equality.
#[derive(Debug, Clone)]
pub struct OperationMerkleTree {
    scope: OperationScope,
    subjects: Vec<CrdtSubject>,
    levels: Vec<Vec<Hash>>,
}

/// Trusted namespace and key prefix assigned to one Authority keyset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationScope {
    pub namespace_id: String,
    pub key_prefix: String,
}

impl OperationScope {
    pub fn contains(&self, operation: &BoundOperation) -> bool {
        operation.operation.subject.namespace_id == self.namespace_id
            && operation
                .operation
                .action
                .key()
                .starts_with(&self.key_prefix)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InclusionProof {
    pub leaf_index: u64,
    pub leaf_count: u64,
    pub siblings: Vec<Hash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedOperationRoot {
    pub root: Hash,
    pub leaf_count: u64,
    pub scope: OperationScope,
    pub keyset_version: KeysetVersion,
    pub authority_id: NodeId,
    /// Ed25519 signature over `message_bytes()`.
    #[serde(with = "signature_hex")]
    pub signature: [u8; 64],
}

mod signature_hex {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        signature: &[u8; 64],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(signature))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 64], D::Error> {
        let encoded = String::deserialize(deserializer)?;
        let bytes = hex::decode(encoded).map_err(serde::de::Error::custom)?;
        bytes
            .try_into()
            .map_err(|_| serde::de::Error::custom("expected 64 signature bytes"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationRootSupport {
    pub signed_root: SignedOperationRoot,
    pub proof: InclusionProof,
}

impl OperationRootSupport {
    /// Verify one Authority's support without treating it as a quorum
    /// certificate. Collection stores must call this before acknowledging a
    /// support and again during recovery.
    pub fn verify_individual(
        &self,
        operation: &BoundOperation,
        registry: &KeysetRegistry,
        trusted_scope: &OperationScope,
        current_epoch: u64,
        epoch_config: &EpochConfig,
    ) -> Result<(), OperationMerkleError> {
        operation
            .validate_structure()
            .map_err(|_| OperationMerkleError::InvalidOperation)?;
        if operation.declaration.mode != CertificationMode::Enabled {
            return Err(OperationMerkleError::CertificationDisabled);
        }
        if !trusted_scope.contains(operation) || &self.signed_root.scope != trusted_scope {
            return Err(OperationMerkleError::WrongScope);
        }
        let version = &self.signed_root.keyset_version;
        if !registry.is_version_valid(version, current_epoch, epoch_config) {
            return Err(OperationMerkleError::InvalidKeyset);
        }
        let keys = registry
            .get_keys(version)
            .ok_or(OperationMerkleError::InvalidKeyset)?;
        validate_distinct_authorities(keys)?;
        if self.proof.leaf_count != self.signed_root.leaf_count
            || !self.proof.verify(operation, &self.signed_root.root)
        {
            return Err(OperationMerkleError::InvalidProof);
        }
        let key = registry
            .get_key_for_authority(version, &self.signed_root.authority_id)
            .ok_or(OperationMerkleError::UnknownAuthority)?;
        key.verify(
            &self.signed_root.message_bytes(),
            &Signature::from_bytes(&self.signed_root.signature),
        )
        .map_err(|_| OperationMerkleError::InvalidSignature)
    }
}

/// One exact operation, with root-plus-inclusion evidence per Authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationMerkleCertificate {
    pub operation: BoundOperation,
    pub supports: Vec<OperationRootSupport>,
}

/// Certificate evidence that no longer stores an operation's value bytes.
/// The exact receipt must be obtained from a trusted durable operation ledger
/// before this evidence can be verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactOperationCertificate {
    pub subject: CrdtSubject,
    pub commitment: Hash,
    pub supports: Vec<OperationRootSupport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCompactOperation {
    certificate: CompactOperationCertificate,
}

impl VerifiedCompactOperation {
    pub fn certificate(&self) -> &CompactOperationCertificate {
        &self.certificate
    }
}

/// Exact operation supported by a verified quorum certificate. The private
/// field prevents callers from manufacturing certified state without proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedOperation {
    certificate: OperationMerkleCertificate,
}

impl VerifiedOperation {
    pub fn bound(&self) -> &BoundOperation {
        &self.certificate.operation
    }

    pub fn certificate(&self) -> &OperationMerkleCertificate {
        &self.certificate
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OperationMerkleError {
    #[error("empty operation tree or Authority keyset")]
    Empty,
    #[error("duplicate operation subject in one tree")]
    DuplicateSubject,
    #[error("invalid operation structure")]
    InvalidOperation,
    #[error("compact certificate does not match the durable operation receipt")]
    ReceiptMismatch,
    #[error("operation certification is disabled by creation policy")]
    CertificationDisabled,
    #[error("operation or signed root is outside trusted Authority scope")]
    WrongScope,
    #[error("leaf is absent from tree")]
    MissingLeaf,
    #[error("invalid Merkle inclusion proof")]
    InvalidProof,
    #[error("duplicate Authority ID or public key in certificate or registry")]
    DuplicateAuthority,
    #[error("unknown or expired keyset")]
    InvalidKeyset,
    #[error("Authority is absent from keyset")]
    UnknownAuthority,
    #[error("invalid Authority signature")]
    InvalidSignature,
    #[error("insufficient Authority signatures: {got}/{needed}")]
    InsufficientSignatures { got: usize, needed: usize },
    #[error("fault bound violates three-fault resilience")]
    InvalidFaultBound,
}

impl OperationMerkleTree {
    /// Only durably admitted operations can enter an Authority-issued tree.
    pub fn from_signable(
        scope: OperationScope,
        operations: &[SignableCommitment],
    ) -> Result<Self, OperationMerkleError> {
        if operations.is_empty() {
            return Err(OperationMerkleError::Empty);
        }
        let mut subjects = Vec::with_capacity(operations.len());
        let mut leaves = Vec::with_capacity(operations.len());
        let mut seen = HashSet::new();
        for op in operations {
            if !scope.contains(op.bound()) {
                return Err(OperationMerkleError::WrongScope);
            }
            if !seen.insert(op.subject().clone()) {
                return Err(OperationMerkleError::DuplicateSubject);
            }
            subjects.push(op.subject().clone());
            leaves.push(leaf_hash(op.subject(), op.digest()));
        }
        let mut levels = vec![leaves];
        while levels.last().expect("nonempty").len() > 1 {
            let prior = levels.last().expect("nonempty");
            let next = prior
                .chunks(2)
                .map(|pair| branch_hash(&pair[0], pair.get(1).unwrap_or(&pair[0])))
                .collect();
            levels.push(next);
        }
        Ok(Self {
            scope,
            subjects,
            levels,
        })
    }

    pub fn root(&self) -> Hash {
        self.levels.last().expect("nonempty")[0]
    }

    pub fn leaf_count(&self) -> u64 {
        self.subjects.len() as u64
    }

    pub fn proof(&self, subject: &CrdtSubject) -> Result<InclusionProof, OperationMerkleError> {
        let mut index = self
            .subjects
            .iter()
            .position(|candidate| candidate == subject)
            .ok_or(OperationMerkleError::MissingLeaf)?;
        let original = index as u64;
        let mut siblings = Vec::with_capacity(self.levels.len() - 1);
        for level in &self.levels[..self.levels.len() - 1] {
            siblings.push(level.get(index ^ 1).copied().unwrap_or(level[index]));
            index /= 2;
        }
        Ok(InclusionProof {
            leaf_index: original,
            leaf_count: self.leaf_count(),
            siblings,
        })
    }
}

impl InclusionProof {
    pub fn verify(&self, operation: &BoundOperation, root: &Hash) -> bool {
        operation.validate_structure().is_ok()
            && self.verify_commitment(&operation.operation.subject, &operation.commitment(), root)
    }

    /// Verify a previously authenticated operation receipt without retaining
    /// its value bytes. The caller must bind `subject` and `commitment` to a
    /// durable receipt and validate the signed roots independently; a Merkle
    /// path alone does not establish an operation certificate.
    pub fn verify_commitment(&self, subject: &CrdtSubject, commitment: &Hash, root: &Hash) -> bool {
        if self.leaf_count == 0 || self.leaf_index >= self.leaf_count {
            return false;
        }
        let mut width = self.leaf_count;
        let mut index = self.leaf_index;
        let mut value = leaf_hash(subject, commitment);
        let mut siblings = self.siblings.iter();
        while width > 1 {
            let Some(sibling) = siblings.next() else {
                return false;
            };
            // An odd last leaf is duplicated, so a forged sibling cannot
            // change its ancestor even if it hashes to the same root later.
            if index.is_multiple_of(2) {
                if index + 1 == width && sibling != &value {
                    return false;
                }
                value = branch_hash(&value, sibling);
            } else {
                value = branch_hash(sibling, &value);
            }
            index /= 2;
            width = width.div_ceil(2);
        }
        siblings.next().is_none() && &value == root
    }
}

impl SignedOperationRoot {
    fn unsigned(
        root: Hash,
        leaf_count: u64,
        scope: OperationScope,
        keyset_version: KeysetVersion,
        authority_id: NodeId,
    ) -> Self {
        Self {
            root,
            leaf_count,
            scope,
            keyset_version,
            authority_id,
            signature: [0; 64],
        }
    }

    /// Domain-separated canonical root message. No wall-clock value or
    /// serialization format participates in Ed25519 verification.
    pub fn message_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(ROOT_DOMAIN.len() as u64).to_be_bytes());
        out.extend_from_slice(ROOT_DOMAIN);
        out.extend_from_slice(&self.keyset_version.0.to_be_bytes());
        out.extend_from_slice(&self.leaf_count.to_be_bytes());
        out.extend_from_slice(&self.root);
        out.extend_from_slice(&(self.scope.namespace_id.len() as u64).to_be_bytes());
        out.extend_from_slice(self.scope.namespace_id.as_bytes());
        out.extend_from_slice(&(self.scope.key_prefix.len() as u64).to_be_bytes());
        out.extend_from_slice(self.scope.key_prefix.as_bytes());
        out.extend_from_slice(&(self.authority_id.0.len() as u64).to_be_bytes());
        out.extend_from_slice(self.authority_id.0.as_bytes());
        out
    }

    pub fn sign(
        tree: &OperationMerkleTree,
        keyset_version: KeysetVersion,
        authority_id: NodeId,
        signing_key: &SigningKey,
    ) -> Self {
        use ed25519_dalek::Signer;
        let mut signed = Self::unsigned(
            tree.root(),
            tree.leaf_count(),
            tree.scope.clone(),
            keyset_version,
            authority_id,
        );
        signed.signature = signing_key.sign(&signed.message_bytes()).to_bytes();
        signed
    }
}

impl OperationMerkleCertificate {
    pub fn compact(&self) -> CompactOperationCertificate {
        CompactOperationCertificate {
            subject: self.operation.operation.subject.clone(),
            commitment: self.operation.commitment(),
            supports: self.supports.clone(),
        }
    }

    /// Verify all supports, including excess ones. `faults` is the configured
    /// fault bound; Lean's uniqueness theorem requires `3f < n` and `n-f`
    /// distinct signers. Every signer must belong to the same keyset.
    pub fn verify(
        &self,
        registry: &KeysetRegistry,
        trusted_scope: &OperationScope,
        keyset_version: KeysetVersion,
        current_epoch: u64,
        epoch_config: &EpochConfig,
        faults: usize,
    ) -> Result<VerifiedOperation, OperationMerkleError> {
        self.operation
            .validate_structure()
            .map_err(|_| OperationMerkleError::InvalidOperation)?;
        if self.operation.declaration.mode != CertificationMode::Enabled {
            return Err(OperationMerkleError::CertificationDisabled);
        }
        if !trusted_scope.contains(&self.operation) {
            return Err(OperationMerkleError::WrongScope);
        }
        verify_supports(
            &self.supports,
            &self.operation.operation.subject,
            &self.operation.commitment(),
            registry,
            trusted_scope,
            keyset_version,
            current_epoch,
            epoch_config,
            faults,
        )?;
        Ok(VerifiedOperation {
            certificate: self.clone(),
        })
    }
}

impl CompactOperationCertificate {
    /// Recheck a compact proof against an authenticated durable receipt. The
    /// receipt is the binding to the former full operation; a caller-supplied
    /// untrusted receipt must never be treated as enough to certify a value.
    #[allow(clippy::too_many_arguments)]
    pub fn verify(
        &self,
        receipt: &OperationReceipt,
        registry: &KeysetRegistry,
        trusted_scope: &OperationScope,
        keyset_version: KeysetVersion,
        current_epoch: u64,
        epoch_config: &EpochConfig,
        faults: usize,
    ) -> Result<VerifiedCompactOperation, OperationMerkleError> {
        if self.subject != receipt.subject || self.commitment != receipt.commitment {
            return Err(OperationMerkleError::ReceiptMismatch);
        }
        if receipt.subject.namespace_id != receipt.declaration.creation_id.namespace_id {
            return Err(OperationMerkleError::InvalidOperation);
        }
        if receipt.action_kind == ReceiptActionKind::Create
            && receipt.subject != receipt.declaration.creation_id
        {
            return Err(OperationMerkleError::InvalidOperation);
        }
        if receipt.declaration.mode != CertificationMode::Enabled {
            return Err(OperationMerkleError::CertificationDisabled);
        }
        if receipt.subject.namespace_id != trusted_scope.namespace_id
            || !receipt
                .declaration
                .key
                .starts_with(&trusted_scope.key_prefix)
        {
            return Err(OperationMerkleError::WrongScope);
        }
        verify_supports(
            &self.supports,
            &self.subject,
            &self.commitment,
            registry,
            trusted_scope,
            keyset_version,
            current_epoch,
            epoch_config,
            faults,
        )?;
        Ok(VerifiedCompactOperation {
            certificate: self.clone(),
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn verify_supports(
    supports: &[OperationRootSupport],
    subject: &CrdtSubject,
    commitment: &Hash,
    registry: &KeysetRegistry,
    trusted_scope: &OperationScope,
    keyset_version: KeysetVersion,
    current_epoch: u64,
    epoch_config: &EpochConfig,
    faults: usize,
) -> Result<(), OperationMerkleError> {
    let keys = registry
        .get_keys(&keyset_version)
        .ok_or(OperationMerkleError::InvalidKeyset)?;
    let n = keys.len();
    if n == 0 {
        return Err(OperationMerkleError::Empty);
    }
    if faults.checked_mul(3).is_none_or(|triple| triple >= n) {
        return Err(OperationMerkleError::InvalidFaultBound);
    }
    if !registry.is_version_valid(&keyset_version, current_epoch, epoch_config) {
        return Err(OperationMerkleError::InvalidKeyset);
    }
    validate_distinct_authorities(keys)?;
    let mut signers = HashSet::new();
    for support in supports {
        let signed = &support.signed_root;
        if &signed.scope != trusted_scope {
            return Err(OperationMerkleError::WrongScope);
        }
        if signed.keyset_version != keyset_version {
            return Err(OperationMerkleError::InvalidKeyset);
        }
        if !signers.insert(&signed.authority_id) {
            return Err(OperationMerkleError::DuplicateAuthority);
        }
        if support.proof.leaf_count != signed.leaf_count
            || !support
                .proof
                .verify_commitment(subject, commitment, &signed.root)
        {
            return Err(OperationMerkleError::InvalidProof);
        }
        let key = registry
            .get_key_for_authority(&keyset_version, &signed.authority_id)
            .ok_or(OperationMerkleError::UnknownAuthority)?;
        key.verify(
            &signed.message_bytes(),
            &Signature::from_bytes(&signed.signature),
        )
        .map_err(|_| OperationMerkleError::InvalidSignature)?;
    }
    let needed = n - faults;
    if signers.len() < needed {
        return Err(OperationMerkleError::InsufficientSignatures {
            got: signers.len(),
            needed,
        });
    }
    Ok(())
}

/// Separate Authority IDs must have separate signing keys for the fault
/// bound to count them as independent signers.
fn validate_distinct_authorities(
    keys: &[(NodeId, ed25519_dalek::VerifyingKey)],
) -> Result<(), OperationMerkleError> {
    let mut ids = HashSet::new();
    let mut public_keys = HashSet::new();
    for (id, key) in keys {
        if !ids.insert(id) || !public_keys.insert(key.to_bytes()) {
            return Err(OperationMerkleError::DuplicateAuthority);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::frontier_sig::NodeSigner;
    use crate::authority::operation_admission::{
        OperationAdmissionPolicy, OperationLockStore, admit_for_signing,
    };
    use crate::hlc::HlcTimestamp;
    use crate::operation::{
        CandidateKvAction, CertificationMode, Declaration, EventDot, OperationHistory,
        OperationLeaf, RecordOutcome,
    };

    struct Allow;
    fn scope() -> OperationScope {
        OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "key-".into(),
        }
    }
    impl OperationAdmissionPolicy for Allow {
        fn authorize(&self, _: &CrdtSubject, _: &CandidateKvAction) -> bool {
            true
        }
        fn event_allowed(&self, _: &CrdtSubject) -> bool {
            true
        }
        fn key_in_range(&self, _: &str, _: &str) -> bool {
            true
        }
        fn creation_binding_valid(&self, _: &BoundOperation) -> bool {
            true
        }
    }

    #[derive(Default)]
    struct Lock(OperationHistory);
    impl OperationLockStore for Lock {
        type Error = crate::operation::OperationError;
        fn record_and_sync(&mut self, bound: BoundOperation) -> Result<RecordOutcome, Self::Error> {
            self.0.record(bound)
        }
    }

    fn operation(id: u64) -> BoundOperation {
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: id,
            },
        };
        BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: format!("key-{id}"),
                    value_bytes: format!("value-{id}").into_bytes(),
                    timestamp: HlcTimestamp {
                        physical: 10,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject,
                key: format!("key-{id}"),
                mode: CertificationMode::Enabled,
            },
        }
    }

    fn fixture() -> (OperationMerkleCertificate, KeysetRegistry, Vec<SigningKey>) {
        let mut lock = Lock::default();
        let bound = operation(1);
        let target = admit_for_signing(&Allow, &mut lock, bound.clone()).unwrap();
        let other = admit_for_signing(&Allow, &mut lock, operation(2)).unwrap();
        let third = admit_for_signing(&Allow, &mut lock, operation(3)).unwrap();
        let mut registry = KeysetRegistry::new();
        let keys: Vec<_> = (0..4)
            .map(|i| SigningKey::from_bytes(&[i as u8 + 1; 32]))
            .collect();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                keys.iter()
                    .enumerate()
                    .map(|(i, key)| (NodeId(format!("auth-{i}")), key.verifying_key()))
                    .collect(),
            )
            .unwrap();
        // Each Authority may sign a different root, while all three support
        // the same target operation through their own inclusion proofs.
        let trees = [
            OperationMerkleTree::from_signable(scope(), &[target.clone(), other.clone()]).unwrap(),
            OperationMerkleTree::from_signable(scope(), &[third, target.clone(), other]).unwrap(),
            OperationMerkleTree::from_signable(scope(), &[target]).unwrap(),
        ];
        let supports = trees
            .iter()
            .enumerate()
            .map(|(i, tree)| OperationRootSupport {
                signed_root: SignedOperationRoot::sign(
                    tree,
                    KeysetVersion(1),
                    NodeId(format!("auth-{i}")),
                    &keys[i],
                ),
                proof: tree.proof(&bound.operation.subject).unwrap(),
            })
            .collect();
        (
            OperationMerkleCertificate {
                operation: bound,
                supports,
            },
            registry,
            keys,
        )
    }

    #[test]
    fn one_public_key_cannot_count_as_multiple_authorities() {
        let (_, _, keys) = fixture();
        let mut registry = KeysetRegistry::new();
        assert!(matches!(
            registry.register_keyset(
                KeysetVersion(1),
                0,
                (0..4)
                    .map(|i| (NodeId(format!("auth-{i}")), keys[0].verifying_key()))
                    .collect(),
            ),
            Err(crate::authority::certificate::CertError::DuplicateAuthorityKey(_))
        ));
        assert!(registry.get_keys(&KeysetVersion(1)).is_none());
    }

    #[test]
    fn receipt_commitment_verifies_the_same_leaf_without_value_bytes() {
        let (certificate, _, _) = fixture();
        let operation = &certificate.operation;
        let receipt = crate::operation::OperationReceipt::from(operation);
        for support in &certificate.supports {
            let proof = &support.proof;
            let root = &support.signed_root.root;
            assert!(proof.verify(operation, root));
            assert!(proof.verify_commitment(&receipt.subject, &receipt.commitment, root));
            let mut wrong_subject = receipt.subject.clone();
            wrong_subject.event_dot.counter += 1;
            assert!(!proof.verify_commitment(&wrong_subject, &receipt.commitment, root));
            let mut wrong_commitment = receipt.commitment;
            wrong_commitment[0] ^= 1;
            assert!(!proof.verify_commitment(&receipt.subject, &wrong_commitment, root));
        }
    }

    #[test]
    fn compact_certificate_requires_exact_receipt_and_full_quorum() {
        let (certificate, registry, _) = fixture();
        let receipt = OperationReceipt::from(&certificate.operation);
        let compact = certificate.compact();
        let serialized = serde_json::to_vec(&compact).unwrap();
        assert!(
            !serialized
                .windows(b"value-1".len())
                .any(|part| part == b"value-1")
        );
        assert!(serde_json::from_slice::<CompactOperationCertificate>(&serialized).is_ok());
        let check = |candidate: &CompactOperationCertificate, receipt: &OperationReceipt| {
            candidate.verify(
                receipt,
                &registry,
                &scope(),
                KeysetVersion(1),
                0,
                &EpochConfig::default(),
                1,
            )
        };
        assert_eq!(check(&compact, &receipt).unwrap().certificate(), &compact);
        let mut wrong_receipt = receipt.clone();
        wrong_receipt.commitment[0] ^= 1;
        assert_eq!(
            check(&compact, &wrong_receipt).unwrap_err(),
            OperationMerkleError::ReceiptMismatch
        );
        let mut wrong_scope_receipt = receipt.clone();
        wrong_scope_receipt.declaration.key = "elsewhere".into();
        assert_eq!(
            check(&compact, &wrong_scope_receipt).unwrap_err(),
            OperationMerkleError::WrongScope
        );
        let mut insufficient = compact.clone();
        insufficient.supports.pop();
        assert!(matches!(
            check(&insufficient, &receipt),
            Err(OperationMerkleError::InsufficientSignatures { .. })
        ));
        let mut forged = compact.clone();
        forged.supports[0].signed_root.signature[0] ^= 1;
        assert_eq!(
            check(&forged, &receipt).unwrap_err(),
            OperationMerkleError::InvalidSignature
        );
        let mut duplicate = compact.clone();
        duplicate.supports[1] = duplicate.supports[0].clone();
        assert_eq!(
            check(&duplicate, &receipt).unwrap_err(),
            OperationMerkleError::DuplicateAuthority
        );
    }

    fn verify(
        cert: &OperationMerkleCertificate,
        registry: &KeysetRegistry,
    ) -> Result<VerifiedOperation, OperationMerkleError> {
        cert.verify(
            registry,
            &scope(),
            KeysetVersion(1),
            0,
            &EpochConfig::default(),
            1,
        )
    }

    #[test]
    fn node_signer_uses_its_own_identity_and_existing_root_message() {
        let mut lock = Lock::default();
        let bound = operation(1);
        let token = admit_for_signing(&Allow, &mut lock, bound.clone()).unwrap();
        let tree = OperationMerkleTree::from_signable(scope(), &[token]).unwrap();
        let seed = [9; 32];
        #[cfg(feature = "native-crypto")]
        let signer = NodeSigner::from_seed(NodeId("authority".into()), &seed, false);
        #[cfg(not(feature = "native-crypto"))]
        let signer = NodeSigner::from_seed(NodeId("authority".into()), &seed);
        let signed = signer.sign_operation_root(&tree, KeysetVersion(7));
        assert_eq!(signed.authority_id, *signer.node_id());
        assert_eq!(signed.keyset_version, KeysetVersion(7));
        assert_eq!(signed.root, tree.root());
        assert_eq!(signed.leaf_count, tree.leaf_count());
        assert_eq!(
            signed,
            SignedOperationRoot::sign(
                &tree,
                KeysetVersion(7),
                signer.node_id().clone(),
                &SigningKey::from_bytes(&seed),
            )
        );

        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(7),
                0,
                vec![(signer.node_id().clone(), signer.verifying_key())],
            )
            .unwrap();
        let certificate = OperationMerkleCertificate {
            operation: bound,
            supports: vec![OperationRootSupport {
                signed_root: signed,
                proof: tree.proof(&operation(1).operation.subject).unwrap(),
            }],
        };
        certificate
            .verify(
                &registry,
                &scope(),
                KeysetVersion(7),
                0,
                &EpochConfig::default(),
                0,
            )
            .unwrap();
    }

    #[test]
    fn different_signed_roots_prove_one_exact_operation() {
        let (cert, registry, _) = fixture();
        assert_ne!(
            cert.supports[0].signed_root.root,
            cert.supports[1].signed_root.root
        );
        assert_eq!(verify(&cert, &registry).unwrap().bound(), &cert.operation);
        let json = serde_json::to_string(&cert).unwrap();
        let recovered: OperationMerkleCertificate = serde_json::from_str(&json).unwrap();
        assert_eq!(
            verify(&recovered, &registry).unwrap().bound(),
            &cert.operation
        );
    }

    #[test]
    fn operation_id_content_policy_and_inclusion_must_match() {
        let (cert, registry, _) = fixture();
        let mut changed = cert.clone();
        changed.operation.operation.subject.event_dot.counter += 1;
        changed.operation.declaration.creation_id = changed.operation.operation.subject.clone();
        assert_eq!(
            verify(&changed, &registry),
            Err(OperationMerkleError::InvalidProof)
        );
        let mut changed = cert.clone();
        if let CandidateKvAction::Create { value_bytes, .. } =
            &mut changed.operation.operation.action
        {
            value_bytes.push(42);
        }
        assert_eq!(
            verify(&changed, &registry),
            Err(OperationMerkleError::InvalidProof)
        );
        let mut changed = cert.clone();
        changed.operation.declaration.mode = CertificationMode::Disabled;
        assert_eq!(
            verify(&changed, &registry),
            Err(OperationMerkleError::CertificationDisabled)
        );
        let mut changed = cert.clone();
        changed.supports[0].proof.siblings[0][0] ^= 1;
        assert_eq!(
            verify(&changed, &registry),
            Err(OperationMerkleError::InvalidProof)
        );
        let mut changed = cert.clone();
        changed.supports[1].proof.leaf_index = 0;
        assert_eq!(
            verify(&changed, &registry),
            Err(OperationMerkleError::InvalidProof)
        );
    }

    #[test]
    fn duplicate_or_missing_signers_and_wrong_keyset_fail() {
        let (cert, registry, _) = fixture();
        let mut short = cert.clone();
        short.supports.pop();
        assert_eq!(
            verify(&short, &registry),
            Err(OperationMerkleError::InsufficientSignatures { got: 2, needed: 3 })
        );
        let mut duplicate = cert.clone();
        duplicate.supports[2] = duplicate.supports[0].clone();
        assert_eq!(
            verify(&duplicate, &registry),
            Err(OperationMerkleError::DuplicateAuthority)
        );
        let mut wrong_keyset = cert.clone();
        wrong_keyset.supports[0].signed_root.keyset_version = KeysetVersion(2);
        assert_eq!(
            verify(&wrong_keyset, &registry),
            Err(OperationMerkleError::InvalidKeyset)
        );
        let mut wrong_signer = cert.clone();
        wrong_signer.supports[0].signed_root.authority_id = NodeId("unregistered".into());
        assert_eq!(
            verify(&wrong_signer, &registry),
            Err(OperationMerkleError::UnknownAuthority)
        );
        let mut wrong_signature = cert.clone();
        wrong_signature.supports[0].signed_root.signature[0] ^= 1;
        assert_eq!(
            verify(&wrong_signature, &registry),
            Err(OperationMerkleError::InvalidSignature)
        );
        assert_eq!(
            cert.verify(
                &registry,
                &scope(),
                KeysetVersion(1),
                0,
                &EpochConfig::default(),
                2
            ),
            Err(OperationMerkleError::InvalidFaultBound)
        );
    }

    #[test]
    fn trusted_scope_is_part_of_every_root_signature() {
        let (cert, registry, _) = fixture();
        let wrong_scope = OperationScope {
            namespace_id: "other".into(),
            key_prefix: "key-".into(),
        };
        assert_eq!(
            cert.verify(
                &registry,
                &wrong_scope,
                KeysetVersion(1),
                0,
                &EpochConfig::default(),
                1
            ),
            Err(OperationMerkleError::WrongScope)
        );
        let wrong_range = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "other-".into(),
        };
        assert_eq!(
            cert.verify(
                &registry,
                &wrong_range,
                KeysetVersion(1),
                0,
                &EpochConfig::default(),
                1
            ),
            Err(OperationMerkleError::WrongScope)
        );
        let mut forged = cert.clone();
        forged.supports[0].signed_root.scope.key_prefix.clear();
        assert_eq!(
            verify(&forged, &registry),
            Err(OperationMerkleError::WrongScope)
        );
    }

    #[test]
    fn tree_rejects_duplicate_subjects_and_proof_shape_changes() {
        let mut lock = Lock::default();
        let op = admit_for_signing(&Allow, &mut lock, operation(1)).unwrap();
        assert!(matches!(
            OperationMerkleTree::from_signable(scope(), &[op.clone(), op.clone()]),
            Err(OperationMerkleError::DuplicateSubject)
        ));
        let tree = OperationMerkleTree::from_signable(scope(), std::slice::from_ref(&op)).unwrap();
        let proof = tree.proof(op.subject()).unwrap();
        assert!(proof.verify(op.bound(), &tree.root()));
        let mut extra = proof.clone();
        extra.siblings.push([0; 32]);
        assert!(!extra.verify(op.bound(), &tree.root()));
        assert_eq!(
            tree.proof(&operation(2).operation.subject),
            Err(OperationMerkleError::MissingLeaf)
        );
    }
}
