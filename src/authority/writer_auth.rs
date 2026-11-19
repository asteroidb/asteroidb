//! Ed25519 writer and creator authentication for exact bound operations.
//!
//! The trust registry is a deployment input independent of Authority quorum
//! keys. Its entries must be authenticated and consistently distributed.
//! Local conflict checks cannot exclude a create hidden on another partition.

use std::collections::{BTreeMap, BTreeSet};

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::operation::{
    BoundOperation, CandidateKvAction, CrdtSubject, Declaration, OperationError, OperationLedger,
};
use crate::types::NodeId;

const WRITER_DOMAIN: &[u8] = b"asteroidb/writer-operation-signature/v2";
const COMPACT_CREATE_DOMAIN: &[u8] = b"asteroidb/creator-creation-proof/v3";

fn message(domain: &[u8], bound: &BoundOperation) -> Vec<u8> {
    let bytes = bound.canonical_bytes();
    let mut out = Vec::with_capacity(16 + domain.len() + bytes.len());
    out.extend_from_slice(&(domain.len() as u64).to_be_bytes());
    out.extend_from_slice(domain);
    out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    out.extend_from_slice(&bytes);
    out
}

fn operation_message(bound: &BoundOperation, creation: &SignedCreation) -> Vec<u8> {
    let mut out = message(WRITER_DOMAIN, bound);
    let creation_message = compact_message(creation);
    put_bytes(&mut out, &creation_message);
    out.extend_from_slice(&creation.signature);
    out
}

mod signature_hex {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 64], D::Error> {
        let encoded = String::deserialize(deserializer)?;
        hex::decode(encoded)
            .map_err(serde::de::Error::custom)?
            .try_into()
            .map_err(|_| serde::de::Error::custom("expected 64 signature bytes"))
    }
}

/// Creator-authenticated Create metadata. The canonical Create commitment
/// covers subject, declaration, key, value and HLC without carrying the value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedCreation {
    pub version: u32,
    pub declaration: Declaration,
    pub create_commitment: [u8; 32],
    /// Digest of the immutable creation policy shared by every receiver.
    pub creation_policy_digest: [u8; 32],
    #[serde(with = "signature_hex")]
    pub signature: [u8; 64],
}

pub type CompactSignedCreation = SignedCreation;

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn compact_message(proof: &SignedCreation) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, COMPACT_CREATE_DOMAIN);
    put_bytes(
        &mut out,
        proof.declaration.creation_id.namespace_id.as_bytes(),
    );
    put_bytes(
        &mut out,
        proof.declaration.creation_id.event_dot.writer.0.as_bytes(),
    );
    out.extend_from_slice(
        &proof
            .declaration
            .creation_id
            .event_dot
            .counter
            .to_be_bytes(),
    );
    put_bytes(&mut out, proof.declaration.key.as_bytes());
    out.push(match proof.declaration.mode {
        crate::operation::CertificationMode::Enabled => 1,
        crate::operation::CertificationMode::Disabled => 2,
    });
    out.extend_from_slice(&proof.create_commitment);
    out.extend_from_slice(&proof.creation_policy_digest);
    out
}

impl SignedCreation {
    pub fn sign_with_policy_digest(
        bound: &BoundOperation,
        key: &SigningKey,
        creation_policy_digest: [u8; 32],
    ) -> Result<Self, WriterAuthError> {
        validate_creation(bound)?;
        let mut proof = Self {
            version: 3,
            declaration: bound.declaration.clone(),
            create_commitment: bound.commitment(),
            creation_policy_digest,
            signature: [0; 64],
        };
        proof.signature = key.sign(&compact_message(&proof)).to_bytes();
        Ok(proof)
    }

    #[cfg(test)]
    pub fn sign(bound: &BoundOperation, key: &SigningKey) -> Result<Self, WriterAuthError> {
        Self::sign_with_policy_digest(bound, key, [0; 32])
    }

    pub fn declaration(&self) -> &Declaration {
        &self.declaration
    }

    pub fn matches_create(&self, bound: &BoundOperation) -> bool {
        matches!(&bound.operation.action, CandidateKvAction::Create { .. }
            if self.version == 3 && self.declaration == bound.declaration
                && self.create_commitment == bound.commitment())
    }

    /// Reverify the creator's value-free declaration after the complete
    /// Create was authenticated and replaced by a durable policy marker.
    pub fn verify(&self, registry: &WriterRegistry) -> Result<(), WriterAuthError> {
        if self.version != 3
            || self.declaration.key.is_empty()
            || self.declaration.creation_id.namespace_id.is_empty()
            || self.declaration.creation_id.event_dot.writer.0.is_empty()
        {
            return Err(WriterAuthError::InvalidCreationSignature);
        }
        let creator = &self.declaration.creation_id.event_dot.writer;
        let key = registry.key_for(creator, &self.declaration.creation_id.namespace_id)?;
        key.verify(
            &compact_message(self),
            &Signature::from_bytes(&self.signature),
        )
        .map_err(|_| WriterAuthError::InvalidCreationSignature)
    }
}

/// Signature of the event writer over the entire operation plus a signed
/// creation whose declaration is referenced by this operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedBoundOperation {
    pub bound: BoundOperation,
    #[serde(with = "signature_hex")]
    pub operation_signature: [u8; 64],
    pub creation: SignedCreation,
}

impl SignedBoundOperation {
    pub fn sign(
        bound: BoundOperation,
        writer_key: &SigningKey,
        creation: SignedCreation,
    ) -> Result<Self, WriterAuthError> {
        bound
            .validate_structure()
            .map_err(WriterAuthError::Structure)?;
        if bound.declaration != *creation.declaration() {
            return Err(WriterAuthError::DeclarationMismatch);
        }
        if matches!(bound.operation.action, CandidateKvAction::Create { .. })
            && !creation.matches_create(&bound)
        {
            return Err(WriterAuthError::CreatePayloadMismatch);
        }
        Ok(Self {
            operation_signature: writer_key
                .sign(&operation_message(&bound, &creation))
                .to_bytes(),
            bound,
            creation,
        })
    }

    pub fn verify(&self, registry: &WriterRegistry) -> Result<(), WriterAuthError> {
        self.bound
            .validate_structure()
            .map_err(WriterAuthError::Structure)?;
        if self.bound.declaration != *self.creation.declaration() {
            return Err(WriterAuthError::DeclarationMismatch);
        }
        if matches!(
            self.bound.operation.action,
            CandidateKvAction::Create { .. }
        ) && !self.creation.matches_create(&self.bound)
        {
            return Err(WriterAuthError::CreatePayloadMismatch);
        }
        self.creation.verify(registry)?;
        let writer = &self.bound.operation.subject.event_dot.writer;
        let writer_key = registry.key_for(writer, &self.bound.operation.subject.namespace_id)?;
        writer_key
            .verify(
                &operation_message(&self.bound, &self.creation),
                &Signature::from_bytes(&self.operation_signature),
            )
            .map_err(|_| WriterAuthError::InvalidOperationSignature)
    }
}

fn validate_creation(bound: &BoundOperation) -> Result<(), WriterAuthError> {
    bound
        .validate_structure()
        .map_err(WriterAuthError::Structure)?;
    if !matches!(bound.operation.action, CandidateKvAction::Create { .. }) {
        return Err(WriterAuthError::NotCreation);
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct WriterEntry {
    key: VerifyingKey,
    namespaces: BTreeSet<String>,
}

/// Trusted writer keys and explicit namespace grants. The caller owns the
/// lifecycle and must reject stale or conflicting registrations.
#[derive(Debug, Default, Clone)]
pub struct WriterRegistry {
    entries: BTreeMap<NodeId, WriterEntry>,
}

impl WriterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stable identity of the trusted writer keys and namespace grants.
    /// Authority directories pin this before admitting signed operations.
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"asteroidb/writer-registry/v1");
        hash.update((self.entries.len() as u64).to_be_bytes());
        for (id, entry) in &self.entries {
            hash.update((id.0.len() as u64).to_be_bytes());
            hash.update(id.0.as_bytes());
            hash.update(entry.key.as_bytes());
            hash.update((entry.namespaces.len() as u64).to_be_bytes());
            for namespace in &entry.namespaces {
                hash.update((namespace.len() as u64).to_be_bytes());
                hash.update(namespace.as_bytes());
            }
        }
        hash.finalize().into()
    }

    /// Parse a trusted JSON key manifest, for example from
    /// `ASTEROIDB_WRITER_KEYS`. An empty manifest cannot enable admission.
    pub fn from_json(json: &str) -> Result<Self, WriterAuthError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Registration {
            writer_id: String,
            public_key_hex: String,
            namespaces: Vec<String>,
        }

        let registrations: Vec<Registration> = serde_json::from_str(json)
            .map_err(|e| WriterAuthError::InvalidManifest(e.to_string()))?;
        if registrations.is_empty() {
            return Err(WriterAuthError::EmptyManifest);
        }
        let mut registry = Self::new();
        for row in registrations {
            if row.writer_id.is_empty() {
                return Err(WriterAuthError::InvalidManifest("empty writer ID".into()));
            }
            let bytes = hex::decode(&row.public_key_hex)
                .map_err(|e| WriterAuthError::InvalidManifest(e.to_string()))?;
            let bytes: [u8; 32] = bytes
                .try_into()
                .map_err(|_| WriterAuthError::InvalidManifest("expected 32 key bytes".into()))?;
            let key = VerifyingKey::from_bytes(&bytes)
                .map_err(|e| WriterAuthError::InvalidManifest(e.to_string()))?;
            if row.namespaces.iter().any(String::is_empty) {
                return Err(WriterAuthError::InvalidManifest("empty namespace".into()));
            }
            let namespaces = row.namespaces.into_iter().collect();
            let writer = NodeId(row.writer_id);
            if registry.entries.contains_key(&writer) {
                return Err(WriterAuthError::ConflictingRegistration(writer));
            }
            registry.register(writer, key, namespaces)?;
        }
        Ok(registry)
    }

    /// Register once. Reusing an identity with another key or grants is
    /// rejected so an in-memory refresh cannot silently rewrite trust.
    pub fn register(
        &mut self,
        writer: NodeId,
        key: VerifyingKey,
        namespaces: BTreeSet<String>,
    ) -> Result<(), WriterAuthError> {
        if namespaces.is_empty() {
            return Err(WriterAuthError::NoNamespaces);
        }
        if let Some(existing) = self.entries.get(&writer) {
            if existing.key == key && existing.namespaces == namespaces {
                return Ok(());
            }
            return Err(WriterAuthError::ConflictingRegistration(writer));
        }
        self.entries.insert(writer, WriterEntry { key, namespaces });
        Ok(())
    }

    fn key_for(&self, writer: &NodeId, namespace: &str) -> Result<&VerifyingKey, WriterAuthError> {
        let entry = self
            .entries
            .get(writer)
            .ok_or_else(|| WriterAuthError::UnknownWriter(writer.clone()))?;
        if !entry.namespaces.contains(namespace) {
            return Err(WriterAuthError::NamespaceDenied(writer.clone()));
        }
        Ok(&entry.key)
    }
}

/// Check all creations retained by this Authority before recording a new
/// candidate. This is local evidence, not a distributed uniqueness proof.
pub fn check_local_create_conflict(
    ledger: &OperationLedger,
    bound: &BoundOperation,
) -> Result<(), WriterAuthError> {
    let target = (
        &bound.operation.subject.namespace_id,
        bound.operation.action.key(),
    );
    let declaration: &Declaration = &bound.declaration;
    for existing in ledger.iter() {
        let existing_target = (
            &existing.operation.subject.namespace_id,
            existing.operation.action.key(),
        );
        if existing_target != target {
            continue;
        }
        if existing.declaration != *declaration {
            return Err(WriterAuthError::CompetingCreation(
                existing.declaration.creation_id.clone(),
            ));
        }
        if matches!(bound.operation.action, CandidateKvAction::Create { .. })
            && matches!(existing.operation.action, CandidateKvAction::Create { .. })
            && existing != bound
        {
            return Err(WriterAuthError::CompetingCreation(
                existing.operation.subject.clone(),
            ));
        }
    }
    for receipt in ledger.receipts() {
        if receipt.subject.namespace_id == *target.0
            && receipt.declaration.key == *target.1
            && receipt.declaration != *declaration
        {
            return Err(WriterAuthError::CompetingCreation(
                receipt.declaration.creation_id.clone(),
            ));
        }
    }
    Ok(())
}

/// A compact proof on a follow-up carries only a Create commitment. Before
/// an Authority signs it, its own durable ledger must witness that exact
/// authenticated Create. A retained receipt is sufficient after compaction.
/// The caller must have verified the signed Create before adding it to this
/// ledger; this check does not authenticate an arbitrary ledger insertion.
pub fn check_compact_creation_witness(
    ledger: &OperationLedger,
    signed: &SignedBoundOperation,
) -> Result<(), WriterAuthError> {
    let proof = &signed.creation;
    if matches!(
        signed.bound.operation.action,
        CandidateKvAction::Create { .. }
    ) {
        return Ok(());
    }
    let creation_id = &proof.declaration.creation_id;
    let witnessed = if let Some(bound) = ledger.get(creation_id) {
        matches!(bound.operation.action, CandidateKvAction::Create { .. })
            && bound.declaration == proof.declaration
            && bound.commitment() == proof.create_commitment
    } else if let Some(receipt) = ledger.receipt(creation_id) {
        receipt.action_kind == crate::operation::ReceiptActionKind::Create
            && receipt.declaration == proof.declaration
            && receipt.commitment == proof.create_commitment
    } else {
        false
    };
    if witnessed {
        Ok(())
    } else {
        Err(WriterAuthError::MissingCreationWitness)
    }
}

#[derive(Debug, Error)]
pub enum WriterAuthError {
    #[error("invalid writer key manifest: {0}")]
    InvalidManifest(String),
    #[error("writer key manifest is empty")]
    EmptyManifest,
    #[error("invalid operation structure: {0:?}")]
    Structure(OperationError),
    #[error("compact follow-up has no matching durable Create witness")]
    MissingCreationWitness,
    #[error("creation evidence is not a create action")]
    NotCreation,
    #[error("operation declaration differs from signed creation")]
    DeclarationMismatch,
    #[error("create operation differs from signed creation payload")]
    CreatePayloadMismatch,
    #[error("invalid creator signature")]
    InvalidCreationSignature,
    #[error("invalid writer signature")]
    InvalidOperationSignature,
    #[error("unknown writer: {0:?}")]
    UnknownWriter(NodeId),
    #[error("writer lacks a namespace grant: {0:?}")]
    NamespaceDenied(NodeId),
    #[error("writer registration needs at least one namespace")]
    NoNamespaces,
    #[error("conflicting writer registration: {0:?}")]
    ConflictingRegistration(NodeId),
    #[error("competing creation against {0:?}")]
    CompetingCreation(CrdtSubject),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::HlcTimestamp;
    use crate::operation::{CertificationMode, EventDot, OperationLeaf};
    use tempfile::tempdir;

    fn subject(writer: &str, counter: u64) -> CrdtSubject {
        CrdtSubject {
            namespace_id: "tenant".into(),
            event_dot: EventDot {
                writer: NodeId(writer.into()),
                counter,
            },
        }
    }

    fn create() -> BoundOperation {
        let subject = subject("creator", 1);
        BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: "profile/a".into(),
                    value_bytes: b"initial".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 12,
                        logical: 2,
                        node_id: "creator".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject,
                key: "profile/a".into(),
                mode: CertificationMode::Enabled,
            },
        }
    }

    fn keys() -> (SigningKey, SigningKey, WriterRegistry) {
        let creator = SigningKey::from_bytes(&[1; 32]);
        let writer = SigningKey::from_bytes(&[2; 32]);
        let json = format!(
            "[{{\"writer_id\":\"creator\",\"public_key_hex\":\"{}\",\"namespaces\":[\"tenant\"]}},{{\"writer_id\":\"writer\",\"public_key_hex\":\"{}\",\"namespaces\":[\"tenant\"]}}]",
            hex::encode(creator.verifying_key().as_bytes()),
            hex::encode(writer.verifying_key().as_bytes()),
        );
        let registry = WriterRegistry::from_json(&json).unwrap();
        (creator, writer, registry)
    }

    #[test]
    fn signatures_bind_exact_create_and_writer_action() {
        let (creator, writer, registry) = keys();
        let original = create();
        let creation = SignedCreation::sign(&original, &creator).unwrap();
        let signed_create =
            SignedBoundOperation::sign(original.clone(), &creator, creation.clone()).unwrap();
        signed_create.verify(&registry).unwrap();

        let mut add = original;
        add.operation.subject = subject("writer", 9);
        add.operation.action = CandidateKvAction::Add {
            key: "profile/a".into(),
            value_bytes: b"second".to_vec(),
            timestamp: HlcTimestamp {
                physical: 20,
                logical: 0,
                node_id: "writer".into(),
            },
        };
        let signed = SignedBoundOperation::sign(add, &writer, creation).unwrap();
        signed.verify(&registry).unwrap();
        let mut changed = signed.clone();
        if let CandidateKvAction::Add { value_bytes, .. } = &mut changed.bound.operation.action {
            *value_bytes = b"altered".to_vec();
        }
        assert!(matches!(
            changed.verify(&registry),
            Err(WriterAuthError::InvalidOperationSignature)
        ));
        let mut changed = signed.clone();
        changed.bound.declaration.mode = CertificationMode::Disabled;
        assert!(matches!(
            changed.verify(&registry),
            Err(WriterAuthError::DeclarationMismatch)
        ));
        let mut changed = signed;
        changed.creation.create_commitment[0] ^= 1;
        assert!(matches!(
            changed.verify(&registry),
            Err(WriterAuthError::InvalidCreationSignature)
        ));
    }

    #[test]
    fn rejects_mismatched_create_body_and_namespace_grant() {
        let (creator, _, registry) = keys();
        let original = create();
        let creation = SignedCreation::sign(&original, &creator).unwrap();
        let mut other_create = original.clone();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut other_create.operation.action {
            *value_bytes = b"different".to_vec();
        }
        assert!(matches!(
            SignedBoundOperation::sign(other_create, &creator, creation.clone()),
            Err(WriterAuthError::CreatePayloadMismatch)
        ));
        let mut signed = SignedBoundOperation::sign(original, &creator, creation).unwrap();
        signed.bound.operation.subject.namespace_id = "other".into();
        signed.bound.declaration.creation_id.namespace_id = "other".into();
        signed.creation.declaration.creation_id.namespace_id = "other".into();
        signed.creation.create_commitment = signed.bound.commitment();
        assert!(matches!(
            signed.verify(&registry),
            Err(WriterAuthError::NamespaceDenied(_))
        ));
    }

    #[test]
    fn compact_proof_binds_complete_create_without_repeating_its_value() {
        let (creator, writer, registry) = keys();
        let create = create();
        let proof = SignedCreation::sign(&create, &creator).unwrap();
        let signed_create =
            SignedBoundOperation::sign(create.clone(), &creator, proof.clone()).unwrap();
        signed_create.verify(&registry).unwrap();

        let mut add = create.clone();
        add.operation.subject = subject("writer", 9);
        add.operation.action = CandidateKvAction::Add {
            key: create.declaration.key.clone(),
            value_bytes: b"next value".to_vec(),
            timestamp: HlcTimestamp {
                physical: 21,
                logical: 0,
                node_id: "writer".into(),
            },
        };
        let signed_add = SignedBoundOperation::sign(add, &writer, proof).unwrap();
        signed_add.verify(&registry).unwrap();
        let mut swapped = signed_add.clone();
        swapped.creation =
            SignedCreation::sign_with_policy_digest(&create, &creator, [9; 32]).unwrap();
        assert!(matches!(
            swapped.verify(&registry),
            Err(WriterAuthError::InvalidOperationSignature)
        ));
        let json = serde_json::to_value(&signed_add).unwrap();
        assert!(json["creation"].get("bound").is_none());
        assert!(json["creation"].get("create_commitment").is_some());

        let mut changed = signed_create.clone();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut changed.bound.operation.action {
            value_bytes.push(0);
        }
        assert!(matches!(
            changed.verify(&registry),
            Err(WriterAuthError::CreatePayloadMismatch)
        ));
        let mut changed = signed_create.clone();
        if let CandidateKvAction::Create { timestamp, .. } = &mut changed.bound.operation.action {
            timestamp.logical += 1;
        }
        assert!(matches!(
            changed.verify(&registry),
            Err(WriterAuthError::CreatePayloadMismatch)
        ));
        let mut changed = signed_add.clone();
        changed.creation.create_commitment[0] ^= 1;
        assert!(matches!(
            changed.verify(&registry),
            Err(WriterAuthError::InvalidCreationSignature)
        ));
        let mut changed = signed_add;
        changed.creation.declaration.mode = CertificationMode::Disabled;
        assert!(matches!(
            changed.verify(&registry),
            Err(WriterAuthError::DeclarationMismatch)
        ));
    }

    #[test]
    fn local_creation_conflict_survives_reopen() {
        let dir = tempdir().unwrap();
        let original = create();
        {
            let mut ledger = OperationLedger::open(dir.path()).unwrap();
            ledger.record_and_sync(original.clone()).unwrap();
        }
        let mut ledger = OperationLedger::open(dir.path()).unwrap();
        let mut competing = create();
        competing.operation.subject = subject("writer", 1);
        competing.declaration.creation_id = competing.operation.subject.clone();
        assert!(matches!(
            check_local_create_conflict(&ledger, &competing),
            Err(WriterAuthError::CompetingCreation(_))
        ));
        assert!(check_local_create_conflict(&ledger, &original).is_ok());

        ledger
            .compact_and_sync(&original.operation.subject)
            .unwrap();
        ledger.checkpoint_and_sync().unwrap();
        drop(ledger);
        let ledger = OperationLedger::open(dir.path()).unwrap();
        assert!(ledger.get(&original.operation.subject).is_none());
        assert!(matches!(
            check_local_create_conflict(&ledger, &competing),
            Err(WriterAuthError::CompetingCreation(_))
        ));
        assert!(check_local_create_conflict(&ledger, &original).is_ok());
    }

    #[test]
    fn manifest_rejects_duplicate_writer_and_empty_acl() {
        let key = hex::encode(SigningKey::from_bytes(&[3; 32]).verifying_key().as_bytes());
        let duplicate = format!(
            "[{{\"writer_id\":\"x\",\"public_key_hex\":\"{key}\",\"namespaces\":[\"n\"]}},{{\"writer_id\":\"x\",\"public_key_hex\":\"{key}\",\"namespaces\":[\"n\"]}}]"
        );
        assert!(matches!(
            WriterRegistry::from_json(&duplicate),
            Err(WriterAuthError::ConflictingRegistration(_))
        ));
        let empty =
            format!("[{{\"writer_id\":\"x\",\"public_key_hex\":\"{key}\",\"namespaces\":[]}}]");
        assert!(matches!(
            WriterRegistry::from_json(&empty),
            Err(WriterAuthError::NoNamespaces)
        ));
    }
}
