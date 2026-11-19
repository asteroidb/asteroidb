//! Authority admission for an immutable operation commitment.
//!
//! This is the `MerkleFlow.record` boundary. The policy checks correspond to
//! `OperationAdmission.valid` and `BoundOperation.certifiable` in aste-lean.
//! A successful result is a candidate for a future Merkle leaf; it is neither
//! a root signature nor a certificate. A caller must still build a root from
//! locked leaves and prove inclusion before issuing a root signature.

use crate::authority::creation_policy::{CreationPolicyError, CreationPolicyManifest};
use crate::authority::writer_auth::{
    SignedBoundOperation, WriterAuthError, WriterRegistry, check_compact_creation_witness,
    check_local_create_conflict,
};
use crate::control_plane::system_namespace::SystemNamespace;
use crate::operation::{
    BoundOperation, CandidateKvAction, CrdtSubject, OperationError, RecordOutcome,
};
use crate::types::NodeId;

/// Authentication and policy inputs supplied by the Authority deployment.
///
/// `creation_binding_valid` must authenticate the creation that produced the
/// exact declaration. For a create action it must also authenticate the
/// operation's exact value and timestamp. The deployment must enforce the
/// Lean workload assumptions: an authenticated operation ID has one content,
/// and competing creates for the same namespace/key are excluded. A local
/// lookup alone cannot establish either global assumption.
pub trait OperationAdmissionPolicy {
    fn authorize(&self, subject: &CrdtSubject, action: &CandidateKvAction) -> bool;
    fn event_allowed(&self, subject: &CrdtSubject) -> bool;
    fn key_in_range(&self, namespace_id: &str, key: &str) -> bool;
    fn creation_binding_valid(&self, bound: &BoundOperation) -> bool;
}

/// Trust decisions supplied by a creator authentication and writer identity
/// mechanism. Implementations must enforce one authenticated content per
/// operation ID and exclude competing creates for a namespace/key across the
/// deployment. The local Authority ledger alone cannot establish either
/// distributed workload guarantee.
pub trait OperationTrust {
    fn authorizes(&self, subject: &CrdtSubject, action: &CandidateKvAction) -> bool;
    fn event_allowed(&self, subject: &CrdtSubject) -> bool;
    fn authentic_creation(&self, bound: &BoundOperation) -> bool;
}

/// The deployed control-plane half of operation admission. The configured
/// namespace ID selects the operation assignment. A missing policy, disabled certified range,
/// absent authority, or shadowed prefix all deny admission.
pub struct AuthorityOperationPolicy<'a, T> {
    namespace: &'a SystemNamespace,
    authority_id: &'a NodeId,
    namespace_id: &'a str,
    trust: &'a T,
}

impl<T: OperationTrust> OperationAdmissionPolicy for AuthorityOperationPolicy<'_, T> {
    fn authorize(&self, subject: &CrdtSubject, action: &CandidateKvAction) -> bool {
        self.trust.authorizes(subject, action)
    }

    fn event_allowed(&self, subject: &CrdtSubject) -> bool {
        self.trust.event_allowed(subject)
    }

    fn key_in_range(&self, namespace_id: &str, key: &str) -> bool {
        if namespace_id != self.namespace_id {
            return false;
        }
        let Some(authorities) = self
            .namespace
            .operation_authorities_for_key(namespace_id, key)
        else {
            return false;
        };
        if !authorities.authority_nodes.contains(self.authority_id) {
            return false;
        }
        self.namespace
            .operation_placement_for_key(namespace_id, key)
            .is_some_and(|policy| {
                policy.certified && policy.key_range.prefix == authorities.key_range.prefix
            })
    }

    fn creation_binding_valid(&self, bound: &BoundOperation) -> bool {
        self.trust.authentic_creation(bound)
    }
}

/// Validate a submitted operation against the current control plane and
/// creator trust source, then durably lock its exact content at this
/// Authority. The returned token is eligible for a Merkle leaf; it is not a
/// certificate or an authorization to sign a frontier checkpoint for it.
pub fn admit_for_authority<T: OperationTrust>(
    namespace: &SystemNamespace,
    authority_id: &NodeId,
    namespace_id: &str,
    trust: &T,
    ledger: &mut crate::operation::OperationLedger,
    bound: BoundOperation,
) -> Result<SignableCommitment, AdmissionError<crate::operation::PersistOperationError>> {
    let policy = AuthorityOperationPolicy {
        namespace,
        authority_id,
        namespace_id,
        trust,
    };
    admit_for_signing(&policy, ledger, bound)
}

/// Concrete writer-authenticated Authority entry point. The signed creation
/// and action are checked before a local competing-create check and the
/// durable operation lock. One call must hold exclusive access to `ledger`.
/// A remote partition can still hide a competing creation; deployment-wide
/// exclusion is the separate assumption in Lean's `EdgeCreation.Context`.
pub fn admit_signed_for_authority(
    namespace: &SystemNamespace,
    authority_id: &NodeId,
    namespace_id: &str,
    writers: &WriterRegistry,
    ledger: &mut crate::operation::OperationLedger,
    signed: &SignedBoundOperation,
) -> Result<SignableCommitment, SignedAdmissionError> {
    admit_signed_for_authority_with_creation_policy(
        namespace,
        authority_id,
        namespace_id,
        writers,
        None,
        ledger,
        signed,
    )
}

/// Admission with the same immutable creation policy as the operation inbox.
/// The Authority must make this check itself before durably locking or signing
/// a root; an authenticated packet may also arrive through other callers.
pub fn admit_signed_for_authority_with_creation_policy(
    namespace: &SystemNamespace,
    authority_id: &NodeId,
    namespace_id: &str,
    writers: &WriterRegistry,
    creation_policy: Option<&CreationPolicyManifest>,
    ledger: &mut crate::operation::OperationLedger,
    signed: &SignedBoundOperation,
) -> Result<SignableCommitment, SignedAdmissionError> {
    admit_signed_for_authority_with_policy_witness(
        namespace,
        authority_id,
        namespace_id,
        writers,
        creation_policy,
        ledger,
        signed,
        None,
    )
}

/// The complete Create may have arrived as a durable policy-only witness
/// before its application body. A compact follow-up can then be admitted
/// after independently verifying that exact signed Create and commitment.
#[allow(clippy::too_many_arguments)]
pub fn admit_signed_for_authority_with_policy_witness(
    namespace: &SystemNamespace,
    authority_id: &NodeId,
    namespace_id: &str,
    writers: &WriterRegistry,
    creation_policy: Option<&CreationPolicyManifest>,
    ledger: &mut crate::operation::OperationLedger,
    signed: &SignedBoundOperation,
    creation_witness: Option<&SignedBoundOperation>,
) -> Result<SignableCommitment, SignedAdmissionError> {
    admit_signed_for_authority_with_policy_witness_or_compact(
        namespace,
        authority_id,
        namespace_id,
        writers,
        creation_policy,
        ledger,
        signed,
        creation_witness,
        None,
    )
}

/// A compact marker is available only from an inbox that previously verified
/// and durably stored the complete signed Create.
#[allow(clippy::too_many_arguments)]
pub fn admit_signed_for_authority_with_policy_witness_or_compact(
    namespace: &SystemNamespace,
    authority_id: &NodeId,
    namespace_id: &str,
    writers: &WriterRegistry,
    creation_policy: Option<&CreationPolicyManifest>,
    ledger: &mut crate::operation::OperationLedger,
    signed: &SignedBoundOperation,
    creation_witness: Option<&SignedBoundOperation>,
    creation_marker: Option<&crate::network::operation_transport::VerifiedCompactPolicyWitness>,
) -> Result<SignableCommitment, SignedAdmissionError> {
    signed
        .verify(writers)
        .map_err(SignedAdmissionError::Writer)?;
    if let Some(policy) = creation_policy {
        policy
            .validate_creation(signed)
            .map_err(SignedAdmissionError::CreationPolicy)?;
    }
    if let Err(error) = check_compact_creation_witness(ledger, signed) {
        if !matches!(error, WriterAuthError::MissingCreationWitness) {
            return Err(SignedAdmissionError::Writer(error));
        }
        if let Some(witness) = creation_witness {
            witness
                .verify(writers)
                .map_err(SignedAdmissionError::Writer)?;
            if let Some(policy) = creation_policy {
                policy
                    .validate_creation(witness)
                    .map_err(SignedAdmissionError::CreationPolicy)?;
            }
            if !matches!(
                witness.bound.operation.action,
                CandidateKvAction::Create { .. }
            ) || !signed.creation.matches_create(&witness.bound)
            {
                return Err(SignedAdmissionError::Writer(
                    WriterAuthError::MissingCreationWitness,
                ));
            }
            check_local_create_conflict(ledger, &witness.bound)
                .map_err(SignedAdmissionError::Writer)?;
        } else if let Some(marker) = creation_marker {
            let proof = &signed.creation;
            if marker.creation() != proof
                || marker.identity().subject != proof.declaration.creation_id
                || marker.identity().declaration != proof.declaration
                || marker.identity().bound_commitment != proof.create_commitment
            {
                return Err(SignedAdmissionError::Writer(
                    WriterAuthError::MissingCreationWitness,
                ));
            }
            marker
                .creation()
                .verify(writers)
                .map_err(SignedAdmissionError::Writer)?;
            if let Some(policy) = creation_policy {
                policy
                    .validate_proof(proof)
                    .map_err(SignedAdmissionError::CreationPolicy)?;
            }
        } else {
            return Err(SignedAdmissionError::Writer(error));
        }
    }
    check_local_create_conflict(ledger, &signed.bound).map_err(SignedAdmissionError::Writer)?;
    let trust = VerifiedSignedTrust { signed };
    admit_for_authority(
        namespace,
        authority_id,
        namespace_id,
        &trust,
        ledger,
        signed.bound.clone(),
    )
    .map_err(SignedAdmissionError::Admission)
}

#[derive(Debug)]
pub enum SignedAdmissionError {
    Writer(WriterAuthError),
    CreationPolicy(CreationPolicyError),
    Admission(AdmissionError<crate::operation::PersistOperationError>),
}

struct VerifiedSignedTrust<'a> {
    signed: &'a SignedBoundOperation,
}

impl OperationTrust for VerifiedSignedTrust<'_> {
    fn authorizes(&self, subject: &CrdtSubject, action: &CandidateKvAction) -> bool {
        &self.signed.bound.operation.subject == subject
            && &self.signed.bound.operation.action == action
    }

    fn event_allowed(&self, subject: &CrdtSubject) -> bool {
        &self.signed.bound.operation.subject == subject
    }

    fn authentic_creation(&self, bound: &BoundOperation) -> bool {
        &self.signed.bound == bound
    }
}

/// A durable, atomic one-ID-one-content lock.
///
/// An `Ok` return means the complete `BoundOperation` was committed to stable
/// storage before return. The store must compare full canonical content on
/// every retry, survive process restart, and never replace a prior subject's
/// content. Any write or recovery uncertainty must return `Err`, preventing
/// the caller from obtaining a signable commitment. Implementations must
/// serialize concurrent calls for the same subject.
pub trait OperationLockStore {
    type Error;

    fn record_and_sync(&mut self, bound: BoundOperation) -> Result<RecordOutcome, Self::Error>;
}

impl OperationLockStore for crate::operation::OperationLedger {
    type Error = crate::operation::PersistOperationError;

    fn record_and_sync(&mut self, bound: BoundOperation) -> Result<RecordOutcome, Self::Error> {
        crate::operation::OperationLedger::record_and_sync(self, bound)
    }
}

#[derive(Debug)]
pub enum AdmissionError<E> {
    InvalidStructure(OperationError),
    Unauthorized,
    EventDenied,
    KeyOutOfRange,
    InvalidCreationBinding,
    CertificationDisabled,
    Store(E),
}

/// Proof by construction that admission checks passed and an immutable
/// operation lock was synced. Its private fields prevent an unchecked token
/// from being constructed by another module.
#[derive(Debug, Clone)]
pub struct SignableCommitment {
    bound: BoundOperation,
    digest: [u8; 32],
    outcome: RecordOutcome,
}

impl SignableCommitment {
    pub fn bound(&self) -> &BoundOperation {
        &self.bound
    }

    pub fn subject(&self) -> &CrdtSubject {
        &self.bound.operation.subject
    }

    pub fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    pub fn outcome(&self) -> RecordOutcome {
        self.outcome
    }
}

/// Check the exact payload, then durably lock it before returning the digest
/// that a future Merkle leaf may contain. The store error is always fatal to
/// this admission attempt, including on a retry.
pub fn admit_for_signing<P: OperationAdmissionPolicy, S: OperationLockStore>(
    policy: &P,
    store: &mut S,
    bound: BoundOperation,
) -> Result<SignableCommitment, AdmissionError<S::Error>> {
    bound
        .validate_structure()
        .map_err(AdmissionError::InvalidStructure)?;

    let subject = &bound.operation.subject;
    let action = &bound.operation.action;
    if !policy.authorize(subject, action) {
        return Err(AdmissionError::Unauthorized);
    }
    if !policy.event_allowed(subject) {
        return Err(AdmissionError::EventDenied);
    }
    if !policy.key_in_range(&subject.namespace_id, action.key()) {
        return Err(AdmissionError::KeyOutOfRange);
    }
    if !policy.creation_binding_valid(&bound) {
        return Err(AdmissionError::InvalidCreationBinding);
    }
    if bound.declaration.mode != crate::operation::CertificationMode::Enabled {
        return Err(AdmissionError::CertificationDisabled);
    }

    let digest = bound.commitment();
    let outcome = store
        .record_and_sync(bound.clone())
        .map_err(AdmissionError::Store)?;
    Ok(SignableCommitment {
        bound,
        digest,
        outcome,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use crate::authority::writer_auth::SignedCreation;
    use crate::control_plane::system_namespace::AuthorityDefinition;
    use crate::hlc::HlcTimestamp;
    use crate::operation::{CertificationMode, Declaration, EventDot, OperationLeaf};
    use crate::placement::PlacementPolicy;
    use crate::types::{KeyRange, NodeId, PolicyVersion};
    use ed25519_dalek::SigningKey;

    struct AllowAll;

    impl OperationAdmissionPolicy for AllowAll {
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
    struct TestStore {
        chosen: Option<BoundOperation>,
        fail: bool,
        calls: usize,
    }

    impl OperationLockStore for TestStore {
        type Error = &'static str;

        fn record_and_sync(&mut self, bound: BoundOperation) -> Result<RecordOutcome, Self::Error> {
            self.calls += 1;
            if self.fail {
                return Err("sync failed");
            }
            match &self.chosen {
                Some(old) if old == &bound => Ok(RecordOutcome::Retry),
                Some(_) => Err("conflicting operation"),
                None => {
                    self.chosen = Some(bound);
                    Ok(RecordOutcome::New)
                }
            }
        }
    }

    fn operation() -> BoundOperation {
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 7,
            },
        };
        BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: "key".into(),
                    value_bytes: b"value".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 10,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject,
                key: "key".into(),
                mode: CertificationMode::Enabled,
            },
        }
    }

    #[test]
    fn valid_exact_operation_is_locked_before_token_is_returned() {
        let bound = operation();
        let mut store = TestStore::default();
        let first = admit_for_signing(&AllowAll, &mut store, bound.clone()).unwrap();
        assert_eq!(first.digest(), &bound.commitment());
        assert_eq!(first.bound(), &bound);
        assert_eq!(first.outcome(), RecordOutcome::New);
        assert_eq!(store.chosen.as_ref(), Some(&bound));
        let retry = admit_for_signing(&AllowAll, &mut store, bound).unwrap();
        assert_eq!(retry.outcome(), RecordOutcome::Retry);
    }

    #[test]
    fn store_failure_and_conflicting_content_cannot_produce_token() {
        let mut store = TestStore {
            fail: true,
            ..Default::default()
        };
        assert!(matches!(
            admit_for_signing(&AllowAll, &mut store, operation()),
            Err(AdmissionError::Store("sync failed"))
        ));
        store.fail = false;
        admit_for_signing(&AllowAll, &mut store, operation()).unwrap();
        let mut changed = operation();
        changed.declaration.mode = CertificationMode::Disabled;
        assert!(matches!(
            admit_for_signing(&AllowAll, &mut store, changed),
            Err(AdmissionError::CertificationDisabled)
        ));
        let mut changed = operation();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut changed.operation.action {
            *value_bytes = b"other".to_vec();
        }
        assert!(matches!(
            admit_for_signing(&AllowAll, &mut store, changed),
            Err(AdmissionError::Store("conflicting operation"))
        ));
    }

    #[test]
    fn structural_failure_does_not_touch_store() {
        let mut bound = operation();
        bound.declaration.key = "wrong".into();
        let mut store = TestStore::default();
        assert!(matches!(
            admit_for_signing(&AllowAll, &mut store, bound),
            Err(AdmissionError::InvalidStructure(
                OperationError::DifferentKey
            ))
        ));
        assert_eq!(store.calls, 0);
    }

    #[test]
    fn durable_ledger_rejects_content_swap_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let original = operation();
        {
            let mut ledger = crate::operation::OperationLedger::open(dir.path()).unwrap();
            let token = admit_for_signing(&AllowAll, &mut ledger, original.clone()).unwrap();
            assert_eq!(token.outcome(), RecordOutcome::New);
        }
        let mut ledger = crate::operation::OperationLedger::open(dir.path()).unwrap();
        let retry = admit_for_signing(&AllowAll, &mut ledger, original.clone()).unwrap();
        assert_eq!(retry.outcome(), RecordOutcome::Retry);
        let mut changed = original;
        if let CandidateKvAction::Create { value_bytes, .. } = &mut changed.operation.action {
            *value_bytes = b"replacement".to_vec();
        }
        assert!(matches!(
            admit_for_signing(&AllowAll, &mut ledger, changed),
            Err(AdmissionError::Store(
                crate::operation::PersistOperationError::Operation(
                    OperationError::ConflictingContent(_)
                )
            ))
        ));
    }

    struct TestTrust {
        authorized: bool,
        event: bool,
        creation: bool,
    }

    impl OperationTrust for TestTrust {
        fn authorizes(&self, _: &CrdtSubject, _: &CandidateKvAction) -> bool {
            self.authorized
        }
        fn event_allowed(&self, _: &CrdtSubject) -> bool {
            self.event
        }
        fn authentic_creation(&self, _: &BoundOperation) -> bool {
            self.creation
        }
    }

    fn configured_namespace() -> SystemNamespace {
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(1), KeyRange { prefix: "k".into() }, 1)
                .with_certified(true),
        )
        .unwrap();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange { prefix: "k".into() },
            authority_nodes: vec![NodeId("authority".into())],
            auto_generated: false,
        });
        ns
    }

    #[test]
    fn deployed_policy_requires_trust_and_certified_authority_scope() {
        let dir = tempfile::tempdir().unwrap();
        let mut ledger = crate::operation::OperationLedger::open(dir.path()).unwrap();
        let bound = operation();
        let authority = NodeId("authority".into());
        let mut trust = TestTrust {
            authorized: true,
            event: true,
            creation: false,
        };
        let mut ns = configured_namespace();
        assert!(matches!(
            admit_for_authority(&ns, &authority, "ns", &trust, &mut ledger, bound.clone()),
            Err(AdmissionError::InvalidCreationBinding)
        ));
        assert!(ledger.is_empty());
        trust.creation = true;
        trust.authorized = false;
        assert!(matches!(
            admit_for_authority(&ns, &authority, "ns", &trust, &mut ledger, bound.clone()),
            Err(AdmissionError::Unauthorized)
        ));
        trust.authorized = true;
        trust.event = false;
        assert!(matches!(
            admit_for_authority(&ns, &authority, "ns", &trust, &mut ledger, bound.clone()),
            Err(AdmissionError::EventDenied)
        ));
        trust.event = true;
        assert!(matches!(
            admit_for_authority(&ns, &authority, "other", &trust, &mut ledger, bound.clone()),
            Err(AdmissionError::KeyOutOfRange)
        ));
        assert!(matches!(
            admit_for_authority(
                &ns,
                &NodeId("outsider".into()),
                "ns",
                &trust,
                &mut ledger,
                bound.clone()
            ),
            Err(AdmissionError::KeyOutOfRange)
        ));
        assert_eq!(
            admit_for_authority(&ns, &authority, "ns", &trust, &mut ledger, bound.clone())
                .unwrap()
                .outcome(),
            RecordOutcome::New
        );

        // A more specific authority definition shadows the old range. An
        // authority cannot keep signing through its stale parent scope.
        ns.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "key".into(),
            },
            authority_nodes: vec![NodeId("other".into())],
            auto_generated: false,
        });
        assert!(matches!(
            admit_for_authority(&ns, &authority, "ns", &trust, &mut ledger, bound),
            Err(AdmissionError::KeyOutOfRange)
        ));
        assert_eq!(ledger.len(), 1);
    }

    #[test]
    fn disabled_or_missing_policy_cannot_lock_operation() {
        let dir = tempfile::tempdir().unwrap();
        let mut ledger = crate::operation::OperationLedger::open(dir.path()).unwrap();
        let authority = NodeId("authority".into());
        let trust = TestTrust {
            authorized: true,
            event: true,
            creation: true,
        };
        let mut ns = configured_namespace();
        ns.remove_placement_policy("k");
        assert!(matches!(
            admit_for_authority(&ns, &authority, "ns", &trust, &mut ledger, operation()),
            Err(AdmissionError::KeyOutOfRange)
        ));
        ns.set_placement_policy(PlacementPolicy::new(
            PolicyVersion(2),
            KeyRange { prefix: "k".into() },
            1,
        ))
        .unwrap();
        assert!(matches!(
            admit_for_authority(&ns, &authority, "ns", &trust, &mut ledger, operation()),
            Err(AdmissionError::KeyOutOfRange)
        ));
        assert!(ledger.is_empty());

        // A nested disabled placement policy shadows the certified parent,
        // even when the parent still names this Authority.
        let mut ns = configured_namespace();
        ns.set_placement_policy(PlacementPolicy::new(
            PolicyVersion(2),
            KeyRange {
                prefix: "key".into(),
            },
            1,
        ))
        .unwrap();
        assert!(matches!(
            admit_for_authority(&ns, &authority, "ns", &trust, &mut ledger, operation()),
            Err(AdmissionError::KeyOutOfRange)
        ));
        assert!(ledger.is_empty());
    }

    #[test]
    fn compact_followup_waits_for_exact_durable_create_and_survives_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let key = SigningKey::from_bytes(&[37; 32]);
        let mut writers = WriterRegistry::new();
        writers
            .register(
                NodeId("writer".into()),
                key.verifying_key(),
                BTreeSet::from(["ns".into()]),
            )
            .unwrap();
        let create = operation();
        let proof = SignedCreation::sign(&create, &key).unwrap();
        let signed_create =
            SignedBoundOperation::sign(create.clone(), &key, proof.clone()).unwrap();
        let mut add = create.clone();
        add.operation.subject.event_dot.counter += 1;
        add.operation.action = CandidateKvAction::Add {
            key: "key".into(),
            value_bytes: b"later".to_vec(),
            timestamp: HlcTimestamp {
                physical: 11,
                logical: 0,
                node_id: "writer".into(),
            },
        };
        let signed_add = SignedBoundOperation::sign(add, &key, proof).unwrap();
        let authority = NodeId("authority".into());
        let namespace = configured_namespace();
        let mut ledger = crate::operation::OperationLedger::open(dir.path()).unwrap();

        // The creator's signature on an opaque hash alone is not a witnessed
        // Create. A future reconcile can retry this unchanged inbox packet.
        assert!(matches!(
            admit_signed_for_authority(
                &namespace,
                &authority,
                "ns",
                &writers,
                &mut ledger,
                &signed_add,
            ),
            Err(SignedAdmissionError::Writer(
                WriterAuthError::MissingCreationWitness
            ))
        ));
        assert!(ledger.is_empty());

        admit_signed_for_authority(
            &namespace,
            &authority,
            "ns",
            &writers,
            &mut ledger,
            &signed_create,
        )
        .unwrap();
        admit_signed_for_authority(
            &namespace,
            &authority,
            "ns",
            &writers,
            &mut ledger,
            &signed_add,
        )
        .unwrap();

        // A signed proof for a different initial value must not borrow the
        // existing Create witness, even though its declaration is identical.
        let mut different_create = create.clone();
        if let CandidateKvAction::Create { value_bytes, .. } =
            &mut different_create.operation.action
        {
            *value_bytes = b"different".to_vec();
        }
        let other_proof = SignedCreation::sign(&different_create, &key).unwrap();
        let mut other_add = signed_add.bound.clone();
        other_add.operation.subject.event_dot.counter += 1;
        let other_add = SignedBoundOperation::sign(other_add, &key, other_proof).unwrap();
        assert!(matches!(
            admit_signed_for_authority(
                &namespace,
                &authority,
                "ns",
                &writers,
                &mut ledger,
                &other_add,
            ),
            Err(SignedAdmissionError::Writer(
                WriterAuthError::MissingCreationWitness
            ))
        ));

        ledger.compact_and_sync(&create.operation.subject).unwrap();
        ledger.checkpoint_and_sync().unwrap();
        drop(ledger);
        let mut ledger = crate::operation::OperationLedger::open(dir.path()).unwrap();
        assert!(ledger.get(&create.operation.subject).is_none());
        assert!(ledger.receipt(&create.operation.subject).is_some());
        assert_eq!(
            admit_signed_for_authority(
                &namespace,
                &authority,
                "ns",
                &writers,
                &mut ledger,
                &signed_add,
            )
            .unwrap()
            .outcome(),
            RecordOutcome::Retry
        );
    }
}
