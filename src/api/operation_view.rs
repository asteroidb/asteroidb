//! Read projection over immutable operation bodies.
//!
//! This is the value calculation in `EdgeCore.project`: any retained delete
//! wins, while create/add values use HLC order and an equal-HLC value tie
//! break. The caller chooses the operation basis. Certified reads below use
//! only operation certificates that were verified against held bodies.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::authority::operation_merkle::{VerifiedCompactOperation, VerifiedOperation};
use crate::hlc::HlcTimestamp;
use crate::operation::{
    BoundOperation, CandidateKvAction, CertificationMode, CrdtSubject, OperationHistory,
};

/// A local operation view. `Unknown` means this node holds no selected
/// operation for the key; it makes no cluster-wide absence claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationRead {
    Unknown,
    Deleted,
    Value {
        value_bytes: Vec<u8>,
        timestamp: HlcTimestamp,
    },
}

/// Certified read states. A compact follow-up can be held before its signed
/// Create body arrives, leaving the creation policy unknown temporarily.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CertifiedReadResult {
    Unknown,
    PolicyUnknown,
    NotTargeted,
    NoCertifiedInformation,
    Deleted,
    Value {
        value_bytes: Vec<u8>,
        timestamp: HlcTimestamp,
    },
}

/// Project a finite set of exact operation bodies for one namespace/key.
/// Duplicate bodies and delivery order do not change the result.
pub fn project<'a>(
    operations: impl IntoIterator<Item = &'a BoundOperation>,
    namespace_id: &str,
    key: &str,
) -> OperationRead {
    let mut winner: Option<(HlcTimestamp, Vec<u8>)> = None;
    for bound in operations {
        if bound.operation.subject.namespace_id != namespace_id
            || bound.operation.action.key() != key
        {
            continue;
        }
        match &bound.operation.action {
            CandidateKvAction::Delete { .. } => return OperationRead::Deleted,
            CandidateKvAction::Create {
                value_bytes,
                timestamp,
                ..
            }
            | CandidateKvAction::Add {
                value_bytes,
                timestamp,
                ..
            } => {
                let candidate = (timestamp.clone(), value_bytes.clone());
                if winner.as_ref().is_none_or(|old| &candidate > old) {
                    winner = Some(candidate);
                }
            }
        }
    }
    match winner {
        Some((timestamp, value_bytes)) => OperationRead::Value {
            value_bytes,
            timestamp,
        },
        None => OperationRead::Unknown,
    }
}

/// Ordinary local read: includes every retained operation, certified or not.
pub fn ordinary_read(history: &OperationHistory, namespace_id: &str, key: &str) -> OperationRead {
    project(history.iter(), namespace_id, key)
}

/// A certified projection with the locally known basis supplied by the caller.
/// Each entry in `basis` was verified cryptographically and matches an exact
/// body in the local history, including bodies for other keys. An empty basis
/// with `local_body_seen` means the key is known locally but this node cannot
/// yet compute a certified value for it.
pub struct CertifiedOperationRead<'a> {
    pub result: CertifiedReadResult,
    pub basis: Vec<&'a VerifiedOperation>,
    pub local_body_seen: bool,
}

pub fn certified_read<'a>(
    history: &OperationHistory,
    verified: impl IntoIterator<Item = &'a VerifiedOperation>,
    namespace_id: &str,
    key: &str,
) -> CertifiedOperationRead<'a> {
    certified_read_with_policy(history, verified, namespace_id, key, |_| true)
}

/// Read when a signed compact creation reference may precede the actual
/// authenticated Create. Such a body is visible to ordinary reads, but its
/// declaration cannot yet establish the key's certification policy.
pub fn certified_read_with_policy<'a>(
    history: &OperationHistory,
    verified: impl IntoIterator<Item = &'a VerifiedOperation>,
    namespace_id: &str,
    key: &str,
    policy_known: impl Fn(&BoundOperation) -> bool,
) -> CertifiedOperationRead<'a> {
    certified_read_with_known_mode(history, verified, namespace_id, key, policy_known, None)
}

/// `known_mode` is a separately authenticated creation policy witness. It
/// may arrive before any operation body for this key.
pub fn certified_read_with_known_mode<'a>(
    history: &OperationHistory,
    verified: impl IntoIterator<Item = &'a VerifiedOperation>,
    namespace_id: &str,
    key: &str,
    policy_known: impl Fn(&BoundOperation) -> bool,
    known_mode: Option<CertificationMode>,
) -> CertifiedOperationRead<'a> {
    let on_key = |bound: &&BoundOperation| {
        bound.operation.subject.namespace_id == namespace_id && bound.operation.action.key() == key
    };
    let local_body_seen = history.iter().any(|bound| on_key(&bound));
    let mut seen = HashSet::<CrdtSubject>::new();
    let basis: Vec<&VerifiedOperation> = verified
        .into_iter()
        .filter(|proof| {
            let bound = proof.bound();
            history.get(&bound.operation.subject) == Some(bound)
                && seen.insert(bound.operation.subject.clone())
        })
        .collect();
    let local_mode = history
        .iter()
        .filter(on_key)
        .find(|bound| policy_known(bound))
        .map(|bound| bound.declaration.mode)
        .or(known_mode);
    let result = match local_mode {
        None if local_body_seen => CertifiedReadResult::PolicyUnknown,
        None => CertifiedReadResult::Unknown,
        Some(CertificationMode::Disabled) => CertifiedReadResult::NotTargeted,
        Some(CertificationMode::Enabled) => {
            match project(basis.iter().map(|proof| proof.bound()), namespace_id, key) {
                OperationRead::Unknown if local_body_seen => {
                    CertifiedReadResult::NoCertifiedInformation
                }
                OperationRead::Unknown => CertifiedReadResult::Unknown,
                OperationRead::Deleted => CertifiedReadResult::Deleted,
                OperationRead::Value {
                    value_bytes,
                    timestamp,
                } => CertifiedReadResult::Value {
                    value_bytes,
                    timestamp,
                },
            }
        }
    };
    CertifiedOperationRead {
        result,
        basis,
        local_body_seen,
    }
}

/// Local status of one exact operation ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationStatus {
    Unknown,
    PolicyUnknown,
    NotTargeted,
    Pending,
    Certified,
}

/// Report status from verified local evidence. `policy_known` must reflect an
/// authenticated Create (or its retained receipt); a signed follow-up
/// declaration alone does not establish policy.
pub fn operation_status(
    history: &OperationHistory,
    subject: &CrdtSubject,
    verified: &[VerifiedOperation],
    verified_compact: &[VerifiedCompactOperation],
    policy_known: impl Fn(&CrdtSubject) -> bool,
) -> OperationStatus {
    if let Some(bound) = history.get(subject) {
        return if !policy_known(subject) {
            OperationStatus::PolicyUnknown
        } else if bound.declaration.mode == CertificationMode::Disabled {
            OperationStatus::NotTargeted
        } else if verified.iter().any(|proof| proof.bound() == bound)
            || verified_compact.iter().any(|proof| {
                proof.certificate().subject == *subject
                    && proof.certificate().commitment == bound.commitment()
            })
        {
            OperationStatus::Certified
        } else {
            OperationStatus::Pending
        };
    }
    match history.receipt(subject) {
        None => OperationStatus::Unknown,
        Some(_) if !policy_known(subject) => OperationStatus::PolicyUnknown,
        Some(receipt) if receipt.declaration.mode == CertificationMode::Disabled => {
            OperationStatus::NotTargeted
        }
        Some(receipt)
            if verified.iter().any(|proof| {
                proof.bound().operation.subject == *subject
                    && proof.bound().commitment() == receipt.commitment
            }) || verified_compact.iter().any(|proof| {
                proof.certificate().subject == *subject
                    && proof.certificate().commitment == receipt.commitment
            }) =>
        {
            OperationStatus::Certified
        }
        Some(_) => OperationStatus::Pending,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::certificate::{EpochConfig, KeysetRegistry, KeysetVersion};
    use crate::authority::operation_admission::{OperationAdmissionPolicy, admit_for_signing};
    use crate::authority::operation_merkle::{
        CompactOperationCertificate, OperationMerkleCertificate, OperationMerkleTree,
        OperationRootSupport, OperationScope, SignedOperationRoot,
    };
    use crate::operation::{CertificationMode, CrdtSubject, Declaration, EventDot, OperationLeaf};
    use crate::types::NodeId;
    use ed25519_dalek::SigningKey;

    struct Allow;

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

    fn bound(counter: u64, action: CandidateKvAction) -> BoundOperation {
        let creation_id = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 1,
            },
        };
        BoundOperation {
            operation: OperationLeaf {
                subject: CrdtSubject {
                    namespace_id: "ns".into(),
                    event_dot: EventDot {
                        writer: NodeId("writer".into()),
                        counter,
                    },
                },
                action,
            },
            declaration: Declaration {
                creation_id,
                key: "key".into(),
                mode: CertificationMode::Enabled,
            },
        }
    }

    fn ts(physical: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical: 0,
            node_id: "writer".into(),
        }
    }

    #[test]
    fn ordinary_projection_keeps_pending_update_and_delete() {
        let first = bound(
            1,
            CandidateKvAction::Create {
                key: "key".into(),
                value_bytes: b"100".to_vec(),
                timestamp: ts(10),
            },
        );
        let second = bound(
            2,
            CandidateKvAction::Add {
                key: "key".into(),
                value_bytes: b"200".to_vec(),
                timestamp: ts(20),
            },
        );
        let deletion = bound(3, CandidateKvAction::Delete { key: "key".into() });
        let mut history = OperationHistory::default();
        history.record(first.clone()).unwrap();
        history.record(second.clone()).unwrap();
        assert_eq!(
            ordinary_read(&history, "ns", "key"),
            OperationRead::Value {
                value_bytes: b"200".to_vec(),
                timestamp: ts(20),
            }
        );
        assert_eq!(
            project([&second, &first, &second], "ns", "key"),
            ordinary_read(&history, "ns", "key")
        );
        history.record(deletion).unwrap();
        assert_eq!(ordinary_read(&history, "ns", "key"), OperationRead::Deleted);
    }

    #[test]
    fn equal_hlc_uses_value_tie_break() {
        let low = bound(
            1,
            CandidateKvAction::Create {
                key: "key".into(),
                value_bytes: b"a".to_vec(),
                timestamp: ts(10),
            },
        );
        let high = bound(
            2,
            CandidateKvAction::Add {
                key: "key".into(),
                value_bytes: b"z".to_vec(),
                timestamp: ts(10),
            },
        );
        assert_eq!(
            project([&high, &low], "ns", "key"),
            project([&low, &high], "ns", "key")
        );
        assert_eq!(
            project([&high, &low], "ns", "key"),
            OperationRead::Value {
                value_bytes: b"z".to_vec(),
                timestamp: ts(10),
            }
        );

        let prefix = bound(
            3,
            CandidateKvAction::Add {
                key: "key".into(),
                value_bytes: vec![1],
                timestamp: ts(10),
            },
        );
        let extension = bound(
            4,
            CandidateKvAction::Add {
                key: "key".into(),
                value_bytes: vec![1, 0],
                timestamp: ts(10),
            },
        );
        let larger_first_byte = bound(
            5,
            CandidateKvAction::Add {
                key: "key".into(),
                value_bytes: vec![2],
                timestamp: ts(10),
            },
        );
        assert_eq!(
            project([&prefix, &extension], "ns", "key"),
            OperationRead::Value {
                value_bytes: vec![1, 0],
                timestamp: ts(10),
            }
        );
        assert_eq!(
            project([&extension, &larger_first_byte], "ns", "key"),
            OperationRead::Value {
                value_bytes: vec![2],
                timestamp: ts(10),
            }
        );
    }

    #[test]
    fn unknown_creation_policy_does_not_turn_disabled_declaration_into_opt_out() {
        let mut create = bound(
            1,
            CandidateKvAction::Create {
                key: "key".into(),
                value_bytes: b"value".to_vec(),
                timestamp: ts(1),
            },
        );
        create.declaration.mode = CertificationMode::Disabled;
        let subject = create.operation.subject.clone();
        let mut history = OperationHistory::default();
        history.record(create).unwrap();

        assert_eq!(
            operation_status(&history, &subject, &[], &[], |_| false),
            OperationStatus::PolicyUnknown
        );
        assert_eq!(
            operation_status(&history, &subject, &[], &[], |_| true),
            OperationStatus::NotTargeted
        );
    }

    #[test]
    fn certified_read_distinguishes_absent_pending_and_disabled() {
        let create = bound(
            1,
            CandidateKvAction::Create {
                key: "key".into(),
                value_bytes: b"value".to_vec(),
                timestamp: ts(1),
            },
        );
        let mut history = OperationHistory::default();
        let absent = certified_read(
            &history,
            std::iter::empty::<&VerifiedOperation>(),
            "ns",
            "key",
        );
        assert_eq!(absent.result, CertifiedReadResult::Unknown);
        assert!(!absent.local_body_seen);

        history.record(create.clone()).unwrap();
        let pending = certified_read(
            &history,
            std::iter::empty::<&VerifiedOperation>(),
            "ns",
            "key",
        );
        assert_eq!(pending.result, CertifiedReadResult::NoCertifiedInformation);
        assert!(pending.local_body_seen);
        assert!(pending.basis.is_empty());

        let mut disabled = create;
        disabled.declaration.mode = CertificationMode::Disabled;
        let mut disabled_history = OperationHistory::default();
        disabled_history.record(disabled).unwrap();
        let untargeted = certified_read(
            &disabled_history,
            std::iter::empty::<&VerifiedOperation>(),
            "ns",
            "key",
        );
        assert_eq!(untargeted.result, CertifiedReadResult::NotTargeted);
        assert!(untargeted.local_body_seen);
        assert!(untargeted.basis.is_empty());
    }

    #[test]
    fn pending_update_and_delete_do_not_hide_certified_value() {
        let create = bound(
            1,
            CandidateKvAction::Create {
                key: "key".into(),
                value_bytes: b"100".to_vec(),
                timestamp: ts(10),
            },
        );
        let update = bound(
            2,
            CandidateKvAction::Add {
                key: "key".into(),
                value_bytes: b"200".to_vec(),
                timestamp: ts(20),
            },
        );
        let delete = bound(3, CandidateKvAction::Delete { key: "key".into() });
        let bodies = [create.clone(), update.clone(), delete.clone()];
        let mut history = OperationHistory::default();
        for body in &bodies {
            history.record(body.clone()).unwrap();
        }

        let dir = tempfile::tempdir().unwrap();
        let mut ledger = crate::operation::OperationLedger::open(dir.path()).unwrap();
        let tokens: Vec<_> = bodies
            .iter()
            .map(|body| admit_for_signing(&Allow, &mut ledger, body.clone()).unwrap())
            .collect();
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "key".into(),
        };
        let tree = OperationMerkleTree::from_signable(scope.clone(), &tokens).unwrap();
        let signer = SigningKey::from_bytes(&[7; 32]);
        let authority = NodeId("auth".into());
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![(authority.clone(), signer.verifying_key())],
            )
            .unwrap();
        let root = SignedOperationRoot::sign(&tree, KeysetVersion(1), authority, &signer);
        let verified: Vec<_> = bodies
            .iter()
            .map(|body| {
                OperationMerkleCertificate {
                    operation: body.clone(),
                    supports: vec![OperationRootSupport {
                        signed_root: root.clone(),
                        proof: tree.proof(&body.operation.subject).unwrap(),
                    }],
                }
                .verify(
                    &registry,
                    &scope,
                    KeysetVersion(1),
                    0,
                    &EpochConfig::default(),
                    0,
                )
                .unwrap()
            })
            .collect();

        // The certificate may arrive from an Authority before this receiver
        // learns the authenticated Create. Evidence does not select policy.
        let mut add_only = OperationHistory::default();
        add_only.record(update.clone()).unwrap();
        let before_create =
            certified_read_with_policy(&add_only, &verified[1..2], "ns", "key", |_| false);
        assert_eq!(before_create.result, CertifiedReadResult::PolicyUnknown);
        assert_eq!(before_create.basis.len(), 1);
        let with_policy_witness = certified_read_with_known_mode(
            &add_only,
            &verified[1..2],
            "ns",
            "key",
            |_| true,
            Some(CertificationMode::Enabled),
        );
        assert_eq!(
            with_policy_witness.result,
            CertifiedReadResult::Value {
                value_bytes: b"200".to_vec(),
                timestamp: ts(20),
            }
        );
        assert_eq!(with_policy_witness.basis.len(), 1);
        assert_eq!(
            operation_status(&add_only, &update.operation.subject, &verified, &[], |_| {
                false
            }),
            OperationStatus::PolicyUnknown
        );
        assert_eq!(
            operation_status(&add_only, &update.operation.subject, &verified, &[], |_| {
                true
            }),
            OperationStatus::Certified
        );

        assert_eq!(ordinary_read(&history, "ns", "key"), OperationRead::Deleted);
        let first = certified_read(&history, &verified[..1], "ns", "key");
        assert!(first.local_body_seen);
        assert_eq!(first.basis.len(), 1);
        assert_eq!(
            first.result,
            CertifiedReadResult::Value {
                value_bytes: b"100".to_vec(),
                timestamp: ts(10),
            }
        );
        assert_eq!(
            operation_status(
                &history,
                &update.operation.subject,
                &verified[..1],
                &[],
                |_| true
            ),
            OperationStatus::Pending
        );
        let second = certified_read(&history, &verified[..2], "ns", "key");
        assert_eq!(
            second.result,
            CertifiedReadResult::Value {
                value_bytes: b"200".to_vec(),
                timestamp: ts(20),
            }
        );
        let final_read = certified_read(&history, &verified, "ns", "key");
        assert_eq!(final_read.result, CertifiedReadResult::Deleted);
        assert_eq!(final_read.basis.len(), 3);
        let other_key = certified_read(&history, &verified, "ns", "other");
        assert_eq!(other_key.result, CertifiedReadResult::Unknown);
        assert_eq!(other_key.basis.len(), 3);
        assert_eq!(
            operation_status(&history, &delete.operation.subject, &verified, &[], |_| {
                true
            }),
            OperationStatus::Certified
        );
        ledger.compact_and_sync(&update.operation.subject).unwrap();
        assert_eq!(
            operation_status(
                ledger.history(),
                &update.operation.subject,
                &verified,
                &[],
                |_| true
            ),
            OperationStatus::Certified
        );
        assert_eq!(
            operation_status(
                ledger.history(),
                &update.operation.subject,
                &verified[..1],
                &[],
                |_| true
            ),
            OperationStatus::Pending
        );
        let receipt = ledger.receipt(&update.operation.subject).unwrap();
        let compact = CompactOperationCertificate {
            subject: receipt.subject.clone(),
            commitment: receipt.commitment,
            supports: verified[1].certificate().supports.clone(),
        }
        .verify(
            receipt,
            &registry,
            &scope,
            KeysetVersion(1),
            0,
            &EpochConfig::default(),
            0,
        )
        .unwrap();
        assert_eq!(
            operation_status(
                ledger.history(),
                &update.operation.subject,
                &[],
                &[compact],
                |_| true,
            ),
            OperationStatus::Certified
        );
    }
}
