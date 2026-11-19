//! Immutable deployment policy used when admitting an authenticated creation.
//!
//! A declaration contains the resolved mode, rather than the original override
//! request. A receiver may compare that mode with a range default only while
//! the exact manifest remains pinned for the lifetime of its operation state.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::authority::writer_auth::{SignedBoundOperation, SignedCreation};
use crate::operation::{CertificationMode, Declaration};
use crate::types::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreationPolicyRange {
    pub namespace_id: String,
    /// An empty prefix covers the complete namespace.
    pub key_prefix: String,
    pub default_mode: CertificationMode,
    /// Creators allowed to choose a mode different from the range default.
    pub override_writers: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreationPolicyManifest {
    version: u32,
    ranges: Vec<CreationPolicyRange>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CreationPolicyError {
    #[error("invalid creation policy JSON: {0}")]
    Json(String),
    #[error("unsupported creation policy version")]
    Version,
    #[error("creation policy requires at least one range")]
    Empty,
    #[error("invalid or duplicate creation policy range or override writer")]
    DuplicateOrInvalidRange,
    #[error("creation policy has no range for this namespace and key")]
    NoRange,
    #[error("creation mode differs from range default without creator override permission")]
    OverrideDenied,
    #[error("creation policy digest differs from the local manifest")]
    DigestMismatch,
}

impl CreationPolicyManifest {
    pub fn from_json(json: &str) -> Result<Self, CreationPolicyError> {
        let mut manifest: Self =
            serde_json::from_str(json).map_err(|e| CreationPolicyError::Json(e.to_string()))?;
        if manifest.version != 1 {
            return Err(CreationPolicyError::Version);
        }
        if manifest.ranges.is_empty() {
            return Err(CreationPolicyError::Empty);
        }
        manifest.ranges.sort_by(|a, b| {
            (&a.namespace_id, &a.key_prefix).cmp(&(&b.namespace_id, &b.key_prefix))
        });
        let mut prior: Option<(String, String)> = None;
        for range in &mut manifest.ranges {
            if range.namespace_id.is_empty()
                || range
                    .override_writers
                    .iter()
                    .any(|writer| writer.0.is_empty())
                || range.override_writers.iter().collect::<BTreeSet<_>>().len()
                    != range.override_writers.len()
            {
                return Err(CreationPolicyError::DuplicateOrInvalidRange);
            }
            let identity = (range.namespace_id.clone(), range.key_prefix.clone());
            if prior.as_ref() == Some(&identity) {
                return Err(CreationPolicyError::DuplicateOrInvalidRange);
            }
            prior = Some(identity);
            range.override_writers.sort();
        }
        Ok(manifest)
    }

    /// The canonical digest is pinned before the first operation is stored.
    /// Reordering ranges or override writers does not change its identity.
    pub fn digest(&self) -> [u8; 32] {
        let bytes = serde_json::to_vec(self).expect("validated policy serializes");
        let mut digest = Sha256::new();
        digest.update(b"asteroidb/creation-policy-manifest/v1");
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
        digest.finalize().into()
    }

    /// Check the authenticated creation, not the current update/delete writer.
    /// Signature verification must precede this call. Pinned deployments may
    /// check the exact historical creation against the same immutable policy.
    pub fn validate_creation(
        &self,
        signed: &SignedBoundOperation,
    ) -> Result<(), CreationPolicyError> {
        self.validate_proof(&signed.creation)
    }

    /// Check a verified, value-free Create proof against this exact manifest.
    pub fn validate_proof(&self, proof: &SignedCreation) -> Result<(), CreationPolicyError> {
        if proof.creation_policy_digest != self.digest() {
            return Err(CreationPolicyError::DigestMismatch);
        }
        self.validate_declaration(proof.declaration())
    }

    /// Recheck a compacted, authenticated Create declaration against the
    /// pinned policy without retaining its initial value.
    pub fn validate_declaration(&self, creation: &Declaration) -> Result<(), CreationPolicyError> {
        let namespace = &creation.creation_id.namespace_id;
        let key = &creation.key;
        let range = self
            .ranges
            .iter()
            .filter(|range| range.namespace_id == *namespace && key.starts_with(&range.key_prefix))
            .max_by_key(|range| range.key_prefix.len())
            .ok_or(CreationPolicyError::NoRange)?;
        let creator = &creation.creation_id.event_dot.writer;
        if creation.mode != range.default_mode && !range.override_writers.contains(creator) {
            return Err(CreationPolicyError::OverrideDenied);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    use crate::authority::writer_auth::{SignedBoundOperation, SignedCreation};
    use crate::hlc::HlcTimestamp;
    use crate::operation::{
        BoundOperation, CandidateKvAction, CrdtSubject, Declaration, EventDot, OperationLeaf,
    };

    fn policy() -> CreationPolicyManifest {
        CreationPolicyManifest::from_json(
            r#"{"version":1,"ranges":[
                {"namespace_id":"n","key_prefix":"","default_mode":"Disabled","override_writers":[]},
                {"namespace_id":"n","key_prefix":"profile/","default_mode":"Enabled","override_writers":["creator"]},
                {"namespace_id":"n","key_prefix":"profile/private/","default_mode":"Disabled","override_writers":[]}
            ]}"#,
        )
        .unwrap()
    }

    fn signed(
        key: &str,
        mode: CertificationMode,
        creator: &str,
        action: &str,
    ) -> SignedBoundOperation {
        signed_with_policy(&policy(), key, mode, creator, action)
    }

    fn signed_with_policy(
        policy: &CreationPolicyManifest,
        key: &str,
        mode: CertificationMode,
        creator: &str,
        action: &str,
    ) -> SignedBoundOperation {
        let creator_key = SigningKey::from_bytes(&[1; 32]);
        let writer_key = SigningKey::from_bytes(&[2; 32]);
        let subject = CrdtSubject {
            namespace_id: "n".into(),
            event_dot: EventDot {
                writer: NodeId(creator.into()),
                counter: 1,
            },
        };
        let create = BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: key.into(),
                    value_bytes: b"initial".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 1,
                        logical: 0,
                        node_id: creator.into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject,
                key: key.into(),
                mode,
            },
        };
        let creation =
            SignedCreation::sign_with_policy_digest(&create, &creator_key, policy.digest())
                .unwrap();
        if action == "create" {
            SignedBoundOperation::sign(create, &creator_key, creation).unwrap()
        } else {
            let mut later = create;
            later.operation.subject.event_dot.writer = NodeId("other-writer".into());
            later.operation.subject.event_dot.counter = 2;
            later.operation.action = if action == "delete" {
                CandidateKvAction::Delete { key: key.into() }
            } else {
                CandidateKvAction::Add {
                    key: key.into(),
                    value_bytes: b"later".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 2,
                        logical: 0,
                        node_id: "other-writer".into(),
                    },
                }
            };
            SignedBoundOperation::sign(later, &writer_key, creation).unwrap()
        }
    }

    #[test]
    fn selects_longest_prefix_and_checks_creator_override() {
        let policy = policy();
        assert!(
            policy
                .validate_creation(&signed(
                    "other",
                    CertificationMode::Disabled,
                    "any",
                    "create"
                ))
                .is_ok()
        );
        assert!(
            policy
                .validate_creation(&signed(
                    "profile/a",
                    CertificationMode::Enabled,
                    "any",
                    "create"
                ))
                .is_ok()
        );
        assert!(
            policy
                .validate_creation(&signed(
                    "profile/a",
                    CertificationMode::Disabled,
                    "creator",
                    "create"
                ))
                .is_ok()
        );
        assert_eq!(
            policy.validate_creation(&signed(
                "profile/a",
                CertificationMode::Disabled,
                "unprivileged",
                "create"
            )),
            Err(CreationPolicyError::OverrideDenied)
        );
        assert_eq!(
            policy.validate_creation(&signed(
                "profile/private/a",
                CertificationMode::Enabled,
                "creator",
                "create"
            )),
            Err(CreationPolicyError::OverrideDenied)
        );
    }

    #[test]
    fn later_actions_use_signed_creation_and_missing_range_fails_closed() {
        let policy = policy();
        for action in ["add", "delete"] {
            assert_eq!(
                policy.validate_creation(&signed(
                    "profile/a",
                    CertificationMode::Disabled,
                    "unprivileged",
                    action
                )),
                Err(CreationPolicyError::OverrideDenied)
            );
            assert!(
                policy
                    .validate_creation(&signed(
                        "profile/a",
                        CertificationMode::Disabled,
                        "creator",
                        action
                    ))
                    .is_ok()
            );
        }
        let narrow = CreationPolicyManifest::from_json(
            r#"{"version":1,"ranges":[{"namespace_id":"n","key_prefix":"profile/","default_mode":"Enabled","override_writers":[]}]}"#,
        )
        .unwrap();
        assert_eq!(
            narrow.validate_creation(&signed_with_policy(
                &narrow,
                "other",
                CertificationMode::Enabled,
                "creator",
                "create"
            )),
            Err(CreationPolicyError::NoRange)
        );
    }

    #[test]
    fn exact_manifest_digest_is_required_even_when_both_allow_the_mode() {
        let local = policy();
        let other = CreationPolicyManifest::from_json(
            r#"{"version":1,"ranges":[{"namespace_id":"n","key_prefix":"","default_mode":"Disabled","override_writers":["someone"]}]}"#,
        )
        .unwrap();
        let signed = signed_with_policy(
            &other,
            "other",
            CertificationMode::Disabled,
            "any",
            "create",
        );
        assert_eq!(
            local.validate_creation(&signed),
            Err(CreationPolicyError::DigestMismatch)
        );
        assert_eq!(
            local.validate_proof(&signed.creation),
            Err(CreationPolicyError::DigestMismatch)
        );
    }

    #[test]
    fn duplicate_ranges_and_override_grants_are_rejected() {
        let duplicate = r#"{"version":1,"ranges":[{"namespace_id":"n","key_prefix":"","default_mode":"Enabled","override_writers":[]},{"namespace_id":"n","key_prefix":"","default_mode":"Disabled","override_writers":[]}]}"#;
        assert_eq!(
            CreationPolicyManifest::from_json(duplicate),
            Err(CreationPolicyError::DuplicateOrInvalidRange)
        );
        let duplicate_writer = r#"{"version":1,"ranges":[{"namespace_id":"n","key_prefix":"","default_mode":"Enabled","override_writers":["a","a"]}]}"#;
        assert_eq!(
            CreationPolicyManifest::from_json(duplicate_writer),
            Err(CreationPolicyError::DuplicateOrInvalidRange)
        );
        let differently_ordered = CreationPolicyManifest::from_json(
            r#"{"version":1,"ranges":[{"namespace_id":"n","key_prefix":"profile/private/","default_mode":"Disabled","override_writers":[]},{"namespace_id":"n","key_prefix":"profile/","default_mode":"Enabled","override_writers":["creator"]},{"namespace_id":"n","key_prefix":"","default_mode":"Disabled","override_writers":[]}]}"#,
        )
        .unwrap();
        assert_eq!(differently_ordered.digest(), policy().digest());
    }
}
