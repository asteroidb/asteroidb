//! Candidate selection for EdgeCore ledger value-body reclamation.
//!
//! These functions implement the predicates in Lean's `EdgeCompaction` and
//! `EdgeLiveCompaction`. They do not erase bodies or establish that an external
//! obligation has been discharged. The caller must supply independently
//! established accepted and released ID sets. A returned ID is only a
//! *candidate*: this module only selects ledger bodies. Full operation
//! certificates and outstanding delivery copies can retain the same value
//! bytes after a ledger body becomes a receipt. The signed packet inbox,
//! retry checks, proofs, and transfer catalogs also have independent
//! retention requirements.

use std::collections::BTreeSet;

use crate::operation::{BoundOperation, CandidateKvAction, CrdtSubject, OperationHistory};

/// IDs of accepted, released value bodies for a key with an accepted delete.
///
/// The accepted delete is retained. Pending writes, unreleased writes, and
/// every deletion remain outside the result. This is the conservative
/// `reclaimDeleted` case: the accepted deletion preserves both ordinary and
/// certified value projections even if a write is later replayed.
pub fn deleted_value_candidates(
    history: &OperationHistory,
    namespace_id: &str,
    key: &str,
    accepted: &BTreeSet<CrdtSubject>,
    released: &BTreeSet<CrdtSubject>,
) -> BTreeSet<CrdtSubject> {
    let on_key = |bound: &&BoundOperation| {
        bound.operation.subject.namespace_id == namespace_id && bound.operation.action.key() == key
    };
    if !history.iter().filter(&on_key).any(|bound| {
        matches!(bound.operation.action, CandidateKvAction::Delete { .. })
            && accepted.contains(&bound.operation.subject)
    }) {
        return BTreeSet::new();
    }
    history
        .iter()
        .filter(on_key)
        .filter(|bound| {
            !matches!(bound.operation.action, CandidateKvAction::Delete { .. })
                && accepted.contains(&bound.operation.subject)
                && released.contains(&bound.operation.subject)
        })
        .map(|bound| bound.operation.subject.clone())
        .collect()
}

/// IDs of accepted, released values absorbed by one retained accepted write.
///
/// Dominance uses the same `(HLC, value bytes)` ordering as `operation_view`.
/// Equal values and timestamps are absorbed too; the winner ID stays retained.
/// Missing, deleted, or unaccepted winners yield no candidates. This is the
/// `reclaimDominated` predicate for a certified winner, which preserves both
/// ordinary and certified value projections.
pub fn dominated_value_candidates(
    history: &OperationHistory,
    winner_id: &CrdtSubject,
    accepted: &BTreeSet<CrdtSubject>,
    released: &BTreeSet<CrdtSubject>,
) -> BTreeSet<CrdtSubject> {
    let Some(winner) = history.get(winner_id) else {
        return BTreeSet::new();
    };
    if !accepted.contains(winner_id) {
        return BTreeSet::new();
    }
    let Some(winner_value) = write_value(&winner.operation.action) else {
        return BTreeSet::new();
    };
    history
        .iter()
        .filter(|bound| {
            let id = &bound.operation.subject;
            id != winner_id
                && id.namespace_id == winner_id.namespace_id
                && bound.operation.action.key() == winner.operation.action.key()
                && accepted.contains(id)
                && released.contains(id)
                && write_value(&bound.operation.action).is_some_and(|old| old <= winner_value)
        })
        .map(|bound| bound.operation.subject.clone())
        .collect()
}

fn write_value(action: &CandidateKvAction) -> Option<(&crate::hlc::HlcTimestamp, &[u8])> {
    match action {
        CandidateKvAction::Create {
            value_bytes,
            timestamp,
            ..
        }
        | CandidateKvAction::Add {
            value_bytes,
            timestamp,
            ..
        } => Some((timestamp, value_bytes)),
        CandidateKvAction::Delete { .. } => None,
    }
}

#[cfg(all(test, feature = "native-runtime"))]
mod tests {
    use super::*;
    use crate::api::operation_view::{OperationRead, project};
    use crate::hlc::HlcTimestamp;
    use crate::operation::{CertificationMode, Declaration, EventDot, OperationLeaf};
    use crate::types::NodeId;

    fn id(namespace: &str, counter: u64) -> CrdtSubject {
        CrdtSubject {
            namespace_id: namespace.into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter,
            },
        }
    }

    fn add(namespace: &str, key: &str, counter: u64, time: u64, value: &[u8]) -> BoundOperation {
        BoundOperation {
            operation: OperationLeaf {
                subject: id(namespace, counter),
                action: CandidateKvAction::Add {
                    key: key.into(),
                    value_bytes: value.into(),
                    timestamp: HlcTimestamp {
                        physical: time,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: id(namespace, 0),
                key: key.into(),
                mode: CertificationMode::Enabled,
            },
        }
    }

    fn delete(namespace: &str, key: &str, counter: u64) -> BoundOperation {
        let mut bound = add(namespace, key, counter, 0, b"");
        bound.operation.action = CandidateKvAction::Delete { key: key.into() };
        bound
    }

    fn make_history(operations: &[BoundOperation]) -> OperationHistory {
        let mut history = OperationHistory::default();
        for operation in operations {
            history.record(operation.clone()).unwrap();
        }
        history
    }

    fn projected_after(
        history: &OperationHistory,
        removed: &BTreeSet<CrdtSubject>,
        selected: Option<&BTreeSet<CrdtSubject>>,
        namespace: &str,
        key: &str,
    ) -> OperationRead {
        project(
            history.iter().filter(|bound| {
                !removed.contains(&bound.operation.subject)
                    && selected.is_none_or(|ids| ids.contains(&bound.operation.subject))
            }),
            namespace,
            key,
        )
    }

    #[test]
    fn deleted_candidates_require_accepted_delete_and_keep_pending_and_obligated_values() {
        let operations = [
            add("ns", "key", 1, 1, b"old"),
            add("ns", "key", 2, 2, b"pending"),
            add("ns", "key", 3, 3, b"obligated"),
            delete("ns", "key", 4),
            add("ns", "other", 5, 4, b"other"),
            add("else", "key", 6, 5, b"other namespace"),
        ];
        let history = make_history(&operations);
        let accepted = BTreeSet::from([id("ns", 1), id("ns", 3)]);
        let released = BTreeSet::from([id("ns", 1), id("ns", 2), id("ns", 4)]);
        assert!(deleted_value_candidates(&history, "ns", "key", &accepted, &released).is_empty());
        let accepted = BTreeSet::from([
            id("ns", 1),
            id("ns", 3),
            id("ns", 4),
            id("ns", 5),
            id("else", 6),
        ]);
        let released = BTreeSet::from([
            id("ns", 1),
            id("ns", 2),
            id("ns", 4),
            id("ns", 5),
            id("else", 6),
        ]);
        let candidates = deleted_value_candidates(&history, "ns", "key", &accepted, &released);
        assert_eq!(candidates, BTreeSet::from([id("ns", 1)]));
        assert_eq!(
            projected_after(&history, &BTreeSet::new(), None, "ns", "key"),
            projected_after(&history, &candidates, None, "ns", "key")
        );
        assert_eq!(
            projected_after(&history, &BTreeSet::new(), Some(&accepted), "ns", "key"),
            projected_after(&history, &candidates, Some(&accepted), "ns", "key")
        );
        // Even a later replay of the removed value cannot revive the key.
        assert_eq!(
            project(
                history
                    .iter()
                    .filter(|bound| !candidates.contains(&bound.operation.subject))
                    .chain(std::iter::once(&operations[0])),
                "ns",
                "key"
            ),
            OperationRead::Deleted
        );
    }

    #[test]
    fn dominated_candidates_use_equal_hlc_value_tie_and_keep_winner_pending_and_deletes() {
        let operations = [
            add("ns", "key", 1, 10, b"a"),
            add("ns", "key", 2, 10, b"b"),
            add("ns", "key", 3, 10, b"c"),
            add("ns", "key", 4, 10, b"b"),
            add("ns", "key", 5, 1, b"pending"),
            add("ns", "key", 6, 1, b"obligated"),
            delete("ns", "key", 7),
            add("ns", "other", 8, 1, b"other"),
        ];
        let history = make_history(&operations);
        let accepted = BTreeSet::from([
            id("ns", 1),
            id("ns", 2),
            id("ns", 3),
            id("ns", 4),
            id("ns", 6),
            id("ns", 7),
        ]);
        let released = BTreeSet::from([
            id("ns", 1),
            id("ns", 2),
            id("ns", 3),
            id("ns", 4),
            id("ns", 5),
            id("ns", 7),
        ]);
        assert!(
            dominated_value_candidates(&history, &id("ns", 5), &accepted, &released).is_empty()
        );
        assert!(
            dominated_value_candidates(&history, &id("ns", 7), &accepted, &released).is_empty()
        );
        let candidates = dominated_value_candidates(&history, &id("ns", 2), &accepted, &released);
        assert_eq!(candidates, BTreeSet::from([id("ns", 1), id("ns", 4)]));
        assert_eq!(
            projected_after(&history, &BTreeSet::new(), None, "ns", "key"),
            projected_after(&history, &candidates, None, "ns", "key")
        );
        assert_eq!(
            projected_after(&history, &BTreeSet::new(), Some(&accepted), "ns", "key"),
            projected_after(&history, &candidates, Some(&accepted), "ns", "key")
        );
        // Without the deletion, the higher equal-HLC value still wins.
        let without_delete = make_history(&operations[..6]);
        let candidates =
            dominated_value_candidates(&without_delete, &id("ns", 2), &accepted, &released);
        assert_eq!(
            projected_after(&without_delete, &BTreeSet::new(), None, "ns", "key"),
            projected_after(&without_delete, &candidates, None, "ns", "key")
        );
        assert_eq!(
            projected_after(
                &without_delete,
                &BTreeSet::new(),
                Some(&accepted),
                "ns",
                "key"
            ),
            projected_after(&without_delete, &candidates, Some(&accepted), "ns", "key")
        );
        assert_eq!(
            projected_after(&without_delete, &candidates, None, "ns", "key"),
            OperationRead::Value {
                value_bytes: b"c".to_vec(),
                timestamp: HlcTimestamp {
                    physical: 10,
                    logical: 0,
                    node_id: "writer".into(),
                },
            }
        );
    }

    #[test]
    fn small_state_gc_candidates_preserve_both_read_projections() {
        // Enumerate deletion, equal-HLC byte ties, and all certification and
        // release subsets. This checks the read invariant rather than the
        // candidate predicate's implementation details.
        for shape in 0..64_u32 {
            let operations = (0..3)
                .map(|index| match (shape >> (index * 2)) & 3 {
                    0 => add("ns", "key", index as u64 + 1, 1, &[1]),
                    1 => add("ns", "key", index as u64 + 1, 1, &[1, 0]),
                    2 => add("ns", "key", index as u64 + 1, 1, &[2]),
                    _ => delete("ns", "key", index as u64 + 1),
                })
                .collect::<Vec<_>>();
            let history = make_history(&operations);
            for accepted_mask in 0..8_u32 {
                let accepted = (0..3)
                    .filter(|index| accepted_mask & (1 << index) != 0)
                    .map(|index| id("ns", index + 1))
                    .collect::<BTreeSet<_>>();
                for released_mask in 0..8_u32 {
                    let released = (0..3)
                        .filter(|index| released_mask & (1 << index) != 0)
                        .map(|index| id("ns", index + 1))
                        .collect::<BTreeSet<_>>();
                    let mut candidates = vec![deleted_value_candidates(
                        &history, "ns", "key", &accepted, &released,
                    )];
                    for winner in 1..=3 {
                        candidates.push(dominated_value_candidates(
                            &history,
                            &id("ns", winner),
                            &accepted,
                            &released,
                        ));
                    }
                    for removed in candidates {
                        for selected in [None, Some(&accepted)] {
                            assert_eq!(
                                projected_after(&history, &BTreeSet::new(), selected, "ns", "key",),
                                projected_after(&history, &removed, selected, "ns", "key"),
                                "shape={shape}, accepted={accepted_mask}, released={released_mask}, removed={removed:?}",
                            );
                        }
                    }
                }
            }
        }
    }
}
