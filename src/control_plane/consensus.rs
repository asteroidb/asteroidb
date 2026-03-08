use std::collections::HashSet;

use crate::error::CrdtError;
use crate::placement::PlacementPolicy;
use crate::types::NodeId;

use super::system_namespace::AuthorityDefinition;

/// Simulates control-plane Authority consensus for system namespace updates.
/// In MVP, this is a simple majority check (FR-009).
pub struct ControlPlaneConsensus {
    authority_nodes: Vec<NodeId>,
}

impl ControlPlaneConsensus {
    /// Creates a new consensus instance with the given authority nodes.
    pub fn new(authority_nodes: Vec<NodeId>) -> Self {
        Self { authority_nodes }
    }

    /// Validates that a policy update has majority approval.
    pub fn propose_policy_update(
        &self,
        _policy: PlacementPolicy,
        approvals: &[NodeId],
    ) -> Result<(), CrdtError> {
        if !self.has_majority(approvals) {
            return Err(CrdtError::PolicyDenied(
                "insufficient approvals for policy update".into(),
            ));
        }
        Ok(())
    }

    /// Proposes an authority definition update. Applies only if a majority of
    /// authority nodes have approved.
    pub fn propose_authority_update(
        &self,
        _def: AuthorityDefinition,
        approvals: &[NodeId],
    ) -> Result<(), CrdtError> {
        if !self.has_majority(approvals) {
            return Err(CrdtError::PolicyDenied(
                "insufficient approvals for authority update".into(),
            ));
        }
        Ok(())
    }

    /// Proposes a placement policy removal. Removes only if a majority of
    /// authority nodes have approved (FR-009).
    pub fn propose_policy_removal(
        &self,
        _prefix: &str,
        approvals: &[NodeId],
    ) -> Result<(), CrdtError> {
        if !self.has_majority(approvals) {
            return Err(CrdtError::PolicyDenied(
                "insufficient approvals for policy removal".into(),
            ));
        }
        Ok(())
    }

    /// Returns `true` if the given approvals constitute a majority of the
    /// authority nodes. Duplicate approvals from the same node are counted
    /// only once, and approvals from non-authority nodes are ignored.
    pub fn has_majority(&self, approvals: &[NodeId]) -> bool {
        let authority_set: HashSet<&NodeId> = self.authority_nodes.iter().collect();
        let unique_valid: HashSet<&NodeId> = approvals
            .iter()
            .filter(|a| authority_set.contains(a))
            .collect();
        let majority = self.authority_nodes.len() / 2 + 1;
        unique_valid.len() >= majority
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::system_namespace::SystemNamespace;
    use crate::types::{KeyRange, PolicyVersion};

    fn node_id(s: &str) -> NodeId {
        NodeId(s.into())
    }

    fn key_range(prefix: &str) -> KeyRange {
        KeyRange {
            prefix: prefix.into(),
        }
    }

    fn make_policy(prefix: &str) -> PlacementPolicy {
        PlacementPolicy::new(PolicyVersion(1), key_range(prefix), 3)
    }

    fn make_authority_def(prefix: &str, nodes: &[&str]) -> AuthorityDefinition {
        AuthorityDefinition {
            key_range: key_range(prefix),
            authority_nodes: nodes.iter().map(|s| node_id(s)).collect(),
            auto_generated: false,
        }
    }

    fn three_node_consensus() -> ControlPlaneConsensus {
        ControlPlaneConsensus::new(vec![node_id("n1"), node_id("n2"), node_id("n3")])
    }

    // --- has_majority ---

    #[test]
    fn has_majority_with_all_nodes() {
        let c = three_node_consensus();
        assert!(c.has_majority(&[node_id("n1"), node_id("n2"), node_id("n3")]));
    }

    #[test]
    fn has_majority_with_exact_majority() {
        let c = three_node_consensus();
        assert!(c.has_majority(&[node_id("n1"), node_id("n2")]));
    }

    #[test]
    fn no_majority_with_minority() {
        let c = three_node_consensus();
        assert!(!c.has_majority(&[node_id("n1")]));
    }

    #[test]
    fn no_majority_with_empty_approvals() {
        let c = three_node_consensus();
        assert!(!c.has_majority(&[]));
    }

    #[test]
    fn non_authority_nodes_ignored() {
        let c = three_node_consensus();
        // "n4" is not an authority node, so only "n1" counts
        assert!(!c.has_majority(&[node_id("n1"), node_id("n4")]));
    }

    #[test]
    fn duplicate_approvals_counted_once() {
        let c = three_node_consensus();
        // "n1" appears three times but should count as only one unique approval
        assert!(!c.has_majority(&[node_id("n1"), node_id("n1"), node_id("n1")]));
    }

    #[test]
    fn duplicate_approvals_do_not_inflate_quorum() {
        let c = three_node_consensus();
        // Two unique authority nodes (n1, n2) = majority, even with duplicates
        assert!(c.has_majority(&[node_id("n1"), node_id("n2"), node_id("n1"), node_id("n2")]));
        // One unique authority node (n1) repeated != majority
        assert!(!c.has_majority(&[node_id("n1"), node_id("n1")]));
    }

    #[test]
    fn duplicate_non_authority_approvals_ignored() {
        let c = three_node_consensus();
        // "n4" is not authority; even duplicated many times, only n1 counts
        assert!(!c.has_majority(&[node_id("n1"), node_id("n4"), node_id("n4"), node_id("n4")]));
    }

    #[test]
    fn duplicate_approvals_policy_update_rejected() {
        let c = three_node_consensus();
        // Same node duplicated should not reach majority
        let result = c.propose_policy_update(make_policy("user/"), &[node_id("n1"), node_id("n1")]);
        assert!(result.is_err());
    }

    #[test]
    fn duplicate_approvals_authority_update_rejected() {
        let c = three_node_consensus();
        let result = c.propose_authority_update(
            make_authority_def("user/", &["a1"]),
            &[node_id("n1"), node_id("n1")],
        );
        assert!(result.is_err());
    }

    #[test]
    fn has_majority_five_nodes() {
        let c = ControlPlaneConsensus::new(vec![
            node_id("n1"),
            node_id("n2"),
            node_id("n3"),
            node_id("n4"),
            node_id("n5"),
        ]);
        // majority of 5 is 3
        assert!(c.has_majority(&[node_id("n1"), node_id("n2"), node_id("n3")]));
        assert!(!c.has_majority(&[node_id("n1"), node_id("n2")]));
    }

    // --- propose_policy_update ---

    #[test]
    fn propose_policy_with_majority_succeeds() {
        let c = three_node_consensus();
        let result = c.propose_policy_update(make_policy("user/"), &[node_id("n1"), node_id("n2")]);
        assert!(result.is_ok());
    }

    #[test]
    fn propose_policy_without_majority_fails() {
        let c = three_node_consensus();
        let result = c.propose_policy_update(make_policy("user/"), &[node_id("n1")]);
        assert!(result.is_err());
        match result.unwrap_err() {
            CrdtError::PolicyDenied(msg) => {
                assert!(msg.contains("insufficient approvals"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn propose_policy_increments_version() {
        let c = three_node_consensus();
        let approvals = [node_id("n1"), node_id("n2")];
        let mut ns = SystemNamespace::new();
        assert_eq!(*ns.version(), PolicyVersion(1));

        c.propose_policy_update(make_policy("user/"), &approvals)
            .unwrap();
        ns.set_placement_policy(make_policy("user/"));
        assert_eq!(*ns.version(), PolicyVersion(2));

        c.propose_policy_update(make_policy("order/"), &approvals)
            .unwrap();
        ns.set_placement_policy(make_policy("order/"));
        assert_eq!(*ns.version(), PolicyVersion(3));
    }

    // --- propose_authority_update ---

    #[test]
    fn propose_authority_with_majority_succeeds() {
        let c = three_node_consensus();
        let result = c.propose_authority_update(
            make_authority_def("user/", &["a1", "a2", "a3"]),
            &[node_id("n1"), node_id("n2")],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn propose_authority_without_majority_fails() {
        let c = three_node_consensus();
        let result =
            c.propose_authority_update(make_authority_def("user/", &["a1"]), &[node_id("n1")]);
        assert!(result.is_err());
    }

    // --- namespace access ---

    #[test]
    fn namespace_reflects_approved_changes() {
        let c = three_node_consensus();
        let approvals = [node_id("n1"), node_id("n2")];

        let mut ns = SystemNamespace::new();

        c.propose_policy_update(make_policy("user/"), &approvals)
            .unwrap();
        ns.set_placement_policy(make_policy("user/"));

        c.propose_authority_update(make_authority_def("user/", &["a1", "a2"]), &approvals)
            .unwrap();
        ns.set_authority_definition(make_authority_def("user/", &["a1", "a2"]));

        assert_eq!(ns.all_placement_policies().len(), 1);
        assert_eq!(ns.all_authority_definitions().len(), 1);
        assert_eq!(*ns.version(), PolicyVersion(3));
    }

    // --- propose_policy_removal ---

    #[test]
    fn propose_policy_removal_with_majority_succeeds() {
        let c = three_node_consensus();
        let approvals = [node_id("n1"), node_id("n2")];
        c.propose_policy_update(make_policy("user/"), &approvals)
            .unwrap();

        let result = c.propose_policy_removal("user/", &approvals);
        assert!(result.is_ok());
    }

    #[test]
    fn propose_policy_removal_without_majority_fails() {
        let c = three_node_consensus();
        let approvals = [node_id("n1"), node_id("n2")];
        c.propose_policy_update(make_policy("user/"), &approvals)
            .unwrap();

        let result = c.propose_policy_removal("user/", &[node_id("n1")]);
        assert!(result.is_err());
        match result.unwrap_err() {
            CrdtError::PolicyDenied(msg) => {
                assert!(msg.contains("insufficient approvals"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn propose_policy_removal_nonexistent_returns_none() {
        let c = three_node_consensus();
        let approvals = [node_id("n1"), node_id("n2")];
        let result = c.propose_policy_removal("missing/", &approvals);
        assert!(result.is_ok());
    }

    #[test]
    fn failed_proposals_do_not_change_namespace() {
        let c = three_node_consensus();
        let insufficient = [node_id("n1")];

        let _ = c.propose_policy_update(make_policy("user/"), &insufficient);
        let _ = c.propose_authority_update(make_authority_def("user/", &["a1"]), &insufficient);

        let ns = SystemNamespace::new();
        assert!(ns.all_placement_policies().is_empty());
        assert!(ns.all_authority_definitions().is_empty());
        assert_eq!(*ns.version(), PolicyVersion(1));
    }
}
