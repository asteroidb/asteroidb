use std::collections::HashSet;

use crate::node::Node;
use crate::placement::PlacementPolicy;
use crate::types::{KeyRange, NodeId};

/// A planned data migration triggered by a placement policy change.
///
/// Given an old policy, a new policy, the current cluster nodes, and the
/// set of keys currently stored on each node, this plan describes which
/// keys must be copied to new target nodes (additions) and which keys
/// should be removed from nodes that no longer match the policy (removals).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalancePlan {
    /// The key range this plan applies to.
    pub key_range: KeyRange,
    /// Keys that need to be pushed to new target nodes.
    pub additions: Vec<RebalanceAddition>,
    /// Keys that should be removed from nodes that no longer match.
    pub removals: Vec<RebalanceRemoval>,
}

/// A single key that needs to be migrated to a new target node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceAddition {
    pub key: String,
    pub target_node: NodeId,
}

/// A single key that should be removed from a node no longer matching the policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceRemoval {
    pub key: String,
    pub source_node: NodeId,
}

impl RebalancePlan {
    /// Compute a rebalance plan for a policy change.
    ///
    /// # Arguments
    ///
    /// * `old_policy` - The previous placement policy (or `None` for a newly created policy).
    /// * `new_policy` - The updated placement policy.
    /// * `nodes` - All nodes currently in the cluster.
    /// * `current_keys` - Keys in this key range that exist in the local store.
    /// * `local_node_id` - The ID of the node computing this plan.
    ///
    /// The plan computes:
    /// - **Additions**: for each key, any node that matches the new policy but
    ///   did NOT match the old policy is a target for migration.
    /// - **Removals**: for each key, any node that matched the old policy but
    ///   does NOT match the new policy should have the key removed.
    pub fn compute(
        old_policy: Option<&PlacementPolicy>,
        new_policy: &PlacementPolicy,
        nodes: &[Node],
        current_keys: &[String],
        local_node_id: &NodeId,
    ) -> Self {
        let old_matching: HashSet<&NodeId> = match old_policy {
            Some(old) => old.select_nodes(nodes).into_iter().map(|n| &n.id).collect(),
            None => HashSet::new(),
        };
        let new_matching: HashSet<&NodeId> = new_policy
            .select_nodes(nodes)
            .into_iter()
            .map(|n| &n.id)
            .collect();

        // Nodes that are new targets (in new but not in old).
        let added_nodes: Vec<&NodeId> = new_matching.difference(&old_matching).copied().collect();
        // Nodes that are no longer targets (in old but not in new).
        let removed_nodes: Vec<&NodeId> = old_matching.difference(&new_matching).copied().collect();

        let mut additions = Vec::new();
        let mut removals = Vec::new();

        for key in current_keys {
            // Only push to new nodes from the local node (the node that holds the data).
            for &target in &added_nodes {
                if target != local_node_id {
                    additions.push(RebalanceAddition {
                        key: key.clone(),
                        target_node: target.clone(),
                    });
                }
            }

            // Record removals for nodes that no longer match.
            for &source in &removed_nodes {
                removals.push(RebalanceRemoval {
                    key: key.clone(),
                    source_node: source.clone(),
                });
            }
        }

        RebalancePlan {
            key_range: new_policy.key_range.clone(),
            additions,
            removals,
        }
    }

    /// Returns the total number of operations (additions + removals) in this plan.
    pub fn total_operations(&self) -> usize {
        self.additions.len() + self.removals.len()
    }

    /// Returns `true` if this plan has no work to do.
    pub fn is_empty(&self) -> bool {
        self.additions.is_empty() && self.removals.is_empty()
    }

    /// Return a rate-limited slice of additions, up to `max_keys` entries.
    ///
    /// This allows the executor to process migrations in bounded batches
    /// to avoid overwhelming the cluster during large rebalance operations.
    pub fn additions_batch(&self, offset: usize, max_keys: usize) -> &[RebalanceAddition] {
        let end = (offset + max_keys).min(self.additions.len());
        if offset >= self.additions.len() {
            return &[];
        }
        &self.additions[offset..end]
    }
}

/// Default maximum number of keys to migrate per sync cycle.
pub const DEFAULT_REBALANCE_BATCH_SIZE: usize = 50;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{KeyRange, NodeMode, PolicyVersion, Tag};
    use std::collections::HashSet;

    fn tag(s: &str) -> Tag {
        Tag(s.into())
    }

    fn key_range(prefix: &str) -> KeyRange {
        KeyRange {
            prefix: prefix.into(),
        }
    }

    fn node(id: &str, mode: NodeMode, tags: &[&str]) -> Node {
        let mut n = Node::new(NodeId(id.into()), mode);
        for t in tags {
            n.add_tag(tag(t));
        }
        n
    }

    fn nid(s: &str) -> NodeId {
        NodeId(s.into())
    }

    // --- RebalancePlan::compute ---

    #[test]
    fn compute_no_change_produces_empty_plan() {
        let policy = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
            .with_required_tags([tag("dc:tokyo")].into());

        let nodes = vec![
            node("n1", NodeMode::Store, &["dc:tokyo"]),
            node("n2", NodeMode::Store, &["dc:tokyo"]),
        ];
        let keys = vec!["user/alice".to_string(), "user/bob".to_string()];

        let plan = RebalancePlan::compute(Some(&policy), &policy, &nodes, &keys, &nid("n1"));

        assert!(plan.is_empty());
        assert_eq!(plan.total_operations(), 0);
    }

    #[test]
    fn compute_new_node_matches_produces_additions() {
        let old_policy = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
            .with_required_tags([tag("dc:tokyo")].into());
        let new_policy = PlacementPolicy::new(PolicyVersion(2), key_range("user/"), 3)
            .with_required_tags(HashSet::new()); // now all nodes match

        let nodes = vec![
            node("n1", NodeMode::Store, &["dc:tokyo"]),
            node("n2", NodeMode::Store, &["dc:osaka"]),
        ];
        let keys = vec!["user/alice".to_string()];

        let plan =
            RebalancePlan::compute(Some(&old_policy), &new_policy, &nodes, &keys, &nid("n1"));

        // n2 is new (wasn't matching old policy with dc:tokyo requirement)
        assert_eq!(plan.additions.len(), 1);
        assert_eq!(plan.additions[0].key, "user/alice");
        assert_eq!(plan.additions[0].target_node, nid("n2"));
        assert!(plan.removals.is_empty());
    }

    #[test]
    fn compute_node_no_longer_matches_produces_removals() {
        let old_policy = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
            .with_required_tags(HashSet::new()); // all nodes match
        let new_policy = PlacementPolicy::new(PolicyVersion(2), key_range("user/"), 3)
            .with_required_tags([tag("dc:tokyo")].into()); // only tokyo nodes

        let nodes = vec![
            node("n1", NodeMode::Store, &["dc:tokyo"]),
            node("n2", NodeMode::Store, &["dc:osaka"]),
        ];
        let keys = vec!["user/alice".to_string()];

        let plan =
            RebalancePlan::compute(Some(&old_policy), &new_policy, &nodes, &keys, &nid("n1"));

        // n2 no longer matches
        assert!(plan.additions.is_empty());
        assert_eq!(plan.removals.len(), 1);
        assert_eq!(plan.removals[0].key, "user/alice");
        assert_eq!(plan.removals[0].source_node, nid("n2"));
    }

    #[test]
    fn compute_new_policy_no_old_treats_all_as_additions() {
        let new_policy = PlacementPolicy::new(PolicyVersion(1), key_range("data/"), 2)
            .with_required_tags([tag("dc:tokyo")].into());

        let nodes = vec![
            node("n1", NodeMode::Store, &["dc:tokyo"]),
            node("n2", NodeMode::Store, &["dc:tokyo"]),
        ];
        let keys = vec!["data/k1".to_string()];

        let plan = RebalancePlan::compute(None, &new_policy, &nodes, &keys, &nid("n1"));

        // n2 should be a target (n1 is local, excluded from additions)
        assert_eq!(plan.additions.len(), 1);
        assert_eq!(plan.additions[0].target_node, nid("n2"));
        assert!(plan.removals.is_empty());
    }

    #[test]
    fn compute_local_node_excluded_from_additions() {
        let new_policy = PlacementPolicy::new(PolicyVersion(1), key_range("data/"), 2);

        let nodes = vec![
            node("local", NodeMode::Store, &[]),
            node("remote", NodeMode::Store, &[]),
        ];
        let keys = vec!["data/k1".to_string()];

        let plan = RebalancePlan::compute(None, &new_policy, &nodes, &keys, &nid("local"));

        // Only remote should appear as target, not local
        assert_eq!(plan.additions.len(), 1);
        assert_eq!(plan.additions[0].target_node, nid("remote"));
    }

    #[test]
    fn compute_multiple_keys_multiple_nodes() {
        let old_policy = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
            .with_required_tags([tag("dc:tokyo")].into());
        let new_policy = PlacementPolicy::new(PolicyVersion(2), key_range("user/"), 3)
            .with_required_tags(HashSet::new());

        let nodes = vec![
            node("n1", NodeMode::Store, &["dc:tokyo"]),
            node("n2", NodeMode::Store, &["dc:osaka"]),
            node("n3", NodeMode::Store, &["dc:singapore"]),
        ];
        let keys = vec!["user/alice".to_string(), "user/bob".to_string()];

        let plan =
            RebalancePlan::compute(Some(&old_policy), &new_policy, &nodes, &keys, &nid("n1"));

        // n2 and n3 are new targets, 2 keys each = 4 additions
        assert_eq!(plan.additions.len(), 4);
        assert!(plan.removals.is_empty());
    }

    #[test]
    fn compute_forbidden_tag_change() {
        let old_policy = PlacementPolicy::new(PolicyVersion(1), key_range("data/"), 3);
        let new_policy = PlacementPolicy::new(PolicyVersion(2), key_range("data/"), 3)
            .with_forbidden_tags([tag("decommissioned")].into());

        let nodes = vec![
            node("n1", NodeMode::Store, &[]),
            node("n2", NodeMode::Store, &["decommissioned"]),
        ];
        let keys = vec!["data/k1".to_string()];

        let plan =
            RebalancePlan::compute(Some(&old_policy), &new_policy, &nodes, &keys, &nid("n1"));

        // n2 should be removed (now forbidden)
        assert!(plan.additions.is_empty());
        assert_eq!(plan.removals.len(), 1);
        assert_eq!(plan.removals[0].source_node, nid("n2"));
    }

    // --- RebalancePlan methods ---

    #[test]
    fn total_operations_and_is_empty() {
        let empty = RebalancePlan {
            key_range: key_range("test/"),
            additions: vec![],
            removals: vec![],
        };
        assert_eq!(empty.total_operations(), 0);
        assert!(empty.is_empty());

        let non_empty = RebalancePlan {
            key_range: key_range("test/"),
            additions: vec![RebalanceAddition {
                key: "k1".into(),
                target_node: nid("n1"),
            }],
            removals: vec![RebalanceRemoval {
                key: "k2".into(),
                source_node: nid("n2"),
            }],
        };
        assert_eq!(non_empty.total_operations(), 2);
        assert!(!non_empty.is_empty());
    }

    // --- Rate limiting (additions_batch) ---

    #[test]
    fn additions_batch_returns_correct_slice() {
        let plan = RebalancePlan {
            key_range: key_range("data/"),
            additions: (0..10)
                .map(|i| RebalanceAddition {
                    key: format!("data/k{i}"),
                    target_node: nid("n1"),
                })
                .collect(),
            removals: vec![],
        };

        let batch1 = plan.additions_batch(0, 3);
        assert_eq!(batch1.len(), 3);
        assert_eq!(batch1[0].key, "data/k0");
        assert_eq!(batch1[2].key, "data/k2");

        let batch2 = plan.additions_batch(3, 3);
        assert_eq!(batch2.len(), 3);
        assert_eq!(batch2[0].key, "data/k3");

        let batch_end = plan.additions_batch(8, 5);
        assert_eq!(batch_end.len(), 2); // only 2 remaining

        let batch_past = plan.additions_batch(10, 5);
        assert!(batch_past.is_empty());
    }

    #[test]
    fn additions_batch_empty_plan() {
        let plan = RebalancePlan {
            key_range: key_range("data/"),
            additions: vec![],
            removals: vec![],
        };

        assert!(plan.additions_batch(0, 10).is_empty());
    }
}
