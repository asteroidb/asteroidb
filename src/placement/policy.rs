use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::node::Node;
use crate::types::{KeyRange, NodeMode, PolicyVersion, Tag};

/// Placement policy for data within a key range (FR-007).
///
/// Controls replica count, tag-based node selection, partition behaviour,
/// and whether the key range requires certification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementPolicy {
    pub version: PolicyVersion,
    pub key_range: KeyRange,
    pub replica_count: usize,
    pub required_tags: HashSet<Tag>,
    pub forbidden_tags: HashSet<Tag>,
    pub allow_local_write_on_partition: bool,
    pub certified: bool,
}

impl PlacementPolicy {
    /// Creates a new placement policy with the given version, key range, and
    /// replica count. Required/forbidden tags default to empty, local write on
    /// partition defaults to `false`, and certified defaults to `false`.
    pub fn new(version: PolicyVersion, key_range: KeyRange, replica_count: usize) -> Self {
        Self {
            version,
            key_range,
            replica_count,
            required_tags: HashSet::new(),
            forbidden_tags: HashSet::new(),
            allow_local_write_on_partition: false,
            certified: false,
        }
    }

    /// Sets the required tags for this policy (builder pattern).
    pub fn with_required_tags(mut self, tags: HashSet<Tag>) -> Self {
        self.required_tags = tags;
        self
    }

    /// Sets the forbidden tags for this policy (builder pattern).
    pub fn with_forbidden_tags(mut self, tags: HashSet<Tag>) -> Self {
        self.forbidden_tags = tags;
        self
    }

    /// Sets whether local writes are allowed during network partitions
    /// (builder pattern).
    pub fn with_local_write_on_partition(mut self, allow: bool) -> Self {
        self.allow_local_write_on_partition = allow;
        self
    }

    /// Sets whether this key range requires certification (builder pattern).
    pub fn with_certified(mut self, certified: bool) -> Self {
        self.certified = certified;
        self
    }

    /// Returns `true` if the given node satisfies this policy.
    ///
    /// A node matches when:
    /// 1. Its mode is `Store` or `Both` (subscribe-only nodes cannot hold data).
    /// 2. It has **all** required tags.
    /// 3. It has **none** of the forbidden tags.
    pub fn matches_node(&self, node: &Node) -> bool {
        if node.mode == NodeMode::Subscribe {
            return false;
        }
        if !node.has_all_tags(&self.required_tags) {
            return false;
        }
        if !self.forbidden_tags.is_empty() && node.has_any_tag(&self.forbidden_tags) {
            return false;
        }
        true
    }

    /// Returns all nodes from the given slice that match this policy.
    pub fn select_nodes<'a>(&self, nodes: &'a [Node]) -> Vec<&'a Node> {
        nodes.iter().filter(|n| self.matches_node(n)).collect()
    }

    /// Returns `true` if the number of matching nodes is at least
    /// `replica_count`.
    pub fn is_satisfied(&self, nodes: &[Node]) -> bool {
        self.select_nodes(nodes).len() >= self.replica_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeId;

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

    // --- Builder pattern ---

    #[test]
    fn builder_defaults() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3);
        assert_eq!(p.version, PolicyVersion(1));
        assert_eq!(p.key_range, key_range("user/"));
        assert_eq!(p.replica_count, 3);
        assert!(p.required_tags.is_empty());
        assert!(p.forbidden_tags.is_empty());
        assert!(!p.allow_local_write_on_partition);
        assert!(!p.certified);
    }

    #[test]
    fn builder_with_required_tags() {
        let tags: HashSet<Tag> = [tag("dc:tokyo")].into();
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
            .with_required_tags(tags.clone());
        assert_eq!(p.required_tags, tags);
    }

    #[test]
    fn builder_with_forbidden_tags() {
        let tags: HashSet<Tag> = [tag("decommissioned")].into();
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
            .with_forbidden_tags(tags.clone());
        assert_eq!(p.forbidden_tags, tags);
    }

    #[test]
    fn builder_with_local_write_on_partition() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
            .with_local_write_on_partition(true);
        assert!(p.allow_local_write_on_partition);
    }

    #[test]
    fn builder_with_certified() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3).with_certified(true);
        assert!(p.certified);
    }

    #[test]
    fn builder_chaining() {
        let p = PlacementPolicy::new(PolicyVersion(2), key_range("order/"), 5)
            .with_required_tags([tag("dc:tokyo")].into())
            .with_forbidden_tags([tag("decommissioned")].into())
            .with_local_write_on_partition(true)
            .with_certified(true);
        assert_eq!(p.version, PolicyVersion(2));
        assert_eq!(p.replica_count, 5);
        assert!(p.required_tags.contains(&tag("dc:tokyo")));
        assert!(p.forbidden_tags.contains(&tag("decommissioned")));
        assert!(p.allow_local_write_on_partition);
        assert!(p.certified);
    }

    // --- matches_node ---

    #[test]
    fn matches_node_with_required_tags() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1)
            .with_required_tags([tag("dc:tokyo")].into());
        let n = node("n1", NodeMode::Store, &["dc:tokyo", "rack:a1"]);
        assert!(p.matches_node(&n));
    }

    #[test]
    fn matches_node_missing_required_tag() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1)
            .with_required_tags([tag("dc:tokyo")].into());
        let n = node("n1", NodeMode::Store, &["dc:osaka"]);
        assert!(!p.matches_node(&n));
    }

    #[test]
    fn matches_node_with_forbidden_tag() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1)
            .with_forbidden_tags([tag("decommissioned")].into());
        let n = node("n1", NodeMode::Store, &["dc:tokyo", "decommissioned"]);
        assert!(!p.matches_node(&n));
    }

    #[test]
    fn matches_node_subscribe_mode_rejected() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1);
        let n = node("n1", NodeMode::Subscribe, &["dc:tokyo"]);
        assert!(!p.matches_node(&n));
    }

    #[test]
    fn matches_node_store_mode_accepted() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1);
        let n = node("n1", NodeMode::Store, &[]);
        assert!(p.matches_node(&n));
    }

    #[test]
    fn matches_node_both_mode_accepted() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1);
        let n = node("n1", NodeMode::Both, &[]);
        assert!(p.matches_node(&n));
    }

    #[test]
    fn matches_node_no_required_no_forbidden() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1);
        let n = node("n1", NodeMode::Store, &["anything"]);
        assert!(p.matches_node(&n));
    }

    #[test]
    fn matches_node_required_and_forbidden_combined() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1)
            .with_required_tags([tag("dc:tokyo")].into())
            .with_forbidden_tags([tag("decommissioned")].into());

        // Has required, no forbidden → match
        let n1 = node("n1", NodeMode::Store, &["dc:tokyo"]);
        assert!(p.matches_node(&n1));

        // Has required AND forbidden → no match
        let n2 = node("n2", NodeMode::Store, &["dc:tokyo", "decommissioned"]);
        assert!(!p.matches_node(&n2));

        // Missing required → no match
        let n3 = node("n3", NodeMode::Store, &["dc:osaka"]);
        assert!(!p.matches_node(&n3));
    }

    // --- select_nodes ---

    #[test]
    fn select_nodes_filters_correctly() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 2)
            .with_required_tags([tag("dc:tokyo")].into())
            .with_forbidden_tags([tag("decommissioned")].into());

        let nodes = vec![
            node("n1", NodeMode::Store, &["dc:tokyo"]),     // match
            node("n2", NodeMode::Store, &["dc:osaka"]),     // no required tag
            node("n3", NodeMode::Subscribe, &["dc:tokyo"]), // wrong mode
            node("n4", NodeMode::Both, &["dc:tokyo"]),      // match
            node("n5", NodeMode::Store, &["dc:tokyo", "decommissioned"]), // forbidden
        ];

        let selected = p.select_nodes(&nodes);
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].id, NodeId("n1".into()));
        assert_eq!(selected[1].id, NodeId("n4".into()));
    }

    #[test]
    fn select_nodes_empty_list() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1);
        let nodes: Vec<Node> = vec![];
        let selected = p.select_nodes(&nodes);
        assert!(selected.is_empty());
    }

    // --- is_satisfied ---

    #[test]
    fn is_satisfied_enough_nodes() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 2)
            .with_required_tags([tag("dc:tokyo")].into());

        let nodes = vec![
            node("n1", NodeMode::Store, &["dc:tokyo"]),
            node("n2", NodeMode::Both, &["dc:tokyo"]),
            node("n3", NodeMode::Store, &["dc:osaka"]),
        ];

        assert!(p.is_satisfied(&nodes));
    }

    #[test]
    fn is_satisfied_not_enough_nodes() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
            .with_required_tags([tag("dc:tokyo")].into());

        let nodes = vec![
            node("n1", NodeMode::Store, &["dc:tokyo"]),
            node("n2", NodeMode::Store, &["dc:osaka"]),
        ];

        assert!(!p.is_satisfied(&nodes));
    }

    #[test]
    fn is_satisfied_exact_count() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 2);
        let nodes = vec![
            node("n1", NodeMode::Store, &[]),
            node("n2", NodeMode::Both, &[]),
        ];
        assert!(p.is_satisfied(&nodes));
    }

    #[test]
    fn is_satisfied_zero_replicas() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 0);
        let nodes: Vec<Node> = vec![];
        assert!(p.is_satisfied(&nodes));
    }

    // --- Serde ---

    #[test]
    fn serde_round_trip() {
        let p = PlacementPolicy::new(PolicyVersion(3), key_range("order/"), 5)
            .with_required_tags([tag("dc:tokyo"), tag("tier:hot")].into())
            .with_forbidden_tags([tag("decommissioned")].into())
            .with_local_write_on_partition(true)
            .with_certified(true);

        let json = serde_json::to_string(&p).unwrap();
        let back: PlacementPolicy = serde_json::from_str(&json).unwrap();

        assert_eq!(back.version, PolicyVersion(3));
        assert_eq!(back.key_range, key_range("order/"));
        assert_eq!(back.replica_count, 5);
        assert!(back.required_tags.contains(&tag("dc:tokyo")));
        assert!(back.required_tags.contains(&tag("tier:hot")));
        assert!(back.forbidden_tags.contains(&tag("decommissioned")));
        assert!(back.allow_local_write_on_partition);
        assert!(back.certified);
    }

    #[test]
    fn serde_round_trip_empty_tags() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("data/"), 1);
        let json = serde_json::to_string(&p).unwrap();
        let back: PlacementPolicy = serde_json::from_str(&json).unwrap();
        assert!(back.required_tags.is_empty());
        assert!(back.forbidden_tags.is_empty());
    }
}
