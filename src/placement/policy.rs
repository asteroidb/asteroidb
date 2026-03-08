use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::node::Node;
use crate::placement::latency::LatencyModel;
use crate::types::{KeyRange, NodeId, NodeMode, PolicyVersion, Tag};

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
    /// Optional latency constraint for replica selection (milliseconds).
    /// When set, only nodes within this RTT of the reference node are eligible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_read_latency_ms: Option<f64>,
    /// Soft preference for a cost tier tag value (e.g., `"low"`).
    /// Nodes with a `cost:` tag matching this value are ranked higher.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_cost_tier: Option<String>,
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
            max_read_latency_ms: None,
            preferred_cost_tier: None,
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

    /// Sets the maximum read latency constraint in milliseconds (builder pattern).
    pub fn with_max_read_latency_ms(mut self, max_ms: f64) -> Self {
        self.max_read_latency_ms = Some(max_ms);
        self
    }

    /// Sets the preferred cost tier (builder pattern).
    pub fn with_preferred_cost_tier(mut self, tier: String) -> Self {
        self.preferred_cost_tier = Some(tier);
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

    /// Returns nodes from the given slice that match this policy, up to
    /// `replica_count`. If fewer eligible nodes exist, returns all of them.
    pub fn select_nodes<'a>(&self, nodes: &'a [Node]) -> Vec<&'a Node> {
        nodes
            .iter()
            .filter(|n| self.matches_node(n))
            .take(self.replica_count)
            .collect()
    }

    /// Returns `true` if the number of matching nodes is at least
    /// `replica_count`.
    pub fn is_satisfied(&self, nodes: &[Node]) -> bool {
        self.select_nodes(nodes).len() >= self.replica_count
    }

    /// Returns `true` if the given node satisfies tag matching **and**
    /// the optional latency constraint relative to `reference_node`.
    ///
    /// If `max_read_latency_ms` is `None`, this is equivalent to
    /// `matches_node`. When set, the latency model must contain a
    /// measurement from `reference_node` to `node` that is within
    /// the threshold. Nodes without measurements are excluded when a
    /// latency constraint is present.
    pub fn matches_node_with_latency(
        &self,
        node: &Node,
        latency_model: &LatencyModel,
        reference_node: &NodeId,
    ) -> bool {
        if !self.matches_node(node) {
            return false;
        }
        if let Some(max_ms) = self.max_read_latency_ms {
            match latency_model.get_latency(reference_node, &node.id) {
                Some(stats) => stats.avg_ms <= max_ms,
                // No measurement available — cannot verify constraint.
                None => {
                    // If the reference node is the same node, latency is 0.
                    *reference_node == node.id
                }
            }
        } else {
            true
        }
    }

    /// Select matching nodes and rank them by:
    /// 1. Cost tier preference (preferred tier first).
    /// 2. Latency (lower is better).
    ///
    /// Nodes that do not pass `matches_node_with_latency` are excluded.
    pub fn select_nodes_ranked<'a>(
        &self,
        nodes: &'a [Node],
        latency_model: &LatencyModel,
        reference_node: &NodeId,
    ) -> Vec<&'a Node> {
        let cost_tag_prefix = "cost:";

        let mut candidates: Vec<(&Node, bool, f64)> = nodes
            .iter()
            .filter(|n| self.matches_node_with_latency(n, latency_model, reference_node))
            .map(|n| {
                // Check cost tier preference.
                let preferred = if let Some(ref tier) = self.preferred_cost_tier {
                    let expected_tag = Tag(format!("{cost_tag_prefix}{tier}"));
                    n.has_tag(&expected_tag)
                } else {
                    false
                };

                // Get latency to this node.
                let latency = latency_model
                    .get_latency(reference_node, &n.id)
                    .map(|s| s.avg_ms)
                    .unwrap_or(if *reference_node == n.id {
                        0.0
                    } else {
                        f64::MAX
                    });

                (n, preferred, latency)
            })
            .collect();

        // Sort: preferred cost tier first, then by latency ascending.
        candidates.sort_by(|a, b| {
            // preferred=true sorts before preferred=false
            b.1.cmp(&a.1)
                .then_with(|| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal))
        });

        candidates
            .into_iter()
            .map(|(n, _, _)| n)
            .take(self.replica_count)
            .collect()
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

    // --- Latency-constrained matching ---

    #[test]
    fn matches_node_with_latency_no_constraint() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1);
        let n = node("n1", NodeMode::Store, &["dc:tokyo"]);
        let model = crate::placement::latency::LatencyModel::new();
        // No latency constraint → falls back to tag-only matching.
        assert!(p.matches_node_with_latency(&n, &model, &NodeId("ref".into())));
    }

    #[test]
    fn matches_node_with_latency_within_bound() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1)
            .with_max_read_latency_ms(50.0);
        let n = node("n1", NodeMode::Store, &[]);
        let mut model = crate::placement::latency::LatencyModel::new();
        model.update_latency(&NodeId("ref".into()), &NodeId("n1".into()), 30.0, 1000);
        assert!(p.matches_node_with_latency(&n, &model, &NodeId("ref".into())));
    }

    #[test]
    fn matches_node_with_latency_exceeds_bound() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1)
            .with_max_read_latency_ms(20.0);
        let n = node("n1", NodeMode::Store, &[]);
        let mut model = crate::placement::latency::LatencyModel::new();
        model.update_latency(&NodeId("ref".into()), &NodeId("n1".into()), 30.0, 1000);
        assert!(!p.matches_node_with_latency(&n, &model, &NodeId("ref".into())));
    }

    #[test]
    fn matches_node_with_latency_no_measurement() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1)
            .with_max_read_latency_ms(50.0);
        let n = node("n1", NodeMode::Store, &[]);
        let model = crate::placement::latency::LatencyModel::new();
        // No measurement and not self → excluded.
        assert!(!p.matches_node_with_latency(&n, &model, &NodeId("ref".into())));
    }

    #[test]
    fn matches_node_with_latency_self_reference() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 1)
            .with_max_read_latency_ms(50.0);
        let n = node("n1", NodeMode::Store, &[]);
        let model = crate::placement::latency::LatencyModel::new();
        // Reference is the node itself → latency is 0.
        assert!(p.matches_node_with_latency(&n, &model, &NodeId("n1".into())));
    }

    // --- Cost-tier ranking ---

    #[test]
    fn select_nodes_ranked_by_cost_tier() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
            .with_preferred_cost_tier("low".to_string());
        let nodes = vec![
            node("n1", NodeMode::Store, &["cost:high"]),
            node("n2", NodeMode::Store, &["cost:low"]),
            node("n3", NodeMode::Store, &["cost:low"]),
        ];
        let model = crate::placement::latency::LatencyModel::new();
        let ranked = p.select_nodes_ranked(&nodes, &model, &NodeId("ref".into()));

        // Preferred ("low") nodes should come first.
        assert_eq!(ranked[0].id, NodeId("n2".into()));
        assert_eq!(ranked[1].id, NodeId("n3".into()));
        assert_eq!(ranked[2].id, NodeId("n1".into()));
    }

    #[test]
    fn select_nodes_ranked_by_latency() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3);
        let nodes = vec![
            node("n1", NodeMode::Store, &[]),
            node("n2", NodeMode::Store, &[]),
            node("n3", NodeMode::Store, &[]),
        ];
        let mut model = crate::placement::latency::LatencyModel::new();
        model.update_latency(&NodeId("ref".into()), &NodeId("n1".into()), 50.0, 1000);
        model.update_latency(&NodeId("ref".into()), &NodeId("n2".into()), 10.0, 1000);
        model.update_latency(&NodeId("ref".into()), &NodeId("n3".into()), 30.0, 1000);

        let ranked = p.select_nodes_ranked(&nodes, &model, &NodeId("ref".into()));
        assert_eq!(ranked[0].id, NodeId("n2".into())); // 10ms
        assert_eq!(ranked[1].id, NodeId("n3".into())); // 30ms
        assert_eq!(ranked[2].id, NodeId("n1".into())); // 50ms
    }

    #[test]
    fn select_nodes_ranked_cost_then_latency() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
            .with_preferred_cost_tier("low".to_string());
        let nodes = vec![
            node("n1", NodeMode::Store, &["cost:high"]),
            node("n2", NodeMode::Store, &["cost:low"]),
            node("n3", NodeMode::Store, &["cost:low"]),
        ];
        let mut model = crate::placement::latency::LatencyModel::new();
        model.update_latency(&NodeId("ref".into()), &NodeId("n1".into()), 5.0, 1000);
        model.update_latency(&NodeId("ref".into()), &NodeId("n2".into()), 30.0, 1000);
        model.update_latency(&NodeId("ref".into()), &NodeId("n3".into()), 10.0, 1000);

        let ranked = p.select_nodes_ranked(&nodes, &model, &NodeId("ref".into()));
        // Cost tier preferred first: n3 (low, 10ms), n2 (low, 30ms), n1 (high, 5ms)
        assert_eq!(ranked[0].id, NodeId("n3".into()));
        assert_eq!(ranked[1].id, NodeId("n2".into()));
        assert_eq!(ranked[2].id, NodeId("n1".into()));
    }

    #[test]
    fn select_nodes_ranked_with_latency_constraint() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 2)
            .with_max_read_latency_ms(25.0);
        let nodes = vec![
            node("n1", NodeMode::Store, &[]),
            node("n2", NodeMode::Store, &[]),
            node("n3", NodeMode::Store, &[]),
        ];
        let mut model = crate::placement::latency::LatencyModel::new();
        model.update_latency(&NodeId("ref".into()), &NodeId("n1".into()), 10.0, 1000);
        model.update_latency(&NodeId("ref".into()), &NodeId("n2".into()), 50.0, 1000);
        model.update_latency(&NodeId("ref".into()), &NodeId("n3".into()), 20.0, 1000);

        let ranked = p.select_nodes_ranked(&nodes, &model, &NodeId("ref".into()));
        // n2 excluded (50ms > 25ms)
        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].id, NodeId("n1".into()));
        assert_eq!(ranked[1].id, NodeId("n3".into()));
    }

    // --- Backward compatibility ---

    #[test]
    fn backward_compat_policy_without_new_fields() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 2)
            .with_required_tags([tag("dc:tokyo")].into());

        let nodes = vec![
            node("n1", NodeMode::Store, &["dc:tokyo"]),
            node("n2", NodeMode::Store, &["dc:tokyo"]),
        ];

        // Original select_nodes still works.
        assert_eq!(p.select_nodes(&nodes).len(), 2);
        assert!(p.is_satisfied(&nodes));

        // New methods also work without latency data.
        let model = crate::placement::latency::LatencyModel::new();
        let ranked = p.select_nodes_ranked(&nodes, &model, &NodeId("ref".into()));
        assert_eq!(ranked.len(), 2);
    }

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
    fn serde_round_trip_with_new_fields() {
        let p = PlacementPolicy::new(PolicyVersion(1), key_range("data/"), 2)
            .with_max_read_latency_ms(50.0)
            .with_preferred_cost_tier("low".to_string());

        let json = serde_json::to_string(&p).unwrap();
        let back: PlacementPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(back.max_read_latency_ms, Some(50.0));
        assert_eq!(back.preferred_cost_tier, Some("low".to_string()));
    }

    #[test]
    fn serde_backward_compat_missing_new_fields() {
        // Simulate JSON from an older version without the new fields.
        let json = r#"{
            "version": 1,
            "key_range": {"prefix": "user/"},
            "replica_count": 3,
            "required_tags": [],
            "forbidden_tags": [],
            "allow_local_write_on_partition": false,
            "certified": false
        }"#;
        let p: PlacementPolicy = serde_json::from_str(json).unwrap();
        assert_eq!(p.max_read_latency_ms, None);
        assert_eq!(p.preferred_cost_tier, None);
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
