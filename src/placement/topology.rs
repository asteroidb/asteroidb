use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::node::Node;
use crate::placement::latency::LatencyModel;
use crate::types::NodeId;

/// Tag prefix used to identify the region of a node (e.g., `region:us-east`).
const REGION_TAG_PREFIX: &str = "region:";

/// Information about a single region in the topology.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionInfo {
    /// Region name (the value part of the `region:` tag).
    pub name: String,
    /// Number of nodes in this region.
    pub node_count: usize,
    /// Node IDs belonging to this region.
    pub node_ids: Vec<String>,
    /// Average latency in milliseconds to other regions.
    /// Keys are region names, values are average RTT in ms.
    pub inter_region_latency_ms: HashMap<String, f64>,
}

/// A view of the cluster topology grouped by region, with inter-region
/// latency information.
///
/// Built from the current set of nodes and latency measurements. Intended
/// to be serialized for the `GET /api/topology` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyView {
    /// Regions in the cluster.
    pub regions: Vec<RegionInfo>,
    /// Total number of nodes in the cluster.
    pub total_nodes: usize,
}

impl TopologyView {
    /// Build a topology view from a set of nodes and a latency model.
    ///
    /// Nodes are grouped by their `region:*` tag. Nodes without a region
    /// tag are placed in a synthetic `"unknown"` region.
    pub fn build(nodes: &[Node], latency_model: &LatencyModel) -> Self {
        // Group nodes by region.
        let mut region_nodes: HashMap<String, Vec<&Node>> = HashMap::new();

        for node in nodes {
            let region = extract_region(node).unwrap_or_else(|| "unknown".to_string());
            region_nodes.entry(region).or_default().push(node);
        }

        // Compute inter-region latencies.
        let all_stats = latency_model.all_stats();
        // Build node->region lookup.
        let node_region: HashMap<&NodeId, String> = nodes
            .iter()
            .map(|n| {
                let region = extract_region(n).unwrap_or_else(|| "unknown".to_string());
                (&n.id, region)
            })
            .collect();

        // Aggregate latencies between regions.
        // Key: (from_region, to_region), Value: (sum, count)
        let mut region_latency_agg: HashMap<(String, String), (f64, u64)> = HashMap::new();

        for ((from, to), stats) in &all_stats {
            if let (Some(from_region), Some(to_region)) =
                (node_region.get(from), node_region.get(to))
                && from_region != to_region
            {
                let entry = region_latency_agg
                    .entry((from_region.clone(), to_region.clone()))
                    .or_insert((0.0, 0));
                entry.0 += stats.avg_ms;
                entry.1 += 1;
            }
        }

        let mut regions: Vec<RegionInfo> = region_nodes
            .into_iter()
            .map(|(region_name, nodes_in_region)| {
                let mut node_ids: Vec<String> =
                    nodes_in_region.iter().map(|n| n.id.0.clone()).collect();
                node_ids.sort();

                // Compute inter-region latencies for this region.
                let mut inter_region_latency_ms = HashMap::new();
                for ((from_r, to_r), (sum, count)) in &region_latency_agg {
                    if from_r == &region_name && *count > 0 {
                        inter_region_latency_ms.insert(to_r.clone(), sum / *count as f64);
                    }
                }

                RegionInfo {
                    name: region_name,
                    node_count: nodes_in_region.len(),
                    node_ids,
                    inter_region_latency_ms,
                }
            })
            .collect();

        regions.sort_by(|a, b| a.name.cmp(&b.name));

        TopologyView {
            total_nodes: nodes.len(),
            regions,
        }
    }

    /// Return the list of regions.
    pub fn regions(&self) -> &[RegionInfo] {
        &self.regions
    }
}

/// Extract the region name from a node's tags.
///
/// Looks for a tag matching `region:*` and returns the value part.
/// Returns `None` if no region tag is found.
fn extract_region(node: &Node) -> Option<String> {
    node.tags
        .iter()
        .find(|t| t.0.starts_with(REGION_TAG_PREFIX))
        .map(|t| t.0[REGION_TAG_PREFIX.len()..].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NodeId, NodeMode, Tag};

    fn tag(s: &str) -> Tag {
        Tag(s.into())
    }

    fn nid(s: &str) -> NodeId {
        NodeId(s.into())
    }

    fn node(id: &str, mode: NodeMode, tags: &[&str]) -> Node {
        let mut n = Node::new(nid(id), mode);
        for t in tags {
            n.add_tag(tag(t));
        }
        n
    }

    #[test]
    fn build_groups_by_region() {
        let nodes = vec![
            node("n1", NodeMode::Store, &["region:us-east"]),
            node("n2", NodeMode::Store, &["region:us-east"]),
            node("n3", NodeMode::Store, &["region:eu-west"]),
        ];
        let model = LatencyModel::new();

        let topo = TopologyView::build(&nodes, &model);
        assert_eq!(topo.total_nodes, 3);
        assert_eq!(topo.regions.len(), 2);

        let us = topo.regions.iter().find(|r| r.name == "us-east").unwrap();
        assert_eq!(us.node_count, 2);

        let eu = topo.regions.iter().find(|r| r.name == "eu-west").unwrap();
        assert_eq!(eu.node_count, 1);
    }

    #[test]
    fn build_unknown_region_for_untagged() {
        let nodes = vec![
            node("n1", NodeMode::Store, &["region:us-east"]),
            node("n2", NodeMode::Store, &[]),
        ];
        let model = LatencyModel::new();

        let topo = TopologyView::build(&nodes, &model);
        assert_eq!(topo.regions.len(), 2);

        let unknown = topo.regions.iter().find(|r| r.name == "unknown").unwrap();
        assert_eq!(unknown.node_count, 1);
        assert_eq!(unknown.node_ids, vec!["n2"]);
    }

    #[test]
    fn build_inter_region_latency() {
        let nodes = vec![
            node("n1", NodeMode::Store, &["region:us-east"]),
            node("n2", NodeMode::Store, &["region:eu-west"]),
        ];
        let mut model = LatencyModel::new();
        model.update_latency(&nid("n1"), &nid("n2"), 80.0, 1000);

        let topo = TopologyView::build(&nodes, &model);
        let us = topo.regions.iter().find(|r| r.name == "us-east").unwrap();
        assert!((us.inter_region_latency_ms["eu-west"] - 80.0).abs() < 0.01);

        // n2 -> n1 was not measured, so eu-west should not have inter-region data.
        let eu = topo.regions.iter().find(|r| r.name == "eu-west").unwrap();
        assert!(eu.inter_region_latency_ms.is_empty());
    }

    #[test]
    fn build_empty_nodes() {
        let topo = TopologyView::build(&[], &LatencyModel::new());
        assert_eq!(topo.total_nodes, 0);
        assert!(topo.regions.is_empty());
    }

    #[test]
    fn regions_accessor() {
        let nodes = vec![node("n1", NodeMode::Store, &["region:ap-northeast"])];
        let topo = TopologyView::build(&nodes, &LatencyModel::new());
        let regions = topo.regions();
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].name, "ap-northeast");
    }

    #[test]
    fn serde_round_trip() {
        let nodes = vec![
            node("n1", NodeMode::Store, &["region:us-east"]),
            node("n2", NodeMode::Store, &["region:eu-west"]),
        ];
        let mut model = LatencyModel::new();
        model.update_latency(&nid("n1"), &nid("n2"), 75.0, 1000);

        let topo = TopologyView::build(&nodes, &model);
        let json = serde_json::to_string(&topo).unwrap();
        let back: TopologyView = serde_json::from_str(&json).unwrap();

        assert_eq!(back.total_nodes, 2);
        assert_eq!(back.regions.len(), 2);
    }

    #[test]
    fn node_ids_are_sorted() {
        let nodes = vec![
            node("n3", NodeMode::Store, &["region:us-east"]),
            node("n1", NodeMode::Store, &["region:us-east"]),
            node("n2", NodeMode::Store, &["region:us-east"]),
        ];
        let topo = TopologyView::build(&nodes, &LatencyModel::new());
        let us = topo.regions.iter().find(|r| r.name == "us-east").unwrap();
        assert_eq!(us.node_ids, vec!["n1", "n2", "n3"]);
    }
}
