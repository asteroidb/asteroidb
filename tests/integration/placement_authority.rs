//! Integration tests: PlacementPolicy × Authority constraint verification (#30).
//!
//! Validates that nodes selected by PlacementPolicy correctly participate in
//! Authority consensus via AckFrontierSet, and that tag-based filtering,
//! node mode restrictions, and policy version changes are handled properly.

use std::collections::HashSet;

use asteroidb_poc::authority::{AckFrontier, AckFrontierSet, FrontierScope};
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::node::Node;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::types::{KeyRange, NodeId, NodeMode, PolicyVersion, Tag};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn tag(s: &str) -> Tag {
    Tag(s.into())
}

fn tags(names: &[&str]) -> HashSet<Tag> {
    names.iter().map(|s| tag(s)).collect()
}

fn key_range(prefix: &str) -> KeyRange {
    KeyRange {
        prefix: prefix.into(),
    }
}

fn make_node(id: &str, mode: NodeMode, tag_names: &[&str]) -> Node {
    let mut n = Node::new(NodeId(id.into()), mode);
    for t in tag_names {
        n.add_tag(tag(t));
    }
    n
}

fn make_ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
    HlcTimestamp {
        physical,
        logical,
        node_id: node.into(),
    }
}

fn make_frontier(
    authority_id: &str,
    physical: u64,
    logical: u32,
    prefix: &str,
    policy_version: u64,
) -> AckFrontier {
    AckFrontier {
        authority_id: NodeId(authority_id.into()),
        frontier_hlc: make_ts(physical, logical, authority_id),
        key_range: key_range(prefix),
        policy_version: PolicyVersion(policy_version),
        digest_hash: format!("{authority_id}-{physical}-{logical}"),
    }
}

// ---------------------------------------------------------------------------
// 1. Selected nodes manage ack_frontier as Authority
// ---------------------------------------------------------------------------

#[test]
fn selected_nodes_manage_ack_frontier() {
    // Build a set of nodes, some matching the policy
    let nodes = vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo"]),
        make_node("n2", NodeMode::Store, &["dc:tokyo"]),
        make_node("n3", NodeMode::Store, &["dc:osaka"]),
        make_node("n4", NodeMode::Both, &["dc:tokyo"]),
    ];

    let policy = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
        .with_required_tags(tags(&["dc:tokyo"]))
        .with_certified(true);

    // Select authority nodes via placement policy
    let selected = policy.select_nodes(&nodes);
    assert_eq!(selected.len(), 3); // n1, n2, n4

    // Each selected node acts as authority and reports ack_frontier
    let mut frontier_set = AckFrontierSet::new();
    for (i, node) in selected.iter().enumerate() {
        let physical = 1000 + (i as u64) * 100; // 1000, 1100, 1200
        frontier_set.update(make_frontier(node.id.0.as_str(), physical, 0, "user/", 1));
    }

    // All 3 selected authorities are tracked
    assert_eq!(frontier_set.all().len(), 3);
    for node in &selected {
        assert!(
            frontier_set.get(&node.id).is_some(),
            "Authority {} should be tracked",
            node.id.0
        );
    }

    // Non-selected node (n3) is NOT an authority
    assert!(frontier_set.get(&NodeId("n3".into())).is_none());

    // Majority frontier should work with exactly 3 authorities
    let total_authorities = selected.len();
    let mf = frontier_set.majority_frontier(total_authorities).unwrap();
    // Sorted frontiers: [1000, 1100, 1200], majority=2, index=3-2=1 → 1100
    assert_eq!(mf.physical, 1100);
}

// ---------------------------------------------------------------------------
// 2. Only nodes with required_tags participate in Authority consensus
// ---------------------------------------------------------------------------

#[test]
fn required_tags_filter_authority_participants() {
    let nodes = vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo", "tier:hot"]),
        make_node("n2", NodeMode::Store, &["dc:tokyo"]), // missing tier:hot
        make_node("n3", NodeMode::Both, &["dc:tokyo", "tier:hot"]),
        make_node("n4", NodeMode::Store, &["dc:osaka", "tier:hot"]), // missing dc:tokyo
        make_node("n5", NodeMode::Store, &["dc:tokyo", "tier:hot"]),
    ];

    let policy = PlacementPolicy::new(PolicyVersion(1), key_range("order/"), 3)
        .with_required_tags(tags(&["dc:tokyo", "tier:hot"]))
        .with_certified(true);

    let selected = policy.select_nodes(&nodes);
    // Only n1, n3, n5 have both required tags
    assert_eq!(selected.len(), 3);

    let selected_ids: Vec<&str> = selected.iter().map(|n| n.id.0.as_str()).collect();
    assert!(selected_ids.contains(&"n1"));
    assert!(selected_ids.contains(&"n3"));
    assert!(selected_ids.contains(&"n5"));
    assert!(!selected_ids.contains(&"n2"));
    assert!(!selected_ids.contains(&"n4"));

    // Build frontier set with only selected authorities
    let mut frontier_set = AckFrontierSet::new();
    for (i, node) in selected.iter().enumerate() {
        frontier_set.update(make_frontier(
            node.id.0.as_str(),
            2000 + (i as u64) * 50,
            0,
            "order/",
            1,
        ));
    }

    // Certification works with the correct authority count
    let ts_below = make_ts(2040, 0, "client");
    assert!(frontier_set.is_certified_at(&ts_below, selected.len()));

    // Non-participant n2 has no frontier
    assert!(frontier_set.get(&NodeId("n2".into())).is_none());
}

// ---------------------------------------------------------------------------
// 3. Nodes with forbidden_tags are excluded from Authority consensus
// ---------------------------------------------------------------------------

#[test]
fn forbidden_tags_exclude_from_authority() {
    let nodes = vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo"]),
        make_node("n2", NodeMode::Store, &["dc:tokyo", "decommissioned"]),
        make_node("n3", NodeMode::Both, &["dc:tokyo"]),
        make_node("n4", NodeMode::Store, &["dc:tokyo", "maintenance"]),
        make_node("n5", NodeMode::Store, &["dc:tokyo"]),
    ];

    let policy = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
        .with_required_tags(tags(&["dc:tokyo"]))
        .with_forbidden_tags(tags(&["decommissioned", "maintenance"]))
        .with_certified(true);

    let selected = policy.select_nodes(&nodes);
    // n2 (decommissioned) and n4 (maintenance) are excluded
    assert_eq!(selected.len(), 3);

    let selected_ids: Vec<&str> = selected.iter().map(|n| n.id.0.as_str()).collect();
    assert!(selected_ids.contains(&"n1"));
    assert!(selected_ids.contains(&"n3"));
    assert!(selected_ids.contains(&"n5"));
    assert!(
        !selected_ids.contains(&"n2"),
        "decommissioned node must be excluded"
    );
    assert!(
        !selected_ids.contains(&"n4"),
        "maintenance node must be excluded"
    );

    // Frontier set only tracks non-forbidden authorities
    let mut frontier_set = AckFrontierSet::new();
    for node in &selected {
        frontier_set.update(make_frontier(node.id.0.as_str(), 3000, 0, "user/", 1));
    }

    assert_eq!(frontier_set.all().len(), 3);
    assert!(frontier_set.get(&NodeId("n2".into())).is_none());
    assert!(frontier_set.get(&NodeId("n4".into())).is_none());

    // All 3 at same timestamp → majority frontier = 3000
    let mf = frontier_set.majority_frontier(selected.len()).unwrap();
    assert_eq!(mf.physical, 3000);
}

// ---------------------------------------------------------------------------
// 4. Error handling when min_replicas not met (Authority shortage)
// ---------------------------------------------------------------------------

#[test]
fn min_replicas_not_met_authority_shortage() {
    let nodes = vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo"]),
        make_node("n2", NodeMode::Subscribe, &["dc:tokyo"]), // Subscribe mode
        make_node("n3", NodeMode::Store, &["dc:osaka"]),     // wrong tag
    ];

    // Policy requires 3 replicas with dc:tokyo
    let policy = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
        .with_required_tags(tags(&["dc:tokyo"]))
        .with_certified(true);

    // Only n1 matches → insufficient
    let selected = policy.select_nodes(&nodes);
    assert_eq!(selected.len(), 1);
    assert!(!policy.is_satisfied(&nodes));

    // With only 1 authority out of required 3, majority cannot be reached
    let mut frontier_set = AckFrontierSet::new();
    frontier_set.update(make_frontier("n1", 5000, 0, "user/", 1));

    // majority_frontier returns None when insufficient authorities
    assert!(frontier_set.majority_frontier(3).is_none());

    // Nothing can be certified without majority
    let ts = make_ts(4000, 0, "client");
    assert!(!frontier_set.is_certified_at(&ts, 3));
}

#[test]
fn min_replicas_not_met_zero_matching_nodes() {
    let nodes = vec![
        make_node("n1", NodeMode::Subscribe, &["dc:tokyo"]),
        make_node("n2", NodeMode::Subscribe, &["dc:tokyo"]),
    ];

    let policy = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 2)
        .with_required_tags(tags(&["dc:tokyo"]))
        .with_certified(true);

    // No Store/Both nodes → 0 selected
    let selected = policy.select_nodes(&nodes);
    assert!(selected.is_empty());
    assert!(!policy.is_satisfied(&nodes));

    // Empty frontier set has no majority
    let frontier_set = AckFrontierSet::new();
    assert!(frontier_set.majority_frontier(2).is_none());
}

// ---------------------------------------------------------------------------
// 5. Authority set reconstruction after policy change
// ---------------------------------------------------------------------------

#[test]
fn authority_set_reconstruction_after_policy_change() {
    let nodes = vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo", "tier:hot"]),
        make_node("n2", NodeMode::Store, &["dc:tokyo"]),
        make_node("n3", NodeMode::Both, &["dc:osaka", "tier:hot"]),
        make_node("n4", NodeMode::Store, &["dc:tokyo", "tier:hot"]),
        make_node("n5", NodeMode::Store, &["dc:osaka"]),
    ];

    // Initial policy: require dc:tokyo, version 1
    let policy_v1 = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 2)
        .with_required_tags(tags(&["dc:tokyo"]))
        .with_certified(true);

    let selected_v1 = policy_v1.select_nodes(&nodes);
    let selected_v1_ids: Vec<&str> = selected_v1.iter().map(|n| n.id.0.as_str()).collect();
    assert!(selected_v1_ids.contains(&"n1"));
    assert!(selected_v1_ids.contains(&"n2"));
    assert!(selected_v1_ids.contains(&"n4"));

    // Build frontier set with v1 authorities
    let mut frontier_set_v1 = AckFrontierSet::new();
    for node in &selected_v1 {
        frontier_set_v1.update(make_frontier(node.id.0.as_str(), 6000, 0, "user/", 1));
    }

    // Policy change: now require dc:osaka, version 2
    let policy_v2 = PlacementPolicy::new(PolicyVersion(2), key_range("user/"), 2)
        .with_required_tags(tags(&["dc:osaka"]))
        .with_certified(true);

    let selected_v2 = policy_v2.select_nodes(&nodes);
    let selected_v2_ids: Vec<&str> = selected_v2.iter().map(|n| n.id.0.as_str()).collect();
    assert!(selected_v2_ids.contains(&"n3"));
    assert!(selected_v2_ids.contains(&"n5"));
    // n1, n2, n4 are no longer authorities
    assert!(!selected_v2_ids.contains(&"n1"));
    assert!(!selected_v2_ids.contains(&"n2"));
    assert!(!selected_v2_ids.contains(&"n4"));

    // Build NEW frontier set for v2 authorities (fresh start)
    let mut frontier_set_v2 = AckFrontierSet::new();
    for node in &selected_v2 {
        frontier_set_v2.update(make_frontier(node.id.0.as_str(), 7000, 0, "user/", 2));
    }

    // Old authorities not in new set
    assert!(frontier_set_v2.get(&NodeId("n1".into())).is_none());
    assert!(frontier_set_v2.get(&NodeId("n2".into())).is_none());

    // New authority set functions correctly
    assert_eq!(frontier_set_v2.all().len(), 2);
    let mf = frontier_set_v2
        .majority_frontier(selected_v2.len())
        .unwrap();
    assert_eq!(mf.physical, 7000);
}

#[test]
fn policy_change_adds_tag_requirement_shrinks_authority_set() {
    let nodes = vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo", "tier:hot"]),
        make_node("n2", NodeMode::Store, &["dc:tokyo"]),
        make_node("n3", NodeMode::Store, &["dc:tokyo", "tier:hot"]),
    ];

    // v1: only dc:tokyo required → all 3 match
    let policy_v1 = PlacementPolicy::new(PolicyVersion(1), key_range("data/"), 2)
        .with_required_tags(tags(&["dc:tokyo"]));
    assert_eq!(policy_v1.select_nodes(&nodes).len(), 3);
    assert!(policy_v1.is_satisfied(&nodes));

    // v2: add tier:hot requirement → only n1, n3 match
    let policy_v2 = PlacementPolicy::new(PolicyVersion(2), key_range("data/"), 2)
        .with_required_tags(tags(&["dc:tokyo", "tier:hot"]));
    let selected_v2 = policy_v2.select_nodes(&nodes);
    assert_eq!(selected_v2.len(), 2);
    assert!(policy_v2.is_satisfied(&nodes));

    // n2 is removed from authority set
    let selected_v2_ids: Vec<&str> = selected_v2.iter().map(|n| n.id.0.as_str()).collect();
    assert!(!selected_v2_ids.contains(&"n2"));

    // If we raise replica_count to 3, policy is no longer satisfied
    let policy_v3 = PlacementPolicy::new(PolicyVersion(3), key_range("data/"), 3)
        .with_required_tags(tags(&["dc:tokyo", "tier:hot"]));
    assert!(!policy_v3.is_satisfied(&nodes));
}

// ---------------------------------------------------------------------------
// 6. Subscribe-mode nodes do NOT participate in Authority consensus
// ---------------------------------------------------------------------------

#[test]
fn subscribe_mode_excluded_from_authority() {
    let nodes = vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo"]),
        make_node("n2", NodeMode::Subscribe, &["dc:tokyo"]),
        make_node("n3", NodeMode::Both, &["dc:tokyo"]),
        make_node("n4", NodeMode::Subscribe, &["dc:tokyo"]),
        make_node("n5", NodeMode::Store, &["dc:tokyo"]),
    ];

    let policy = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
        .with_required_tags(tags(&["dc:tokyo"]))
        .with_certified(true);

    let selected = policy.select_nodes(&nodes);
    // Only Store and Both modes are accepted
    assert_eq!(selected.len(), 3); // n1, n3, n5

    for node in &selected {
        assert_ne!(
            node.mode,
            NodeMode::Subscribe,
            "Subscribe node {} should not be authority",
            node.id.0
        );
    }

    // Build frontier set — no Subscribe nodes contribute
    let mut frontier_set = AckFrontierSet::new();
    for node in &selected {
        frontier_set.update(make_frontier(node.id.0.as_str(), 8000, 0, "user/", 1));
    }

    // Subscribe nodes are absent
    assert!(frontier_set.get(&NodeId("n2".into())).is_none());
    assert!(frontier_set.get(&NodeId("n4".into())).is_none());

    // Majority works with only Store/Both authorities
    let mf = frontier_set.majority_frontier(selected.len()).unwrap();
    assert_eq!(mf.physical, 8000);
}

#[test]
fn subscribe_only_cluster_has_no_authorities() {
    let nodes = vec![
        make_node("n1", NodeMode::Subscribe, &["dc:tokyo"]),
        make_node("n2", NodeMode::Subscribe, &["dc:tokyo"]),
        make_node("n3", NodeMode::Subscribe, &["dc:tokyo"]),
    ];

    let policy = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 2)
        .with_required_tags(tags(&["dc:tokyo"]))
        .with_certified(true);

    let selected = policy.select_nodes(&nodes);
    assert!(selected.is_empty());
    assert!(!policy.is_satisfied(&nodes));
}

// ---------------------------------------------------------------------------
// 7. PolicyVersion change triggers ack_frontier reset
// ---------------------------------------------------------------------------

#[test]
fn policy_version_change_resets_ack_frontier() {
    let nodes = vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo"]),
        make_node("n2", NodeMode::Store, &["dc:tokyo"]),
        make_node("n3", NodeMode::Store, &["dc:tokyo"]),
    ];

    let policy_v1 = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 3)
        .with_required_tags(tags(&["dc:tokyo"]))
        .with_certified(true);

    let selected = policy_v1.select_nodes(&nodes);
    assert_eq!(selected.len(), 3);

    // Build frontier set under policy version 1 with high timestamps
    let mut frontier_set_v1 = AckFrontierSet::new();
    for node in &selected {
        frontier_set_v1.update(make_frontier(node.id.0.as_str(), 9000, 0, "user/", 1));
    }

    // Everything below 9000 is certified under v1
    assert!(frontier_set_v1.is_certified_at(&make_ts(8500, 0, "client"), selected.len()));

    // Policy version bumps to 2 → new AckFrontierSet (reset)
    let policy_v2 = PlacementPolicy::new(PolicyVersion(2), key_range("user/"), 3)
        .with_required_tags(tags(&["dc:tokyo"]))
        .with_certified(true);
    assert_eq!(policy_v2.version, PolicyVersion(2));

    // Fresh frontier set for the new policy version
    let mut frontier_set_v2 = AckFrontierSet::new();

    // Initially empty — nothing is certified
    assert!(frontier_set_v2.majority_frontier(selected.len()).is_none());
    assert!(!frontier_set_v2.is_certified_at(&make_ts(8500, 0, "client"), selected.len()));

    // As authorities re-report under v2, certification resumes
    let selected_v2 = policy_v2.select_nodes(&nodes);
    for (i, node) in selected_v2.iter().enumerate() {
        frontier_set_v2.update(make_frontier(
            node.id.0.as_str(),
            10000 + (i as u64) * 100,
            0,
            "user/",
            2,
        ));
    }

    // Now certification works under the new version
    // Sorted: [10000, 10100, 10200], majority=2, index=3-2=1 → 10100
    let mf = frontier_set_v2
        .majority_frontier(selected_v2.len())
        .unwrap();
    assert_eq!(mf.physical, 10100);
    assert!(frontier_set_v2.is_certified_at(&make_ts(10050, 0, "client"), selected_v2.len()));

    // Verify policy_version field on the frontiers
    for frontier in frontier_set_v2.all() {
        assert_eq!(frontier.policy_version, PolicyVersion(2));
    }
}

#[test]
fn frontier_tracks_policy_version_correctly() {
    // Verify that AckFrontier records preserve policy_version
    let f_v1 = make_frontier("auth-1", 5000, 0, "user/", 1);
    assert_eq!(f_v1.policy_version, PolicyVersion(1));

    let f_v2 = make_frontier("auth-1", 6000, 0, "user/", 2);
    assert_eq!(f_v2.policy_version, PolicyVersion(2));

    // An AckFrontierSet can hold frontiers with different policy versions
    // scoped by {key_range, policy_version, authority_id}
    let mut frontier_set = AckFrontierSet::new();
    frontier_set.update(f_v1);

    let scope_v1 = FrontierScope::new(
        KeyRange { prefix: "user/".into() },
        PolicyVersion(1),
        NodeId("auth-1".into()),
    );
    let tracked = frontier_set.get_scoped(&scope_v1).unwrap();
    assert_eq!(tracked.policy_version, PolicyVersion(1));

    // Update with newer policy version creates a separate scoped entry
    frontier_set.update(f_v2);
    let scope_v2 = FrontierScope::new(
        KeyRange { prefix: "user/".into() },
        PolicyVersion(2),
        NodeId("auth-1".into()),
    );
    let tracked = frontier_set.get_scoped(&scope_v2).unwrap();
    assert_eq!(tracked.policy_version, PolicyVersion(2));
    assert_eq!(tracked.frontier_hlc.physical, 6000);

    // v1 entry is still independently accessible (no contamination)
    let tracked_v1 = frontier_set.get_scoped(&scope_v1).unwrap();
    assert_eq!(tracked_v1.policy_version, PolicyVersion(1));
    assert_eq!(tracked_v1.frontier_hlc.physical, 5000);
}

// ---------------------------------------------------------------------------
// Combined scenario: end-to-end placement → authority → certification
// ---------------------------------------------------------------------------

#[test]
fn end_to_end_placement_to_certification() {
    // Simulate a realistic scenario: deploy nodes, apply policy, run authority consensus

    // 7-node cluster across two DCs
    let nodes = vec![
        make_node("tokyo-1", NodeMode::Store, &["dc:tokyo", "tier:hot"]),
        make_node("tokyo-2", NodeMode::Store, &["dc:tokyo", "tier:hot"]),
        make_node("tokyo-3", NodeMode::Both, &["dc:tokyo", "tier:warm"]),
        make_node("osaka-1", NodeMode::Store, &["dc:osaka", "tier:hot"]),
        make_node("osaka-2", NodeMode::Store, &["dc:osaka", "tier:warm"]),
        make_node(
            "monitor-1",
            NodeMode::Subscribe,
            &["dc:tokyo", "role:monitor"],
        ),
        make_node("decom-1", NodeMode::Store, &["dc:tokyo", "decommissioned"]),
    ];

    // Policy: certified key range, Tokyo hot-tier nodes, exclude decommissioned
    let policy = PlacementPolicy::new(PolicyVersion(1), key_range("txn/"), 2)
        .with_required_tags(tags(&["dc:tokyo", "tier:hot"]))
        .with_forbidden_tags(tags(&["decommissioned"]))
        .with_certified(true);

    let authorities = policy.select_nodes(&nodes);
    assert_eq!(authorities.len(), 2); // tokyo-1, tokyo-2

    let auth_ids: Vec<&str> = authorities.iter().map(|n| n.id.0.as_str()).collect();
    assert!(auth_ids.contains(&"tokyo-1"));
    assert!(auth_ids.contains(&"tokyo-2"));
    assert!(policy.is_satisfied(&nodes));

    // Authorities report frontiers at different speeds
    let mut frontier_set = AckFrontierSet::new();
    frontier_set.update(make_frontier("tokyo-1", 50_000, 0, "txn/", 1));
    frontier_set.update(make_frontier("tokyo-2", 45_000, 0, "txn/", 1));

    let total_auth = authorities.len();

    // Majority frontier = min of both (2 authorities, majority=2) → 45000
    let mf = frontier_set.majority_frontier(total_auth).unwrap();
    assert_eq!(mf.physical, 45_000);

    // Writes at or before 45000 are certified
    assert!(frontier_set.is_certified_at(&make_ts(44_000, 0, "client"), total_auth));
    assert!(frontier_set.is_certified_at(&make_ts(45_000, 0, "tokyo-2"), total_auth));

    // Writes after 45000 are not yet certified
    assert!(!frontier_set.is_certified_at(&make_ts(46_000, 0, "client"), total_auth));

    // tokyo-2 catches up
    frontier_set.update(make_frontier("tokyo-2", 50_000, 0, "txn/", 1));
    let mf = frontier_set.majority_frontier(total_auth).unwrap();
    assert_eq!(mf.physical, 50_000);

    // Now 46000 is certified
    assert!(frontier_set.is_certified_at(&make_ts(46_000, 0, "client"), total_auth));
}

#[test]
fn mixed_mode_cluster_authority_selection() {
    // Cluster with all three node modes
    let nodes = vec![
        make_node("store-1", NodeMode::Store, &["dc:tokyo"]),
        make_node("store-2", NodeMode::Store, &["dc:tokyo"]),
        make_node("both-1", NodeMode::Both, &["dc:tokyo"]),
        make_node("sub-1", NodeMode::Subscribe, &["dc:tokyo"]),
        make_node("sub-2", NodeMode::Subscribe, &["dc:tokyo"]),
    ];

    let policy = PlacementPolicy::new(PolicyVersion(1), key_range("data/"), 3)
        .with_required_tags(tags(&["dc:tokyo"]))
        .with_certified(true);

    let selected = policy.select_nodes(&nodes);
    assert_eq!(selected.len(), 3); // store-1, store-2, both-1

    // Verify modes
    for node in &selected {
        assert!(
            node.mode == NodeMode::Store || node.mode == NodeMode::Both,
            "Node {} has mode {:?}, expected Store or Both",
            node.id.0,
            node.mode
        );
    }

    // Build frontier set
    let mut frontier_set = AckFrontierSet::new();
    frontier_set.update(make_frontier("store-1", 11_000, 0, "data/", 1));
    frontier_set.update(make_frontier("store-2", 11_500, 0, "data/", 1));
    frontier_set.update(make_frontier("both-1", 11_200, 0, "data/", 1));

    // majority=2, sorted: [11000, 11200, 11500], index=3-2=1 → 11200
    let mf = frontier_set.majority_frontier(selected.len()).unwrap();
    assert_eq!(mf.physical, 11_200);
}

#[test]
fn forbidden_tags_dynamically_added_excludes_node() {
    let nodes = vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo"]),
        make_node("n2", NodeMode::Store, &["dc:tokyo", "quarantine"]),
        make_node("n3", NodeMode::Store, &["dc:tokyo"]),
    ];

    // v1: no forbidden tags → all 3 selected
    let policy_v1 = PlacementPolicy::new(PolicyVersion(1), key_range("user/"), 2)
        .with_required_tags(tags(&["dc:tokyo"]));
    assert_eq!(policy_v1.select_nodes(&nodes).len(), 3);

    // v2: forbid quarantine → n2 excluded
    let policy_v2 = PlacementPolicy::new(PolicyVersion(2), key_range("user/"), 2)
        .with_required_tags(tags(&["dc:tokyo"]))
        .with_forbidden_tags(tags(&["quarantine"]));
    let selected_v2 = policy_v2.select_nodes(&nodes);
    assert_eq!(selected_v2.len(), 2);

    let ids: Vec<&str> = selected_v2.iter().map(|n| n.id.0.as_str()).collect();
    assert!(!ids.contains(&"n2"));

    // Frontier set only tracks non-quarantined nodes
    let mut frontier_set = AckFrontierSet::new();
    for node in &selected_v2 {
        frontier_set.update(make_frontier(node.id.0.as_str(), 12_000, 0, "user/", 2));
    }

    assert_eq!(frontier_set.all().len(), 2);
    assert!(frontier_set.get(&NodeId("n2".into())).is_none());
    assert!(frontier_set.majority_frontier(2).is_some());
}
