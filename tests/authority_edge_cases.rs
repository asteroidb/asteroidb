//! Edge-case tests for authority node add/remove dynamics (Issue #256).
//!
//! Covers:
//! 1. Adding an authority node while writes are in-flight.
//! 2. Removing an authority node — quorum still works with remaining nodes.
//! 3. Removing enough authority nodes to break quorum — writes fail gracefully.
//! 4. Rapid add/remove cycles.
//! 5. Authority set change with concurrent certification.
//! 6. Policy version increments correctly across add/remove operations.
//! 7. recalculate_authorities after node tag changes.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::authority::ack_frontier::AckFrontier;
use asteroidb_poc::authority::certificate::{
    AuthoritySignature, KeysetVersion, MajorityCertificate, create_certificate_message,
    sign_message,
};
use asteroidb_poc::authority::verifier::verify_proof;
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::node::Node;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::runtime::{NodeRunner, NodeRunnerConfig};
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, NodeMode, PolicyVersion, Tag};

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn node_id(s: &str) -> NodeId {
    NodeId(s.into())
}

fn kr(prefix: &str) -> KeyRange {
    KeyRange {
        prefix: prefix.into(),
    }
}

fn tag(s: &str) -> Tag {
    Tag(s.into())
}

fn make_node(id: &str, mode: NodeMode, tags: &[&str]) -> Node {
    let mut n = Node::new(node_id(id), mode);
    for t in tags {
        n.add_tag(tag(t));
    }
    n
}

fn ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
    HlcTimestamp {
        physical,
        logical,
        node_id: node.into(),
    }
}

fn counter_value(n: i64) -> CrdtValue {
    let mut counter = PnCounter::new();
    for _ in 0..n {
        counter.increment(&node_id("writer"));
    }
    CrdtValue::Counter(counter)
}

fn wrap_ns(ns: SystemNamespace) -> Arc<RwLock<SystemNamespace>> {
    Arc::new(RwLock::new(ns))
}

fn wrap_api(api: CertifiedApi) -> Arc<Mutex<CertifiedApi>> {
    Arc::new(Mutex::new(api))
}

fn default_metrics() -> Arc<RuntimeMetrics> {
    Arc::new(RuntimeMetrics::default())
}

fn fast_config() -> NodeRunnerConfig {
    NodeRunnerConfig {
        certification_interval: Duration::from_millis(10),
        cleanup_interval: Duration::from_secs(60),
        compaction_check_interval: Duration::from_secs(60),
        frontier_report_interval: Duration::from_millis(10),
        sync_interval: None,
        ping_interval: None,
        ..NodeRunnerConfig::default()
    }
}

fn make_authority_def(prefix: &str, nodes: &[&str]) -> AuthorityDefinition {
    AuthorityDefinition {
        key_range: kr(prefix),
        authority_nodes: nodes.iter().map(|s| node_id(s)).collect(),
        auto_generated: false,
    }
}

fn make_certified_policy(prefix: &str, tags: &[&str]) -> PlacementPolicy {
    let tag_set = tags.iter().map(|t| tag(t)).collect();
    PlacementPolicy::new(PolicyVersion(1), kr(prefix), 3)
        .with_certified(true)
        .with_required_tags(tag_set)
}

// ===========================================================================
// Test 1: Adding an authority node while writes are in-flight
// ===========================================================================

#[tokio::test]
async fn add_authority_while_writes_in_flight() {
    // Start with 3 authorities, write some data, then add a 4th.
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(make_authority_def("", &["auth-1", "auth-2", "auth-3"]));
    // PlacementPolicy stores its own version (PolicyVersion(1)), which is
    // what resolve_scope returns and PendingWrite records.
    let policy = PlacementPolicy::new(PolicyVersion(1), kr(""), 1).with_certified(true);
    ns.set_placement_policy(policy);

    let shared_ns = wrap_ns(ns);
    let mut api = CertifiedApi::new(node_id("auth-1"), shared_ns.clone());

    // Issue writes before adding the new authority.
    for i in 0..5 {
        api.certified_write(format!("key-{i}"), counter_value(1), OnTimeout::Pending)
            .unwrap();
    }

    // All writes should be pending.
    assert_eq!(api.pending_writes().len(), 5);
    for pw in api.pending_writes() {
        assert_eq!(pw.status, CertificationStatus::Pending);
    }

    // Now add auth-4 to the authority set (bumps namespace version, but the
    // placement policy version stored in pending writes stays PolicyVersion(1)).
    {
        let mut ns = shared_ns.write().unwrap();
        ns.set_authority_definition(make_authority_def(
            "",
            &["auth-1", "auth-2", "auth-3", "auth-4"],
        ));
    }

    // Feed frontier acks using the PLACEMENT POLICY version that writes stored.
    // The writes were recorded with total_authorities=3 and policy_version=PolicyVersion(1).
    // Majority of 3-node set = 2 of 3.
    let far_hlc = ts(u64::MAX - 1, u32::MAX, "zzz");
    let placement_pv = PolicyVersion(1);
    for auth in &["auth-1", "auth-2"] {
        api.update_frontier(AckFrontier {
            authority_id: node_id(auth),
            frontier_hlc: far_hlc.clone(),
            key_range: kr(""),
            policy_version: placement_pv,
            digest_hash: String::new(),
        });
    }

    api.process_certifications();

    // The first 5 writes (issued under the old authority set) should be
    // certified now that a majority (2 of 3) of the old authority set acked.
    let certified_count = api
        .pending_writes()
        .iter()
        .filter(|pw| pw.status == CertificationStatus::Certified)
        .count();
    assert!(
        certified_count >= 5,
        "pre-expansion writes should be certified under the old authority set; got {certified_count}"
    );
}

// ===========================================================================
// Test 2: Removing an authority — quorum still works with remaining nodes
// ===========================================================================

#[tokio::test]
async fn remove_authority_quorum_still_works() {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(make_authority_def("", &["auth-1", "auth-2", "auth-3"]));
    let policy = PlacementPolicy::new(PolicyVersion(1), kr(""), 1).with_certified(true);
    ns.set_placement_policy(policy);

    let shared_ns = wrap_ns(ns);
    let mut api = CertifiedApi::new(node_id("auth-1"), shared_ns.clone());

    // Write under the 3-node authority set.
    // PendingWrite will store: policy_version=PolicyVersion(1), total_authorities=3.
    api.certified_write("key-a".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();

    // Remove auth-3, leaving auth-1 and auth-2.
    {
        let mut ns = shared_ns.write().unwrap();
        ns.set_authority_definition(make_authority_def("", &["auth-1", "auth-2"]));
    }

    // Feed frontier acks using the PLACEMENT POLICY version (PolicyVersion(1)).
    // The write was recorded with total_authorities=3, so majority = 2 of 3.
    let far_hlc = ts(u64::MAX - 1, u32::MAX, "zzz");
    let placement_pv = PolicyVersion(1);
    for auth in &["auth-1", "auth-2"] {
        api.update_frontier(AckFrontier {
            authority_id: node_id(auth),
            frontier_hlc: far_hlc.clone(),
            key_range: kr(""),
            policy_version: placement_pv,
            digest_hash: String::new(),
        });
    }

    api.process_certifications();

    // The write from the original authority set should be certified (2 of 3 acked).
    let certified_count = api
        .pending_writes()
        .iter()
        .filter(|pw| pw.status == CertificationStatus::Certified)
        .count();
    assert!(
        certified_count >= 1,
        "write should be certified after majority of original authority set acked"
    );
}

// ===========================================================================
// Test 3: Breaking quorum — verify writes fail gracefully
// ===========================================================================

#[test]
fn broken_quorum_proof_is_invalid() {
    // Build a proof with only 1-of-5 signers (below majority threshold).
    let kr = kr("user/");
    let hlc = ts(1_700_000_000_000, 42, "node-1");
    let pv = PolicyVersion(1);

    let message = create_certificate_message(&kr, &hlc, &pv);
    let mut cert = MajorityCertificate::new(kr.clone(), hlc.clone(), pv, KeysetVersion(1));

    // Only one signer out of 5.
    let sk = SigningKey::generate(&mut OsRng);
    let vk = sk.verifying_key();
    let sig = sign_message(&sk, &message);
    cert.add_signature(AuthoritySignature {
        authority_id: node_id("auth-0"),
        public_key: vk,
        signature: sig,
        keyset_version: KeysetVersion(1),
    });

    assert!(
        !cert.has_majority(5),
        "1-of-5 signers should not constitute a majority"
    );

    let proof = asteroidb_poc::api::certified::ProofBundle {
        key_range: kr,
        frontier_hlc: hlc,
        policy_version: pv,
        contributing_authorities: vec![node_id("auth-0")],
        total_authorities: 5,
        certificate: Some(cert),
    };

    let result = verify_proof(&proof);
    assert!(
        !result.valid,
        "proof with insufficient signers should not verify as valid"
    );
}

// ===========================================================================
// Test 4: Rapid add/remove cycles
// ===========================================================================

#[test]
fn rapid_add_remove_cycles() {
    let mut ns = SystemNamespace::new();
    // Use a high replica_count (10) so select_nodes does not truncate
    // when we add a 4th node.
    let tag_set = [tag("region:us")].into_iter().collect();
    let policy = PlacementPolicy::new(PolicyVersion(1), kr("data/"), 10)
        .with_certified(true)
        .with_required_tags(tag_set);
    ns.set_placement_policy(policy);

    let base_nodes = vec![
        make_node("n1", NodeMode::Store, &["region:us"]),
        make_node("n2", NodeMode::Store, &["region:us"]),
        make_node("n3", NodeMode::Store, &["region:us"]),
    ];

    // Initial recalculation.
    ns.recalculate_authorities(&base_nodes);
    let initial_def = ns.get_authority_definition("data/").unwrap();
    assert_eq!(initial_def.authority_nodes.len(), 3);

    // Cycle: add node, then remove it, repeated many times.
    for cycle in 0..20 {
        // Add a new node.
        let extra_id = format!("extra-{cycle}");
        let mut nodes_with_extra = base_nodes.clone();
        nodes_with_extra.push(make_node(&extra_id, NodeMode::Store, &["region:us"]));
        ns.recalculate_authorities(&nodes_with_extra);
        let def = ns.get_authority_definition("data/").unwrap();
        assert_eq!(
            def.authority_nodes.len(),
            4,
            "cycle {cycle}: should have 4 authorities after add"
        );

        // Remove the extra node.
        ns.recalculate_authorities(&base_nodes);
        let def = ns.get_authority_definition("data/").unwrap();
        assert_eq!(
            def.authority_nodes.len(),
            3,
            "cycle {cycle}: should have 3 authorities after remove"
        );
    }

    // Final state should be the original 3 authorities.
    let final_def = ns.get_authority_definition("data/").unwrap();
    assert_eq!(final_def.authority_nodes.len(), 3);
    for base in &base_nodes {
        assert!(final_def.authority_nodes.contains(&base.id));
    }
}

// ===========================================================================
// Test 5: Authority set change with concurrent certification
// ===========================================================================

#[tokio::test]
async fn authority_change_during_certification() {
    let mut ns = SystemNamespace::new();
    let policy = PlacementPolicy::new(PolicyVersion(1), kr(""), 1).with_certified(true);
    ns.set_placement_policy(policy);
    ns.set_authority_definition(make_authority_def("", &["auth-1", "auth-2", "auth-3"]));

    let shared_ns = wrap_ns(ns);
    let api = wrap_api(CertifiedApi::new(node_id("auth-1"), shared_ns.clone()));

    // Write under the original authority set.
    {
        let mut api_lock = api.lock().await;
        api_lock
            .certified_write(
                "concurrent-key".into(),
                counter_value(1),
                OnTimeout::Pending,
            )
            .unwrap();
    }

    let cluster_nodes = Arc::new(RwLock::new(vec![
        make_node("auth-1", NodeMode::Store, &[]),
        make_node("auth-2", NodeMode::Store, &[]),
        make_node("auth-3", NodeMode::Store, &[]),
    ]));

    let mut runner = NodeRunner::with_cluster_nodes(
        node_id("auth-1"),
        api.clone(),
        CompactionEngine::with_defaults(),
        fast_config(),
        default_metrics(),
        cluster_nodes.clone(),
    )
    .await;

    let handle = runner.shutdown_handle();
    let ns_clone = shared_ns.clone();
    tokio::spawn(async move {
        // Let the runner start certification.
        tokio::time::sleep(Duration::from_millis(30)).await;

        // Modify the authority set mid-certification.
        {
            let mut ns = ns_clone.write().unwrap();
            ns.set_authority_definition(make_authority_def(
                "",
                &["auth-1", "auth-2", "auth-3", "auth-4"],
            ));
        }

        tokio::time::sleep(Duration::from_millis(80)).await;
        let _ = handle.send(true);
    });

    runner.run().await;

    // The write should either be certified (if the old frontier reached
    // majority before the change) or still pending (if not). The key
    // invariant is that it must NOT be in an inconsistent state.
    let api_lock = api.lock().await;
    let writes = api_lock.pending_writes();
    assert!(
        !writes.is_empty(),
        "pending writes list should still be populated"
    );
    let status = &writes[0].status;
    assert!(
        *status == CertificationStatus::Pending || *status == CertificationStatus::Certified,
        "write status must be Pending or Certified, got: {status:?}"
    );
}

// ===========================================================================
// Test 6: Policy version increments correctly across add/remove
// ===========================================================================

#[test]
fn policy_version_increments_on_authority_changes() {
    let mut ns = SystemNamespace::new();
    assert_eq!(*ns.version(), PolicyVersion(1));

    // Add first authority definition → version bumps.
    ns.set_authority_definition(make_authority_def("user/", &["n1", "n2", "n3"]));
    assert_eq!(*ns.version(), PolicyVersion(2));

    // Modify authority definition → version bumps again.
    ns.set_authority_definition(make_authority_def("user/", &["n1", "n2"]));
    assert_eq!(*ns.version(), PolicyVersion(3));

    // Add a second key range → version bumps.
    ns.set_authority_definition(make_authority_def("order/", &["n4", "n5"]));
    assert_eq!(*ns.version(), PolicyVersion(4));

    // Re-add the same definition (overwrite) → still bumps.
    ns.set_authority_definition(make_authority_def("user/", &["n1", "n2", "n3"]));
    assert_eq!(*ns.version(), PolicyVersion(5));

    // Verify the full history is tracked.
    let history = ns.version_history();
    assert_eq!(
        history,
        &[
            PolicyVersion(1),
            PolicyVersion(2),
            PolicyVersion(3),
            PolicyVersion(4),
            PolicyVersion(5),
        ]
    );
}

// ===========================================================================
// Test 7: recalculate_authorities after node tag changes
// ===========================================================================

#[test]
fn recalculate_authorities_after_tag_change() {
    let mut ns = SystemNamespace::new();
    ns.set_placement_policy(make_certified_policy("data/", &["region:eu"]));

    // Initial: n1 and n2 have region:eu, n3 does not.
    let nodes_v1 = vec![
        make_node("n1", NodeMode::Store, &["region:eu"]),
        make_node("n2", NodeMode::Store, &["region:eu"]),
        make_node("n3", NodeMode::Store, &["region:us"]),
    ];

    let changed = ns.recalculate_authorities(&nodes_v1);
    assert_eq!(changed, 1);
    let def = ns.get_authority_definition("data/").unwrap();
    assert_eq!(def.authority_nodes.len(), 2);
    assert!(def.authority_nodes.contains(&node_id("n1")));
    assert!(def.authority_nodes.contains(&node_id("n2")));
    assert!(!def.authority_nodes.contains(&node_id("n3")));

    // Simulate n3 gaining the region:eu tag (tag change).
    let nodes_v2 = vec![
        make_node("n1", NodeMode::Store, &["region:eu"]),
        make_node("n2", NodeMode::Store, &["region:eu"]),
        make_node("n3", NodeMode::Store, &["region:eu"]), // tag changed
    ];

    let changed = ns.recalculate_authorities(&nodes_v2);
    assert_eq!(changed, 1);
    let def = ns.get_authority_definition("data/").unwrap();
    assert_eq!(def.authority_nodes.len(), 3);
    assert!(def.authority_nodes.contains(&node_id("n3")));

    // Simulate n1 losing the region:eu tag.
    let nodes_v3 = vec![
        make_node("n1", NodeMode::Store, &["region:us"]), // tag changed
        make_node("n2", NodeMode::Store, &["region:eu"]),
        make_node("n3", NodeMode::Store, &["region:eu"]),
    ];

    let changed = ns.recalculate_authorities(&nodes_v3);
    assert_eq!(changed, 1);
    let def = ns.get_authority_definition("data/").unwrap();
    assert_eq!(def.authority_nodes.len(), 2);
    assert!(!def.authority_nodes.contains(&node_id("n1")));
    assert!(def.authority_nodes.contains(&node_id("n2")));
    assert!(def.authority_nodes.contains(&node_id("n3")));
}

// ===========================================================================
// Test 8: Removing all authorities — verifier rejects empty signer set
// ===========================================================================

#[test]
fn empty_authority_set_rejects_certification() {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(make_authority_def("", &["auth-1", "auth-2", "auth-3"]));

    // Remove all authorities.
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr(""),
        authority_nodes: vec![],
        auto_generated: false,
    });

    let def = ns.get_authority_definition("").unwrap();
    assert!(def.authority_nodes.is_empty());

    // A proof with 0 total authorities → verify_proof should fail.
    let proof = asteroidb_poc::api::certified::ProofBundle {
        key_range: kr(""),
        frontier_hlc: ts(1_700_000_000_000, 0, "node-1"),
        policy_version: PolicyVersion(1),
        contributing_authorities: vec![],
        total_authorities: 0,
        certificate: None,
    };

    let result = verify_proof(&proof);
    assert!(
        !result.valid,
        "empty authority set should not produce a valid proof"
    );
}

// ===========================================================================
// Test 9: Concurrent add and frontier processing via NodeRunner
// ===========================================================================

#[tokio::test]
async fn concurrent_add_with_runner_frontier() {
    let mut ns = SystemNamespace::new();
    let policy = PlacementPolicy::new(PolicyVersion(1), kr(""), 1).with_certified(true);
    ns.set_placement_policy(policy);
    ns.set_authority_definition(make_authority_def("", &["auth-1"]));

    let shared_ns = wrap_ns(ns);
    let mut api = CertifiedApi::new(node_id("auth-1"), shared_ns.clone());

    // Write a pending entry.
    api.certified_write("test-key".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();

    let shared_api = wrap_api(api);
    let cluster_nodes = Arc::new(RwLock::new(vec![make_node("auth-1", NodeMode::Store, &[])]));

    let mut runner = NodeRunner::with_cluster_nodes(
        node_id("auth-1"),
        shared_api.clone(),
        CompactionEngine::with_defaults(),
        fast_config(),
        default_metrics(),
        cluster_nodes.clone(),
    )
    .await;

    let nodes_ref = cluster_nodes.clone();
    let ns_ref = shared_ns.clone();
    let handle = runner.shutdown_handle();
    tokio::spawn(async move {
        // Add a new authority while the runner is processing.
        tokio::time::sleep(Duration::from_millis(40)).await;
        {
            let mut ns = ns_ref.write().unwrap();
            ns.set_authority_definition(make_authority_def("", &["auth-1", "auth-2"]));
        }
        nodes_ref
            .write()
            .unwrap()
            .push(make_node("auth-2", NodeMode::Store, &[]));

        tokio::time::sleep(Duration::from_millis(80)).await;
        let _ = handle.send(true);
    });

    runner.run().await;

    // The write should have been processed — either certified (under old
    // or new authority set) or still pending. No panics or inconsistencies.
    let api_lock = shared_api.lock().await;
    let writes = api_lock.pending_writes();
    assert!(!writes.is_empty());
}
