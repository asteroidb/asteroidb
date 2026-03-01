//! Compound E2E test: authority reconfiguration x key rotation x delta sync (#134).
//!
//! Validates that these three features operate correctly when interleaved within
//! a single scenario:
//! 1. Authority auto-reconfiguration (node join/leave, policy change)
//! 2. Key rotation and epoch-based verification
//! 3. Delta sync (including frontier-based incremental pull and full-sync fallback)
//!
//! Acceptance criteria:
//! - Eventual data converges across all participating nodes.
//! - Certified judgment uses the post-reconfiguration authority set and valid epoch.
//! - Old authority set / old keyset does not erroneously produce certified status.
//! - Delta sync correctly transfers data to newly-joined nodes and frontier advances.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout, ProofBundle};
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::authority::ack_frontier::AckFrontier;
use asteroidb_poc::authority::certificate::{
    AuthoritySignature, EpochConfig, EpochManager, KeysetVersion, MajorityCertificate,
    create_certificate_message, sign_message,
};
use asteroidb_poc::authority::verifier::verify_proof_with_registry;
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::network::sync::{DeltaSyncRequest, DeltaSyncResponse};
use asteroidb_poc::node::Node;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::runtime::{NodeRunner, NodeRunnerConfig};
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, NodeMode, PolicyVersion, Tag};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ed25519_dalek::SigningKey;
use http_body_util::BodyExt;
use rand::rngs::OsRng;
use tokio::sync::Mutex;
use tower::ServiceExt;

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
    }
}

fn make_frontier_v(
    authority: &str,
    physical: u64,
    logical: u32,
    prefix: &str,
    version: u64,
) -> AckFrontier {
    AckFrontier {
        authority_id: NodeId(authority.into()),
        frontier_hlc: HlcTimestamp {
            physical,
            logical,
            node_id: authority.into(),
        },
        key_range: KeyRange {
            prefix: prefix.into(),
        },
        policy_version: PolicyVersion(version),
        digest_hash: format!("{authority}-{physical}-{logical}"),
    }
}

fn hlc(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
    HlcTimestamp {
        physical,
        logical,
        node_id: node.into(),
    }
}

fn make_key_pair() -> (SigningKey, ed25519_dalek::VerifyingKey) {
    let sk = SigningKey::generate(&mut OsRng);
    let vk = sk.verifying_key();
    (sk, vk)
}

fn test_state_with_ns(nid: NodeId, ns: Arc<RwLock<SystemNamespace>>) -> Arc<AppState> {
    Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(nid.clone()))),
        certified: Arc::new(Mutex::new(CertifiedApi::new(nid, ns.clone()))),
        namespace: ns,
        metrics: Arc::new(RuntimeMetrics::default()),
        peers: None,
        peer_persist_path: None,
        internal_token: None,
    })
}

async fn body_string(body: Body) -> String {
    let bytes = body.collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Pull delta from a source AppState and apply entries to the target AppState.
async fn sync_delta(
    source: &Arc<AppState>,
    target: &Arc<AppState>,
    frontier: HlcTimestamp,
) -> DeltaSyncResponse {
    let app = router(source.clone());
    let req_body = serde_json::to_string(&DeltaSyncRequest {
        sender: "sync-agent".into(),
        frontier,
    })
    .unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/api/internal/sync/delta")
        .header("content-type", "application/json")
        .body(Body::from(req_body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_string(resp.into_body()).await;
    let delta: DeltaSyncResponse = serde_json::from_str(&body).unwrap();

    // Apply delta entries to target.
    let mut api = target.eventual.lock().await;
    for entry in &delta.entries {
        let _ = api.merge_remote_with_hlc(entry.key.clone(), &entry.value, entry.hlc.clone());
    }

    delta
}

// ===========================================================================
// Main compound E2E test
// ===========================================================================

/// This test exercises all three v0.1.3 features in a single integrated scenario:
///
/// Phase 1: Initial setup (3 nodes, certified policy, keyset v1)
/// Phase 2: Write data (eventual + certified)
/// Phase 3: Authority reconfiguration (n4 joins, n3 removed from authority)
/// Phase 4: Key rotation (keyset v2 registered, v1 within grace period)
/// Phase 5: Delta sync (n4 catches up via delta)
/// Phase 6: Final verification (convergence, certified judgment, safety checks)
#[tokio::test]
async fn authority_reconfig_with_key_rotation_and_delta_sync() {
    // ===================================================================
    // Phase 1: Initial setup
    //
    // 3-node cluster (n1, n2, n3) with a certified placement policy
    // requiring "dc:tokyo" tag. Keyset v1 is registered.
    // ===================================================================

    let mut ns = SystemNamespace::new();

    // Certified placement policy requiring dc:tokyo tag.
    let policy_v1 = PlacementPolicy::new(PolicyVersion(1), kr("sensor/"), 3)
        .with_certified(true)
        .with_required_tags([tag("dc:tokyo")].into());
    ns.set_placement_policy(policy_v1);

    // Manually set the initial authority definition for the 3 nodes.
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr("sensor/"),
        authority_nodes: vec![node_id("n1"), node_id("n2"), node_id("n3")],
    });

    let shared_ns = wrap_ns(ns);

    // Create per-node AppState instances sharing the same namespace.
    let state_n1 = test_state_with_ns(node_id("n1"), shared_ns.clone());
    let state_n2 = test_state_with_ns(node_id("n2"), shared_ns.clone());
    let state_n3 = test_state_with_ns(node_id("n3"), shared_ns.clone());

    // Set up epoch manager and keyset v1.
    let epoch_config = EpochConfig {
        duration_secs: 86400, // 24h epochs
        grace_epochs: 7,
    };
    let base_secs = 1_700_000_000;
    let mut epoch_manager = EpochManager::new(epoch_config.clone(), base_secs);

    let (sk_n1_v1, vk_n1_v1) = make_key_pair();
    let (sk_n2_v1, vk_n2_v1) = make_key_pair();
    let (_sk_n3_v1, vk_n3_v1) = make_key_pair();

    epoch_manager
        .rotate_keyset(
            base_secs,
            vec![
                (node_id("n1"), vk_n1_v1),
                (node_id("n2"), vk_n2_v1),
                (node_id("n3"), vk_n3_v1),
            ],
        )
        .unwrap();

    // ===================================================================
    // Phase 2: Write data (eventual + certified)
    //
    // Write eventual data on multiple nodes and create a certified pending
    // write. Certify it using v1 authority frontiers.
    // ===================================================================

    // Eventual writes on n1.
    {
        let mut api = state_n1.eventual.lock().await;
        api.eventual_counter_inc("sensor/temp-a").unwrap();
        api.eventual_counter_inc("sensor/temp-a").unwrap();
        api.eventual_set_add("sensor/tags", "location:roof".into())
            .unwrap();
    }

    // Eventual writes on n2.
    {
        let mut api = state_n2.eventual.lock().await;
        api.eventual_counter_inc("sensor/temp-b").unwrap();
    }

    // Certified write on n1.
    {
        let mut api = state_n1.certified.lock().await;
        api.certified_write(
            "sensor/calibrated".into(),
            counter_value(10),
            OnTimeout::Pending,
        )
        .unwrap();

        // Get the write timestamp so we can advance frontiers past it.
        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Advance n1 and n2 authority frontiers past the write (majority = 2 of 3).
        api.update_frontier(make_frontier_v("n1", write_ts + 100, 0, "sensor/", 1));
        api.update_frontier(make_frontier_v("n2", write_ts + 200, 0, "sensor/", 1));
        api.process_certifications();

        // Verify the write is now certified.
        assert_eq!(
            api.pending_writes()[0].status,
            CertificationStatus::Certified,
            "Phase 2: initial write should be certified with 2/3 authority frontiers"
        );
    }

    // Verify proof can be verified with keyset v1.
    {
        let api = state_n1.certified.lock().await;
        let read = api.get_certified("sensor/calibrated");
        assert_eq!(read.status, CertificationStatus::Certified);

        let proof = read
            .proof
            .as_ref()
            .expect("proof should exist when certified");
        assert_eq!(proof.policy_version, PolicyVersion(1));

        // Build a real MajorityCertificate signed with v1 keys for verification.
        let message = create_certificate_message(
            &proof.key_range,
            &proof.frontier_hlc,
            &proof.policy_version,
        );
        let mut cert = MajorityCertificate::new(
            proof.key_range.clone(),
            proof.frontier_hlc.clone(),
            proof.policy_version,
            KeysetVersion(1),
        );
        let sig_n1 = sign_message(&sk_n1_v1, &message);
        cert.add_signature(AuthoritySignature {
            authority_id: node_id("n1"),
            public_key: vk_n1_v1,
            signature: sig_n1,
            keyset_version: KeysetVersion(1),
        });
        let sig_n2 = sign_message(&sk_n2_v1, &message);
        cert.add_signature(AuthoritySignature {
            authority_id: node_id("n2"),
            public_key: vk_n2_v1,
            signature: sig_n2,
            keyset_version: KeysetVersion(1),
        });

        let signed_bundle = ProofBundle {
            key_range: proof.key_range.clone(),
            frontier_hlc: proof.frontier_hlc.clone(),
            policy_version: proof.policy_version,
            contributing_authorities: vec![node_id("n1"), node_id("n2")],
            total_authorities: 3,
            certificate: Some(cert),
        };

        let verify_result = verify_proof_with_registry(
            &signed_bundle,
            epoch_manager.registry(),
            epoch_manager.current_epoch(base_secs),
            epoch_manager.config(),
        );
        assert!(
            verify_result.valid,
            "Phase 2: v1-signed proof should be valid at epoch 0"
        );
    }

    // Sync n1 -> n2, n1 -> n3 so all nodes have n1's eventual data.
    let zero_frontier = hlc(0, 0, "");
    sync_delta(&state_n1, &state_n2, zero_frontier.clone()).await;
    sync_delta(&state_n1, &state_n3, zero_frontier.clone()).await;
    // Sync n2 -> n1, n2 -> n3 so all nodes have n2's data.
    sync_delta(&state_n2, &state_n1, zero_frontier.clone()).await;
    sync_delta(&state_n2, &state_n3, zero_frontier.clone()).await;

    // Verify all 3 nodes have converged on eventual data.
    for (name, state) in [("n1", &state_n1), ("n2", &state_n2), ("n3", &state_n3)] {
        let api = state.eventual.lock().await;
        assert!(
            api.get_eventual("sensor/temp-a").is_some(),
            "{name} should have sensor/temp-a after sync"
        );
        assert!(
            api.get_eventual("sensor/temp-b").is_some(),
            "{name} should have sensor/temp-b after sync"
        );
        assert!(
            api.get_eventual("sensor/tags").is_some(),
            "{name} should have sensor/tags after sync"
        );
    }

    // ===================================================================
    // Phase 3: Authority reconfiguration
    //
    // n4 joins the cluster. Policy changes to require "dc:tokyo" +
    // "tier:primary". n3 (which only has dc:tokyo) is excluded from the
    // new authority set. n4 (which has both tags) is included.
    // ===================================================================

    let state_n4 = test_state_with_ns(node_id("n4"), shared_ns.clone());

    let cluster_nodes = Arc::new(RwLock::new(vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo", "tier:primary"]),
        make_node("n2", NodeMode::Store, &["dc:tokyo", "tier:primary"]),
        make_node("n3", NodeMode::Store, &["dc:tokyo"]),
        make_node("n4", NodeMode::Store, &["dc:tokyo", "tier:primary"]),
    ]));

    // Update placement policy: now requires both dc:tokyo AND tier:primary.
    {
        let mut ns = shared_ns.write().unwrap();
        let policy_v2 = PlacementPolicy::new(PolicyVersion(2), kr("sensor/"), 3)
            .with_certified(true)
            .with_required_tags([tag("dc:tokyo"), tag("tier:primary")].into());
        ns.set_placement_policy(policy_v2);

        // Recalculate authorities based on new policy + cluster nodes.
        let nodes = cluster_nodes.read().unwrap().clone();
        let changed = ns.recalculate_authorities(&nodes);
        assert!(
            changed > 0,
            "Phase 3: authority recalculation should detect changes"
        );

        // Verify the new authority set: n1, n2, n4 (all have both tags).
        // n3 only has dc:tokyo, so it is excluded.
        let def = ns
            .get_authority_definition("sensor/")
            .expect("authority definition should exist");
        assert_eq!(
            def.authority_nodes.len(),
            3,
            "Phase 3: new authority set should have 3 members"
        );
        assert!(
            def.authority_nodes.contains(&node_id("n1")),
            "Phase 3: n1 should be in new authority set"
        );
        assert!(
            def.authority_nodes.contains(&node_id("n2")),
            "Phase 3: n2 should be in new authority set"
        );
        assert!(
            def.authority_nodes.contains(&node_id("n4")),
            "Phase 3: n4 should be in new authority set"
        );
        assert!(
            !def.authority_nodes.contains(&node_id("n3")),
            "Phase 3: n3 should NOT be in new authority set (missing tier:primary)"
        );
    }

    // Fence old policy version in the certified API to prevent cross-version
    // frontier pollution.
    {
        let mut api = state_n1.certified.lock().await;
        api.fence_version(&kr("sensor/"), PolicyVersion(1));
        assert!(
            api.is_version_fenced(&kr("sensor/"), &PolicyVersion(1)),
            "Phase 3: v1 should be fenced after transition"
        );
    }

    // ===================================================================
    // Phase 4: Key rotation
    //
    // Register keyset v2 (simulating epoch rotation). v1 is still within
    // the grace period. Verify mixed-version signatures work during grace
    // and that expired keysets are properly rejected.
    // ===================================================================

    let epoch3_secs = base_secs + 86400 * 3; // 3 days later -> epoch 3.
    let (_sk_n1_v2, vk_n1_v2) = make_key_pair();
    let (sk_n2_v2, vk_n2_v2) = make_key_pair();
    let (sk_n4_v2, vk_n4_v2) = make_key_pair();

    epoch_manager
        .rotate_keyset(
            epoch3_secs,
            vec![
                (node_id("n1"), vk_n1_v2),
                (node_id("n2"), vk_n2_v2),
                (node_id("n4"), vk_n4_v2),
            ],
        )
        .unwrap();

    assert_eq!(
        epoch_manager.registry().current_version(),
        KeysetVersion(2),
        "Phase 4: keyset version should be 2 after rotation"
    );

    // Verify v1 is still valid during grace period (grace_epochs=7, registered at
    // epoch 0, current epoch 5 -> 5 <= 0+7 -> valid).
    let epoch5_time = base_secs + 86400 * 5;
    assert!(
        epoch_manager
            .validate_keyset_version(&KeysetVersion(1), epoch5_time)
            .is_ok(),
        "Phase 4: v1 should still be valid at epoch 5 (within grace period)"
    );

    // Verify v1 will be rejected after grace expiry (epoch 8 -> 8 > 0+7 -> expired).
    let epoch8_time = base_secs + 86400 * 8;
    assert!(
        epoch_manager
            .validate_keyset_version(&KeysetVersion(1), epoch8_time)
            .is_err(),
        "Phase 4: v1 should be rejected at epoch 8 (beyond grace period)"
    );

    // Build a mixed-version certificate: n1 signs with v1 (hasn't upgraded),
    // n2 and n4 sign with v2 (already upgraded).
    let new_proof_kr = kr("sensor/");
    let new_proof_hlc = hlc(1_700_000_100_000, 0, "n1");
    let new_proof_pv = PolicyVersion(2);
    let message = create_certificate_message(&new_proof_kr, &new_proof_hlc, &new_proof_pv);

    let mut mixed_cert = MajorityCertificate::new(
        new_proof_kr.clone(),
        new_proof_hlc.clone(),
        new_proof_pv,
        KeysetVersion(2),
    );

    // n1 signs with v1 key (still valid in grace period).
    let sig_n1_old = sign_message(&sk_n1_v1, &message);
    mixed_cert.add_signature(AuthoritySignature {
        authority_id: node_id("n1"),
        public_key: vk_n1_v1,
        signature: sig_n1_old,
        keyset_version: KeysetVersion(1),
    });

    // n2 signs with v2 key.
    let sig_n2_new = sign_message(&sk_n2_v2, &message);
    mixed_cert.add_signature(AuthoritySignature {
        authority_id: node_id("n2"),
        public_key: vk_n2_v2,
        signature: sig_n2_new,
        keyset_version: KeysetVersion(2),
    });

    // n4 signs with v2 key.
    let sig_n4_new = sign_message(&sk_n4_v2, &message);
    mixed_cert.add_signature(AuthoritySignature {
        authority_id: node_id("n4"),
        public_key: vk_n4_v2,
        signature: sig_n4_new,
        keyset_version: KeysetVersion(2),
    });

    let mixed_bundle = ProofBundle {
        key_range: new_proof_kr.clone(),
        frontier_hlc: new_proof_hlc.clone(),
        policy_version: new_proof_pv,
        contributing_authorities: vec![node_id("n1"), node_id("n2"), node_id("n4")],
        total_authorities: 3,
        certificate: Some(mixed_cert),
    };

    // During grace period (epoch 5), mixed v1+v2 signatures are valid.
    let result = verify_proof_with_registry(
        &mixed_bundle,
        epoch_manager.registry(),
        epoch_manager.current_epoch(epoch5_time),
        epoch_manager.config(),
    );
    assert!(
        result.valid,
        "Phase 4: mixed v1+v2 proof should be valid during grace period"
    );
    assert!(result.has_majority);

    // After grace expiry (epoch 8), v1 signatures become invalid, and the
    // proof should fail because n1's v1 signature is no longer accepted.
    let result_expired = verify_proof_with_registry(
        &mixed_bundle,
        epoch_manager.registry(),
        epoch_manager.current_epoch(epoch8_time),
        epoch_manager.config(),
    );
    assert!(
        !result_expired.valid,
        "Phase 4: mixed proof should be invalid after v1 grace period expiry"
    );

    // ===================================================================
    // Phase 5: Delta sync
    //
    // n4 (newly joined) catches up by pulling deltas from n1. Verify
    // that n4 receives all data and that frontier advances correctly.
    // Also test incremental delta (second pull only gets new entries).
    // ===================================================================

    // n4 pulls from n1 with zero frontier (full delta).
    let delta1 = sync_delta(&state_n1, &state_n4, zero_frontier.clone()).await;
    assert!(
        !delta1.entries.is_empty(),
        "Phase 5: n4 should receive entries from n1 via delta sync"
    );
    let frontier_after_first_pull = delta1.sender_frontier.clone();
    assert!(
        frontier_after_first_pull.is_some(),
        "Phase 5: delta response should include sender frontier"
    );

    // Verify n4 now has n1's data.
    {
        let api = state_n4.eventual.lock().await;
        assert!(
            api.get_eventual("sensor/temp-a").is_some(),
            "Phase 5: n4 should have sensor/temp-a after delta sync from n1"
        );
        assert!(
            api.get_eventual("sensor/tags").is_some(),
            "Phase 5: n4 should have sensor/tags after delta sync from n1"
        );
    }

    // n4 also pulls from n2 to get n2's data.
    sync_delta(&state_n2, &state_n4, zero_frontier.clone()).await;
    {
        let api = state_n4.eventual.lock().await;
        assert!(
            api.get_eventual("sensor/temp-b").is_some(),
            "Phase 5: n4 should have sensor/temp-b after delta sync from n2"
        );
    }

    // Write new data on n1 after the initial sync.
    {
        let mut api = state_n1.eventual.lock().await;
        api.eventual_counter_inc("sensor/temp-c").unwrap();
    }

    // Incremental delta: n4 pulls from n1 using the frontier from the first pull.
    // Only the new entry should be returned.
    let delta2 = sync_delta(&state_n1, &state_n4, frontier_after_first_pull.unwrap()).await;

    let delta2_keys: Vec<&str> = delta2.entries.iter().map(|e| e.key.as_str()).collect();
    assert!(
        delta2_keys.contains(&"sensor/temp-c"),
        "Phase 5: incremental delta should contain newly-written sensor/temp-c"
    );
    // The old entries (temp-a, tags) should NOT be in the incremental delta.
    assert!(
        !delta2_keys.contains(&"sensor/temp-a"),
        "Phase 5: incremental delta should NOT contain previously-synced sensor/temp-a"
    );

    // Verify n4 now has the new data too.
    {
        let api = state_n4.eventual.lock().await;
        assert!(
            api.get_eventual("sensor/temp-c").is_some(),
            "Phase 5: n4 should have sensor/temp-c after incremental delta sync"
        );
    }

    // ===================================================================
    // Phase 6: Final verification
    //
    // 6a. Eventual data convergence across all nodes.
    // 6b. Certified judgment with new authority set (policy v2).
    // 6c. Old authority set (v1 frontiers) cannot certify new writes.
    // 6d. Delta sync frontier continues to advance.
    // ===================================================================

    // 6a. Full convergence: sync remaining data across all 4 nodes.
    // n3 -> n4, n4 -> n3 (n3 and n4 need each other's data).
    sync_delta(&state_n3, &state_n4, zero_frontier.clone()).await;
    sync_delta(&state_n4, &state_n3, zero_frontier.clone()).await;
    // n1 -> n3, n1 -> n2 to propagate temp-c.
    sync_delta(&state_n1, &state_n3, zero_frontier.clone()).await;
    sync_delta(&state_n1, &state_n2, zero_frontier.clone()).await;

    let expected_keys = [
        "sensor/temp-a",
        "sensor/temp-b",
        "sensor/tags",
        "sensor/temp-c",
    ];
    for (name, state) in [
        ("n1", &state_n1),
        ("n2", &state_n2),
        ("n3", &state_n3),
        ("n4", &state_n4),
    ] {
        let api = state.eventual.lock().await;
        for key in &expected_keys {
            assert!(
                api.get_eventual(key).is_some(),
                "Phase 6a: {name} should have {key} after full convergence"
            );
        }
    }

    // Verify counter values converged correctly.
    for (name, state) in [
        ("n1", &state_n1),
        ("n2", &state_n2),
        ("n3", &state_n3),
        ("n4", &state_n4),
    ] {
        let api = state.eventual.lock().await;
        match api.get_eventual("sensor/temp-a") {
            Some(CrdtValue::Counter(c)) => {
                assert_eq!(c.value(), 2, "Phase 6a: {name} sensor/temp-a should be 2")
            }
            other => panic!("Phase 6a: {name} expected Counter for temp-a, got {other:?}"),
        }
        match api.get_eventual("sensor/temp-b") {
            Some(CrdtValue::Counter(c)) => {
                assert_eq!(c.value(), 1, "Phase 6a: {name} sensor/temp-b should be 1")
            }
            other => panic!("Phase 6a: {name} expected Counter for temp-b, got {other:?}"),
        }
    }

    // 6b. Certified judgment with new authority set (v2).
    // Write a new certified entry and certify it using v2 frontiers from
    // the new authority set (n1, n2, n4).
    {
        let mut api = state_n1.certified.lock().await;
        api.certified_write(
            "sensor/v2-entry".into(),
            counter_value(42),
            OnTimeout::Pending,
        )
        .unwrap();

        let pw_idx = api.pending_writes().len() - 1;
        let write_ts = api.pending_writes()[pw_idx].timestamp.physical;
        assert_eq!(
            api.pending_writes()[pw_idx].policy_version,
            PolicyVersion(2),
            "Phase 6b: new write should be under policy version 2"
        );

        // Advance new authority frontiers (n1 and n2) under v2.
        api.update_frontier(make_frontier_v("n1", write_ts + 100, 0, "sensor/", 2));
        api.update_frontier(make_frontier_v("n2", write_ts + 200, 0, "sensor/", 2));
        api.process_certifications();

        assert_eq!(
            api.get_certification_status("sensor/v2-entry"),
            CertificationStatus::Certified,
            "Phase 6b: v2 write should be certified by new authority set"
        );
    }

    // 6c. Old authority set (v1 frontiers) cannot certify a v2 write.
    // Create a separate certified API to test isolation.
    {
        let mut api = state_n2.certified.lock().await;
        // Fence v1 on n2's API as well.
        api.fence_version(&kr("sensor/"), PolicyVersion(1));

        api.certified_write(
            "sensor/isolation-test".into(),
            counter_value(99),
            OnTimeout::Pending,
        )
        .unwrap();

        let pw_idx = api.pending_writes().len() - 1;
        let write_ts = api.pending_writes()[pw_idx].timestamp.physical;

        // Attempt to use v1 frontiers (should be blocked by fencing).
        let blocked1 = api.update_frontier(make_frontier_v("n1", write_ts + 500, 0, "sensor/", 1));
        let blocked2 = api.update_frontier(make_frontier_v("n3", write_ts + 500, 0, "sensor/", 1));
        assert!(
            !blocked1,
            "Phase 6c: v1 frontier update should be rejected after fencing"
        );
        assert!(
            !blocked2,
            "Phase 6c: v1 frontier update should be rejected after fencing"
        );

        api.process_certifications();
        assert_eq!(
            api.get_certification_status("sensor/isolation-test"),
            CertificationStatus::Pending,
            "Phase 6c: v1 frontiers must NOT certify a v2 write (version isolation)"
        );

        // Now use v2 frontiers from the correct authority set.
        api.update_frontier(make_frontier_v("n1", write_ts + 100, 0, "sensor/", 2));
        api.update_frontier(make_frontier_v("n4", write_ts + 200, 0, "sensor/", 2));
        api.process_certifications();

        assert_eq!(
            api.get_certification_status("sensor/isolation-test"),
            CertificationStatus::Certified,
            "Phase 6c: v2 frontiers from new authorities should certify the write"
        );
    }

    // 6d. Delta sync frontier continues to advance after all operations.
    // Write one more entry and verify delta sync still works.
    {
        let mut api = state_n1.eventual.lock().await;
        api.eventual_counter_inc("sensor/final-check").unwrap();
    }

    // Capture n1's current frontier.
    let n1_frontier = {
        let api = state_n1.eventual.lock().await;
        api.store().current_frontier().unwrap()
    };

    // Write another entry after the frontier.
    {
        let mut api = state_n1.eventual.lock().await;
        api.eventual_counter_inc("sensor/post-frontier").unwrap();
    }

    // Delta pull using n1's frontier should only get the post-frontier entry.
    let final_delta = sync_delta(&state_n1, &state_n4, n1_frontier).await;
    let final_keys: Vec<&str> = final_delta.entries.iter().map(|e| e.key.as_str()).collect();
    assert!(
        final_keys.contains(&"sensor/post-frontier"),
        "Phase 6d: delta sync should deliver post-frontier entries"
    );
    assert!(
        !final_keys.contains(&"sensor/final-check"),
        "Phase 6d: delta sync should NOT deliver entries at or before the frontier"
    );
    assert!(
        final_delta.sender_frontier.is_some(),
        "Phase 6d: frontier should continue advancing in delta responses"
    );
}

// ===========================================================================
// Test: NodeRunner-driven authority reconfig + version fencing
// ===========================================================================

/// Verifies that NodeRunner automatically detects membership changes and
/// policy version transitions, then correctly fences old versions, even
/// when key rotation has already occurred.
#[tokio::test]
async fn node_runner_reconfig_and_version_fencing_with_rotation() {
    // Setup: n1 is an authority for "data/" under policy v1.
    let mut ns = SystemNamespace::new();
    let policy_v1 = PlacementPolicy::new(PolicyVersion(1), kr("data/"), 1)
        .with_certified(true)
        .with_required_tags([tag("active")].into());
    ns.set_placement_policy(policy_v1);
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr("data/"),
        authority_nodes: vec![node_id("n1")],
    });

    let shared_ns = wrap_ns(ns);
    let mut api = CertifiedApi::new(node_id("n1"), shared_ns.clone());

    // Write a pending entry under v1.
    api.certified_write("data/key-v1".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    assert_eq!(api.pending_writes()[0].policy_version, PolicyVersion(1));

    let shared_api = wrap_api(api);

    let cluster_nodes = Arc::new(RwLock::new(vec![make_node(
        "n1",
        NodeMode::Store,
        &["active"],
    )]));

    let mut runner = NodeRunner::with_cluster_nodes(
        node_id("n1"),
        shared_api.clone(),
        CompactionEngine::with_defaults(),
        fast_config(),
        default_metrics(),
        cluster_nodes.clone(),
    )
    .await;

    assert!(runner.is_authority(), "n1 should be authority initially");

    // Let the runner process a few ticks to certify the v1 write.
    let handle = runner.shutdown_handle();
    let ns_clone = shared_ns.clone();
    let api_clone = shared_api.clone();
    let nodes_ref = cluster_nodes.clone();

    tokio::spawn(async move {
        // Poll until the v1 write is certified (instead of fixed sleep).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            {
                let api = api_clone.lock().await;
                let writes = api.pending_writes();
                if writes.is_empty()
                    || writes
                        .iter()
                        .all(|w| w.status == CertificationStatus::Certified)
                {
                    break;
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for v1 write to be certified");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Transition to v2: change policy.
        {
            let mut ns = ns_clone.write().unwrap();
            let policy_v2 = PlacementPolicy::new(PolicyVersion(2), kr("data/"), 1)
                .with_certified(true)
                .with_required_tags([tag("active")].into());
            ns.set_placement_policy(policy_v2);
        }

        // Also add n2 to the cluster (membership change).
        {
            let mut nodes = nodes_ref.write().unwrap();
            nodes.push(make_node("n2", NodeMode::Store, &["active"]));
        }

        // Poll until NodeRunner detects the version change and fences v1.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            {
                let api = api_clone.lock().await;
                if api.is_version_fenced(&kr("data/"), &PolicyVersion(1)) {
                    break;
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for NodeRunner to fence v1");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Verify authority set was recalculated to include n2.
        let api = api_clone.lock().await;
        let ns = api.namespace().read().unwrap();
        let def = ns.get_authority_definition("data/").unwrap();
        assert!(
            def.authority_nodes.contains(&node_id("n2")),
            "n2 should be added to authority set after join"
        );

        let _ = handle.send(true);
    });

    runner.run().await;

    // After the runner exits, verify the v1 write was certified
    // (frontier reporter would have auto-certified it).
    let api = shared_api.lock().await;
    assert_eq!(
        api.pending_writes()[0].status,
        CertificationStatus::Certified,
        "v1 pending write should be auto-certified by frontier reporter"
    );
}

// ===========================================================================
// Test: Expired keyset cannot produce valid certified proof
// ===========================================================================

/// Verifies that a proof signed entirely with an expired keyset version is
/// rejected, even if the authority set is correct.
#[test]
fn expired_keyset_proof_rejected_after_reconfig() {
    let kr_val = kr("sensor/");
    let hlc_val = hlc(1_700_000_050_000, 0, "n1");
    let pv = PolicyVersion(2);
    let message = create_certificate_message(&kr_val, &hlc_val, &pv);

    let config = EpochConfig {
        duration_secs: 86400,
        grace_epochs: 3,
    };
    let base_secs = 1_700_000_000;
    let mut manager = EpochManager::new(config.clone(), base_secs);

    // v1 keyset registered at epoch 0 with old authority set.
    let (sk_old, vk_old) = make_key_pair();
    let id_old = node_id("n3"); // n3 was removed from authority set
    manager
        .rotate_keyset(base_secs, vec![(id_old.clone(), vk_old)])
        .unwrap();

    // v2 keyset registered at epoch 2 with new authority set.
    let (_, vk_new) = make_key_pair();
    let id_new = node_id("n4"); // n4 joined the authority set
    manager
        .rotate_keyset(base_secs + 86400 * 2, vec![(id_new.clone(), vk_new)])
        .unwrap();

    // Build a proof signed with the old key (v1) from the old authority (n3).
    let mut cert = MajorityCertificate::new(kr_val.clone(), hlc_val.clone(), pv, KeysetVersion(1));
    let sig = sign_message(&sk_old, &message);
    cert.add_signature(AuthoritySignature {
        authority_id: id_old.clone(),
        public_key: vk_old,
        signature: sig,
        keyset_version: KeysetVersion(1),
    });

    let bundle = ProofBundle {
        key_range: kr_val,
        frontier_hlc: hlc_val,
        policy_version: pv,
        contributing_authorities: vec![id_old],
        total_authorities: 1,
        certificate: Some(cert),
    };

    // At epoch 3 (boundary): v1 registered at epoch 0, grace=3 -> 3 <= 0+3 -> valid.
    let result = verify_proof_with_registry(&bundle, manager.registry(), 3, manager.config());
    assert!(
        result.valid,
        "v1 should still be valid at grace boundary (epoch 3)"
    );

    // At epoch 4: v1 expired (4 > 0+3).
    let result_expired =
        verify_proof_with_registry(&bundle, manager.registry(), 4, manager.config());
    assert!(
        !result_expired.valid,
        "v1 should be rejected after grace period (epoch 4)"
    );
}

// ===========================================================================
// Test: Delta sync full-sync fallback scenario
// ===========================================================================

/// Simulates a scenario where delta sync is done from zero frontier
/// (equivalent to full-sync fallback) and verifies complete data transfer.
#[tokio::test]
async fn delta_sync_full_fallback_after_reconfig() {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr(""),
        authority_nodes: vec![node_id("auth-1"), node_id("auth-2"), node_id("auth-3")],
    });

    let shared_ns = wrap_ns(ns);

    let state_old = test_state_with_ns(node_id("old-node"), shared_ns.clone());
    let state_new = test_state_with_ns(node_id("new-node"), shared_ns.clone());

    // Old node has accumulated data over time.
    {
        let mut api = state_old.eventual.lock().await;
        api.eventual_counter_inc("counter-a").unwrap();
        api.eventual_counter_inc("counter-a").unwrap();
        api.eventual_counter_inc("counter-a").unwrap();
        api.eventual_set_add("users", "alice".into()).unwrap();
        api.eventual_set_add("users", "bob".into()).unwrap();
        api.eventual_register_set("config", "value-1".into())
            .unwrap();
    }

    // New node joins and does a full delta pull (zero frontier = full sync).
    let delta = sync_delta(&state_old, &state_new, hlc(0, 0, "")).await;

    assert_eq!(
        delta.entries.len(),
        3,
        "full delta should contain all 3 keys"
    );
    assert!(delta.sender_frontier.is_some(), "should include frontier");

    // Verify all data transferred correctly.
    {
        let api = state_new.eventual.lock().await;

        match api.get_eventual("counter-a") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 3),
            other => panic!("expected Counter(3), got {other:?}"),
        }

        match api.get_eventual("users") {
            Some(CrdtValue::Set(s)) => {
                assert!(s.contains(&"alice".to_string()));
                assert!(s.contains(&"bob".to_string()));
            }
            other => panic!("expected Set with alice+bob, got {other:?}"),
        }

        match api.get_eventual("config") {
            Some(CrdtValue::Register(r)) => {
                assert_eq!(r.get(), Some(&"value-1".to_string()));
            }
            other => panic!("expected Register(value-1), got {other:?}"),
        }
    }

    // Write new data on old node after the initial sync.
    {
        let mut api = state_old.eventual.lock().await;
        api.eventual_counter_inc("counter-b").unwrap();
    }

    // Incremental pull using frontier from first sync.
    let frontier = delta.sender_frontier.unwrap();
    let delta2 = sync_delta(&state_old, &state_new, frontier).await;

    assert_eq!(
        delta2.entries.len(),
        1,
        "incremental delta should only have 1 new entry"
    );
    assert_eq!(delta2.entries[0].key, "counter-b");

    // Final convergence check.
    {
        let api = state_new.eventual.lock().await;
        assert!(
            api.get_eventual("counter-b").is_some(),
            "new node should have counter-b after incremental sync"
        );
    }
}

// ===========================================================================
// Test: NodeRunner delta-fail -> full-sync fallback via real HTTP servers
// ===========================================================================

/// Verifies that NodeRunner's sync loop correctly falls back to full sync
/// when the delta endpoint is unavailable on a peer.
///
/// Setup:
/// 1. Start a "legacy" HTTP server that only serves `/api/internal/keys`
///    (no delta endpoint — returns 404 for `/api/internal/sync/delta`).
/// 2. Write data to the legacy server's store.
/// 3. Create a NodeRunner with SyncClient pointing at the legacy peer.
/// 4. Inject a stale peer frontier so the runner *tries* delta first.
/// 5. Run the runner; it should detect delta failure and fall back to
///    full sync via `/api/internal/keys`.
/// 6. Assert the data arrived at the local node.
#[tokio::test]
async fn node_runner_delta_fail_falls_back_to_full_sync() {
    use asteroidb_poc::http::handlers::{internal_keys, internal_sync};
    use asteroidb_poc::network::sync::SyncClient;
    use asteroidb_poc::network::{PeerConfig, PeerRegistry};
    use axum::routing::{get, post};

    // -- Legacy peer: serves /api/internal/keys but NOT /api/internal/sync/delta --
    let mut ns_legacy = SystemNamespace::new();
    ns_legacy.set_authority_definition(AuthorityDefinition {
        key_range: kr(""),
        authority_nodes: vec![node_id("auth-1")],
    });
    let ns_legacy = Arc::new(RwLock::new(ns_legacy));
    let legacy_state = Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(node_id("legacy")))),
        certified: Arc::new(Mutex::new(CertifiedApi::new(
            node_id("legacy"),
            ns_legacy.clone(),
        ))),
        namespace: ns_legacy,
        metrics: Arc::new(RuntimeMetrics::default()),
        peers: None,
        peer_persist_path: None,
        internal_token: None,
    });

    // Write data to the legacy peer.
    {
        let mut api = legacy_state.eventual.lock().await;
        api.eventual_counter_inc("sync-key-1").unwrap();
        api.eventual_counter_inc("sync-key-1").unwrap();
        api.eventual_set_add("sync-key-2", "val-a".into()).unwrap();
    }

    // Build a router with only /api/internal/keys and /api/internal/sync
    // (no /api/internal/sync/delta → delta pull will get 404).
    let legacy_app = axum::Router::new()
        .route("/api/internal/keys", get(internal_keys))
        .route("/api/internal/sync", post(internal_sync))
        .with_state(legacy_state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let legacy_addr = listener.local_addr().unwrap();
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, legacy_app).await.unwrap();
    });

    // -- Local node with NodeRunner + SyncClient --
    let mut ns_local = SystemNamespace::new();
    ns_local.set_authority_definition(AuthorityDefinition {
        key_range: kr(""),
        authority_nodes: vec![node_id("auth-1")],
    });
    let ns_local = Arc::new(RwLock::new(ns_local));
    let local_api = Arc::new(Mutex::new(EventualApi::new(node_id("local"))));
    let certified_api = Arc::new(Mutex::new(CertifiedApi::new(
        node_id("local"),
        ns_local.clone(),
    )));

    let peer_registry = PeerRegistry::new(
        node_id("local"),
        vec![PeerConfig {
            node_id: node_id("legacy"),
            addr: legacy_addr.to_string(),
        }],
    )
    .unwrap();

    let sync_client = SyncClient::new(peer_registry);

    let config = NodeRunnerConfig {
        certification_interval: Duration::from_millis(10),
        cleanup_interval: Duration::from_secs(60),
        compaction_check_interval: Duration::from_secs(60),
        frontier_report_interval: Duration::from_millis(10),
        sync_interval: Some(Duration::from_millis(20)),
    };

    let metrics = Arc::new(RuntimeMetrics::default());

    let mut runner = NodeRunner::with_sync(
        node_id("local"),
        certified_api.clone(),
        CompactionEngine::with_defaults(),
        config,
        sync_client,
        local_api.clone(),
        metrics.clone(),
    )
    .await;

    // Inject a stale peer frontier so the runner *tries* delta first.
    // This forces the delta-fail -> full-sync fallback path because the
    // legacy server has no /api/internal/sync/delta endpoint.
    runner.inject_peer_frontier(&legacy_addr.to_string(), hlc(1, 0, "stale"));

    // Run NodeRunner; the sync cycle will:
    //   1. See injected peer frontier → try delta pull → 404 → retry → 404
    //   2. Fall back to full sync via /api/internal/keys → data arrives
    let shutdown = runner.shutdown_handle();
    let metrics_check = metrics.clone();
    tokio::spawn(async move {
        // Poll until local node has the data from legacy peer.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            {
                let api = local_api.lock().await;
                let has_key1 = api.get_eventual("sync-key-1").is_some();
                let has_key2 = api.get_eventual("sync-key-2").is_some();
                if has_key1 && has_key2 {
                    break;
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for full-sync fallback to transfer data");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Verify the data is correct.
        {
            let api = local_api.lock().await;
            match api.get_eventual("sync-key-1") {
                Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
                other => panic!("expected Counter(2), got {other:?}"),
            }
            match api.get_eventual("sync-key-2") {
                Some(CrdtValue::Set(s)) => assert!(s.contains(&"val-a".to_string())),
                other => panic!("expected Set with val-a, got {other:?}"),
            }
        }

        // Verify the fallback path was actually taken via metrics.
        let fallback_count = metrics_check
            .sync_fallback_total
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            fallback_count > 0,
            "sync_fallback_total should be > 0, proving delta-fail -> full-sync path was taken"
        );

        let _ = shutdown.send(true);
    });

    runner.run().await;
    server_handle.abort();
}
