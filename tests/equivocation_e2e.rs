//! End-to-end tests for equivocation / split-view detection.
//!
//! Runs in-process HTTP nodes (same scaffolding as `signing_pipeline_e2e`)
//! and exercises the full detection pipeline: conflicting signed frontier
//! attestations arriving via `POST /api/internal/frontiers` (direct lane and
//! relayed `observed` gossip lane), evidence storage and persistence, the
//! `GET /api/authority/equivocations` operator endpoint, metrics, and the
//! opt-in exclusion of accused authorities from certificate assembly.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::authority::ack_frontier::AckFrontier;
use asteroidb_poc::authority::certificate::{EpochConfig, KeysetRegistry, KeysetVersion};
use asteroidb_poc::authority::equivocation::{
    EquivocationDetector, GOSSIP_SAMPLE_MAX, MAX_OBSERVED_PER_REQUEST, ObservedAttestation,
};
use asteroidb_poc::authority::frontier_sig::{
    FrontierSignature, NodeSigner, verify_frontier_signature,
};
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::network::frontier_sync::FrontierSyncClient;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

const TOKEN: &str = "equivocation-e2e-secret";

fn node_id(s: &str) -> NodeId {
    NodeId(s.into())
}

#[cfg(feature = "native-crypto")]
fn make_signer(name: &str, byte: u8) -> NodeSigner {
    let mut seed = [0u8; 32];
    seed[0] = byte;
    NodeSigner::from_seed(node_id(name), &seed, false)
}

#[cfg(not(feature = "native-crypto"))]
fn make_signer(name: &str, byte: u8) -> NodeSigner {
    let mut seed = [0u8; 32];
    seed[0] = byte;
    NodeSigner::from_seed(node_id(name), &seed)
}

fn full_registry(signers: &[&NodeSigner]) -> KeysetRegistry {
    let mut registry = KeysetRegistry::new();
    registry
        .register_keyset(
            KeysetVersion(1),
            0,
            signers
                .iter()
                .map(|s| (s.node_id().clone(), s.verifying_key()))
                .collect(),
        )
        .unwrap();
    registry
}

fn default_namespace() -> SystemNamespace {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: String::new(),
        },
        authority_nodes: vec![node_id("auth-1"), node_id("auth-2"), node_id("auth-3")],
        auto_generated: false,
    });
    ns.set_placement_policy(PlacementPolicy::new(
        PolicyVersion(1),
        KeyRange {
            prefix: String::new(),
        },
        3,
    ))
    .unwrap();
    ns
}

struct NodeOpts {
    registry: Option<KeysetRegistry>,
    exclude_accused: bool,
    persist_path: Option<PathBuf>,
}

impl NodeOpts {
    fn with_registry(registry: KeysetRegistry) -> Self {
        Self {
            registry: Some(registry),
            exclude_accused: false,
            persist_path: None,
        }
    }
}

async fn spawn_node(name: &str, opts: NodeOpts) -> (Arc<AppState>, SocketAddr, JoinHandle<()>) {
    let nid = node_id(name);
    let namespace = Arc::new(RwLock::new(default_namespace()));
    let state = Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(nid.clone()))),
        certified: Arc::new(Mutex::new(CertifiedApi::new(nid, Arc::clone(&namespace)))),
        namespace,
        metrics: Arc::new(RuntimeMetrics::default()),
        peers: None,
        peer_persist_path: None,
        namespace_persist_path: None,
        consensus: Arc::new(Mutex::new(ControlPlaneConsensus::new(vec![]))),
        internal_token: Some(TOKEN.to_string()),
        self_node_id: None,
        self_addr: None,
        latency_model: None,
        cluster_nodes: None,
        slo_tracker: Arc::new(asteroidb_poc::ops::slo::SloTracker::new()),
        keyset_registry: opts.registry.map(|r| Arc::new(RwLock::new(r))),
        epoch_config: EpochConfig::default(),
        current_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        require_signed_frontiers: false,
        equivocation: Arc::new(EquivocationDetector::new(opts.persist_path)),
        exclude_accused_authorities: opts.exclude_accused,
    });

    let app = router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (state, addr, handle)
}

fn make_frontier(authority: &str, physical: u64, digest: &str) -> AckFrontier {
    AckFrontier {
        authority_id: node_id(authority),
        frontier_hlc: HlcTimestamp {
            physical,
            logical: 0,
            node_id: authority.into(),
        },
        key_range: KeyRange {
            prefix: String::new(),
        },
        policy_version: PolicyVersion(1),
        digest_hash: digest.into(),
    }
}

fn sign(signer: &NodeSigner, frontier: &AckFrontier) -> FrontierSignature {
    signer.sign_frontier(frontier, KeysetVersion(1))
}

async fn get_json(addr: &SocketAddr, path: &str) -> serde_json::Value {
    reqwest::Client::new()
        .get(format!("http://{addr}{path}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Current wall clock in ms, used as the base for report HLCs so the
/// detector's future-skew guard is not tripped.
fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ---------------------------------------------------------------
// Direct lane: conflicting signed pushes
// ---------------------------------------------------------------

#[tokio::test]
async fn equivocation_detected_via_push_with_verifiable_evidence() {
    let s1 = make_signer("auth-1", 1);
    let s2 = make_signer("auth-2", 2);
    let s3 = make_signer("auth-3", 3);
    let (state, addr, handle) = spawn_node(
        "node-1",
        NodeOpts::with_registry(full_registry(&[&s1, &s2, &s3])),
    )
    .await;
    let client = FrontierSyncClient::with_token(TOKEN.to_string());

    // Fresh node: endpoint reports nothing.
    let report = get_json(&addr, "/api/authority/equivocations").await;
    assert_eq!(report["evidence_count"], 0);
    assert!(report["accused_authorities"].as_array().unwrap().is_empty());

    // auth-1 signs two conflicting digests for the exact same frontier HLC.
    let hlc = wall_ms();
    let f_a = make_frontier("auth-1", hlc, "digest-a");
    let f_b = make_frontier("auth-1", hlc, "digest-b");
    let resp = client
        .push_signed_frontiers(
            &addr.to_string(),
            vec![f_a.clone()],
            vec![Some(sign(&s1, &f_a))],
        )
        .await
        .unwrap();
    assert_eq!(resp.accepted, 1);
    // The conflicting push is still processed normally (no enforcement).
    client
        .push_signed_frontiers(
            &addr.to_string(),
            vec![f_b.clone()],
            vec![Some(sign(&s1, &f_b))],
        )
        .await
        .expect("conflicting push must not be refused (detection, not enforcement)");

    // Metrics reflect the detection.
    let metrics = get_json(&addr, "/api/metrics").await;
    assert_eq!(metrics["equivocation_detected_total"], 1);
    assert_eq!(metrics["equivocation_accused_authorities"], 1);
    assert!(metrics["equivocation_last_detected_ms"].as_u64().unwrap() > 0);

    // The operator endpoint returns the evidence pair...
    let report = get_json(&addr, "/api/authority/equivocations").await;
    assert_eq!(report["accused_authorities"], serde_json::json!(["auth-1"]));
    assert_eq!(report["evidence_count"], 1);
    let ev = &report["evidence"][0];
    assert_eq!(ev["first"]["frontier"]["digest_hash"], "digest-a");
    assert_eq!(ev["second"]["frontier"]["digest_hash"], "digest-b");

    // ...and both halves re-verify against the registry keys end-to-end:
    // the pair is a portable, non-repudiable proof of misbehaviour.
    let registry = full_registry(&[&s1, &s2, &s3]);
    for side in ["first", "second"] {
        let obs: ObservedAttestation = serde_json::from_value(ev[side].clone()).unwrap();
        verify_frontier_signature(
            &obs.frontier,
            &obs.signature,
            &registry,
            0,
            &EpochConfig::default(),
        )
        .expect("evidence must be third-party verifiable");
    }

    // Duplicate conflict is deduped, not double-counted.
    client
        .push_signed_frontiers(
            &addr.to_string(),
            vec![f_b.clone()],
            vec![Some(sign(&s1, &f_b))],
        )
        .await
        .unwrap();
    let metrics = get_json(&addr, "/api/metrics").await;
    assert_eq!(metrics["equivocation_detected_total"], 1);

    // Detector state on the node matches the HTTP view.
    assert!(state.equivocation.is_accused(&node_id("auth-1")));
    handle.abort();
}

#[tokio::test]
async fn invalid_or_unsigned_conflicts_are_not_evidence() {
    let s1 = make_signer("auth-1", 11);
    let rogue = make_signer("auth-1", 99); // wrong key for auth-1
    let (state, addr, handle) =
        spawn_node("node-1", NodeOpts::with_registry(full_registry(&[&s1]))).await;
    let client = FrontierSyncClient::with_token(TOKEN.to_string());

    let hlc = wall_ms();
    let f_a = make_frontier("auth-1", hlc, "digest-a");
    let f_b = make_frontier("auth-1", hlc, "digest-b");

    // Genuine first observation.
    client
        .push_signed_frontiers(
            &addr.to_string(),
            vec![f_a.clone()],
            vec![Some(sign(&s1, &f_a))],
        )
        .await
        .unwrap();

    // Conflict signed with the wrong key: rejected, never becomes evidence.
    let resp = client
        .push_signed_frontiers(
            &addr.to_string(),
            vec![f_b.clone()],
            vec![Some(sign(&rogue, &f_b))],
        )
        .await
        .unwrap();
    assert_eq!(resp.accepted, 0, "invalid signature must be rejected");

    // Unsigned conflicting report in lenient mode: accepted as a frontier
    // but never becomes evidence (no signature, no proof).
    client
        .push_signed_frontiers(&addr.to_string(), vec![f_b.clone()], vec![None])
        .await
        .unwrap();

    let metrics = get_json(&addr, "/api/metrics").await;
    assert_eq!(metrics["equivocation_detected_total"], 0);
    assert!(!state.equivocation.is_accused(&node_id("auth-1")));
    handle.abort();
}

// ---------------------------------------------------------------
// Split-view: relayed observations via the gossip lane
// ---------------------------------------------------------------

#[tokio::test]
async fn split_view_detected_across_nodes_via_observed_lane() {
    let s1 = make_signer("auth-1", 21);
    let s2 = make_signer("auth-2", 22);
    let s3 = make_signer("auth-3", 23); // the equivocator
    let (state1, addr1, h1) = spawn_node(
        "node-1",
        NodeOpts::with_registry(full_registry(&[&s1, &s2, &s3])),
    )
    .await;
    let (state2, addr2, h2) = spawn_node(
        "node-2",
        NodeOpts::with_registry(full_registry(&[&s1, &s2, &s3])),
    )
    .await;
    let client = FrontierSyncClient::with_token(TOKEN.to_string());

    // auth-3 tells node-1 digest A and node-2 digest B for the same HLC.
    let hlc = wall_ms();
    let f_a = make_frontier("auth-3", hlc, "digest-a");
    let f_b = make_frontier("auth-3", hlc, "digest-b");
    client
        .push_signed_frontiers(
            &addr1.to_string(),
            vec![f_a.clone()],
            vec![Some(sign(&s3, &f_a))],
        )
        .await
        .unwrap();
    client
        .push_signed_frontiers(
            &addr2.to_string(),
            vec![f_b.clone()],
            vec![Some(sign(&s3, &f_b))],
        )
        .await
        .unwrap();

    // Neither node can see the conflict alone.
    assert!(!state1.equivocation.is_accused(&node_id("auth-3")));
    assert!(!state2.equivocation.is_accused(&node_id("auth-3")));

    // node-1's report tick gossips its observations to node-2 (this is
    // exactly what NodeRunner::report_frontiers attaches to its push).
    let observed = state1.equivocation.gossip_summaries(GOSSIP_SAMPLE_MAX);
    assert!(!observed.is_empty(), "node-1 must have indexed digest-a");
    let resp = client
        .push_frontiers_with_observations(&addr2.to_string(), vec![], vec![], observed)
        .await
        .unwrap();
    assert_eq!(
        resp.accepted, 0,
        "observations must not touch frontier state"
    );

    // node-2 now holds cross-checkable views and detects the split view.
    assert!(state2.equivocation.is_accused(&node_id("auth-3")));
    let metrics = get_json(&addr2, "/api/metrics").await;
    assert_eq!(metrics["equivocation_detected_total"], 1);
    assert!(metrics["split_view_observations_total"].as_u64().unwrap() >= 1);

    let report = get_json(&addr2, "/api/authority/equivocations").await;
    assert_eq!(report["accused_authorities"], serde_json::json!(["auth-3"]));

    // node-2's gossip now carries both evidence halves, so the accusation
    // propagates back to node-1 on its next exchange.
    let observed = state2.equivocation.gossip_summaries(GOSSIP_SAMPLE_MAX);
    client
        .push_frontiers_with_observations(&addr1.to_string(), vec![], vec![], observed.clone())
        .await
        .unwrap();
    assert!(state1.equivocation.is_accused(&node_id("auth-3")));

    // Evidence halves are re-gossiped forever (never evicted), so their
    // echoes must be deduped *before* signature re-verification: replaying
    // the same sample must not move the split-view counter or re-detect.
    let metrics_before = get_json(&addr1, "/api/metrics").await;
    client
        .push_frontiers_with_observations(&addr1.to_string(), vec![], vec![], observed)
        .await
        .unwrap();
    let metrics_after = get_json(&addr1, "/api/metrics").await;
    assert_eq!(
        metrics_after["split_view_observations_total"],
        metrics_before["split_view_observations_total"],
        "evidence echoes must not inflate split_view_observations_total"
    );
    assert_eq!(
        metrics_after["equivocation_detected_total"],
        metrics_before["equivocation_detected_total"]
    );

    h1.abort();
    h2.abort();
}

// ---------------------------------------------------------------
// Runner wiring: report tick -> peer push -> receiver-side detection
// ---------------------------------------------------------------

/// End-to-end wiring of the gossip lane: a real `NodeRunner` report tick
/// must attach the split-view sample to its frontier push and deliver it to
/// peers, where the receive path cross-checks it. Regressions in the sample
/// construction or the `push_frontiers_to_peers` hand-off (node_runner.rs)
/// are invisible to the manual-push tests above; this test drives the run
/// loop itself.
#[tokio::test]
async fn runner_report_tick_delivers_gossip_sample_to_peers() {
    use std::time::Duration;

    use asteroidb_poc::compaction::CompactionEngine;
    use asteroidb_poc::network::sync::SyncClient;
    use asteroidb_poc::network::{PeerConfig, PeerRegistry};
    use asteroidb_poc::runtime::{NodeRunner, NodeRunnerConfig};

    let s1 = make_signer("auth-1", 81); // the runner node
    let s3 = make_signer("auth-3", 83); // the equivocator

    // node-2: plain HTTP receiver.
    let (state2, addr2, h2) = spawn_node(
        "node-2",
        NodeOpts::with_registry(full_registry(&[&s1, &s3])),
    )
    .await;

    // auth-3 split view: node-2 is told digest-b directly...
    let client = FrontierSyncClient::with_token(TOKEN.to_string());
    let hlc = wall_ms();
    let f_b = make_frontier("auth-3", hlc, "digest-b");
    client
        .push_signed_frontiers(
            &addr2.to_string(),
            vec![f_b.clone()],
            vec![Some(sign(&s3, &f_b))],
        )
        .await
        .unwrap();
    assert!(!state2.equivocation.is_accused(&node_id("auth-3")));

    // ...while the runner node observed digest-a (as it would have after a
    // direct push from auth-3 to its own HTTP lane).
    let detector1 = Arc::new(EquivocationDetector::new(None));
    let f_a = make_frontier("auth-3", hlc, "digest-a");
    detector1.observe(&f_a, &sign(&s3, &f_a), wall_ms());

    // A real NodeRunner for auth-1 with node-2 as its only peer.
    let namespace = Arc::new(RwLock::new(default_namespace()));
    let certified = Arc::new(Mutex::new(CertifiedApi::new(
        node_id("auth-1"),
        Arc::clone(&namespace),
    )));
    let eventual = Arc::new(Mutex::new(EventualApi::new(node_id("auth-1"))));
    let peer_registry = PeerRegistry::new(
        node_id("auth-1"),
        vec![PeerConfig {
            node_id: node_id("node-2"),
            addr: addr2.to_string(),
        }],
    )
    .unwrap();
    let sync_client =
        SyncClient::with_token(Arc::new(Mutex::new(peer_registry)), TOKEN.to_string());
    let mut registry1 = KeysetRegistry::new();
    registry1
        .register_keyset(
            KeysetVersion(1),
            0,
            vec![
                (s1.node_id().clone(), s1.verifying_key()),
                (s3.node_id().clone(), s3.verifying_key()),
            ],
        )
        .unwrap();
    let config = NodeRunnerConfig {
        certification_interval: Duration::from_millis(50),
        frontier_report_interval: Duration::from_millis(25),
        sync_interval: None,
        ping_interval: None,
        node_signer: Some(Arc::new(s1)),
        keyset_registry: Some(Arc::new(std::sync::RwLock::new(registry1))),
        internal_token: Some(TOKEN.to_string()),
        equivocation: Some(Arc::clone(&detector1)),
        ..NodeRunnerConfig::default()
    };
    let mut runner = NodeRunner::with_sync(
        node_id("auth-1"),
        certified,
        CompactionEngine::with_defaults(),
        config,
        sync_client,
        eventual,
        Arc::new(RuntimeMetrics::default()),
    )
    .await;
    let shutdown = runner.shutdown_handle();
    let runner_handle = tokio::spawn(async move {
        runner.run().await;
    });

    // The runner's report tick must push its signed frontiers *with* the
    // gossip sample to node-2, whose receive path cross-checks digest-a
    // against its indexed digest-b and detects the split view.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !state2.equivocation.is_accused(&node_id("auth-3")) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "runner gossip sample never reached the peer's detector"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let report = get_json(&addr2, "/api/authority/equivocations").await;
    assert_eq!(report["accused_authorities"], serde_json::json!(["auth-3"]));
    assert_eq!(report["evidence_count"], 1);

    let _ = shutdown.send(true);
    let _ = runner_handle.await;
    h2.abort();
}

#[tokio::test]
async fn forged_or_unverifiable_relays_never_become_evidence() {
    let s1 = make_signer("auth-1", 31);
    let s2 = make_signer("auth-2", 32);
    let rogue = make_signer("auth-1", 77); // unregistered key claiming auth-1
    let outsider = make_signer("auth-9", 33); // not in the range's authority set

    // Registry holds s1, s2, and the outsider (auth-9) — the outsider's key
    // is valid, but auth-9 is not in the range's authority set.
    let mut registry = KeysetRegistry::new();
    registry
        .register_keyset(
            KeysetVersion(1),
            0,
            vec![
                (s1.node_id().clone(), s1.verifying_key()),
                (s2.node_id().clone(), s2.verifying_key()),
                (outsider.node_id().clone(), outsider.verifying_key()),
            ],
        )
        .unwrap();

    let (state, addr, handle) = spawn_node("node-1", NodeOpts::with_registry(registry)).await;
    let client = FrontierSyncClient::with_token(TOKEN.to_string());
    let hlc = wall_ms();

    // Seed a genuine observation so a forged conflict would be detectable.
    let f_a = make_frontier("auth-1", hlc, "digest-a");
    client
        .push_signed_frontiers(
            &addr.to_string(),
            vec![f_a.clone()],
            vec![Some(sign(&s1, &f_a))],
        )
        .await
        .unwrap();

    // (1) Forged relay: conflicting digest signed by an unregistered key.
    // A malicious relayer must not be able to frame auth-1.
    let f_b = make_frontier("auth-1", hlc, "digest-b");
    let forged = ObservedAttestation {
        frontier: f_b.clone(),
        signature: sign(&rogue, &f_b),
    };
    client
        .push_frontiers_with_observations(&addr.to_string(), vec![], vec![], vec![forged])
        .await
        .unwrap();
    assert!(!state.equivocation.is_accused(&node_id("auth-1")));

    // (2) Relay about an authority outside the range's authority set is
    // ignored by the membership gate (even with a valid signature).
    let f_x1 = make_frontier("auth-9", hlc, "digest-x1");
    let f_x2 = make_frontier("auth-9", hlc, "digest-x2");
    for f in [&f_x1, &f_x2] {
        let obs = ObservedAttestation {
            frontier: f.clone(),
            signature: sign(&outsider, f),
        };
        client
            .push_frontiers_with_observations(&addr.to_string(), vec![], vec![], vec![obs])
            .await
            .unwrap();
    }
    assert!(!state.equivocation.is_accused(&node_id("auth-9")));

    let metrics = get_json(&addr, "/api/metrics").await;
    assert_eq!(metrics["equivocation_detected_total"], 0);

    // (3) A node without a keyset registry ignores the observed lane
    // entirely: unverifiable pairs can never become evidence.
    let (state_nr, addr_nr, handle_nr) = spawn_node(
        "node-nr",
        NodeOpts {
            registry: None,
            exclude_accused: false,
            persist_path: None,
        },
    )
    .await;
    for (f, signer) in [(&f_a, &s1), (&f_b, &s1)] {
        let obs = ObservedAttestation {
            frontier: (*f).clone(),
            signature: sign(signer, f),
        };
        client
            .push_frontiers_with_observations(&addr_nr.to_string(), vec![], vec![], vec![obs])
            .await
            .unwrap();
    }
    assert!(!state_nr.equivocation.is_accused(&node_id("auth-1")));

    handle.abort();
    handle_nr.abort();
}

#[tokio::test]
async fn observed_lane_is_capped_and_skips_known_entries() {
    let s1 = make_signer("auth-1", 41);
    let (state, addr, handle) =
        spawn_node("node-1", NodeOpts::with_registry(full_registry(&[&s1]))).await;
    let client = FrontierSyncClient::with_token(TOKEN.to_string());
    let base = wall_ms();

    // Seed digest-a at the conflict HLC via the direct lane.
    let f_a = make_frontier("auth-1", base, "digest-a");
    client
        .push_signed_frontiers(
            &addr.to_string(),
            vec![f_a.clone()],
            vec![Some(sign(&s1, &f_a))],
        )
        .await
        .unwrap();

    // Known-exact echo: relaying digest-a again is deduped *before*
    // signature verification, so the split-view counter must not move.
    let echo = ObservedAttestation {
        frontier: f_a.clone(),
        signature: sign(&s1, &f_a),
    };
    client
        .push_frontiers_with_observations(&addr.to_string(), vec![], vec![], vec![echo])
        .await
        .unwrap();
    let metrics = get_json(&addr, "/api/metrics").await;
    assert_eq!(metrics["split_view_observations_total"], 0);

    // Per-request cap: MAX benign fillers followed by a conflicting pair —
    // the entry beyond the cap is ignored, so no evidence appears.
    let mut observed: Vec<ObservedAttestation> = (0..MAX_OBSERVED_PER_REQUEST as u64)
        .map(|i| {
            let f = make_frontier(
                "auth-1",
                base.saturating_sub(1_000 + i),
                &format!("fill-{i}"),
            );
            ObservedAttestation {
                signature: sign(&s1, &f),
                frontier: f,
            }
        })
        .collect();
    let f_b = make_frontier("auth-1", base, "digest-b");
    let conflict = ObservedAttestation {
        frontier: f_b.clone(),
        signature: sign(&s1, &f_b),
    };
    observed.push(conflict.clone());
    let resp = client
        .push_frontiers_with_observations(&addr.to_string(), vec![], vec![], observed)
        .await
        .expect("over-cap request must still succeed");
    assert_eq!(resp.accepted, 0);
    assert!(
        !state.equivocation.is_accused(&node_id("auth-1")),
        "entry beyond the per-request cap must be ignored"
    );
    let metrics = get_json(&addr, "/api/metrics").await;
    assert_eq!(
        metrics["split_view_observations_total"],
        MAX_OBSERVED_PER_REQUEST as u64
    );

    // The dropped entry was genuinely valid: sent alone, it is detected.
    client
        .push_frontiers_with_observations(&addr.to_string(), vec![], vec![], vec![conflict])
        .await
        .unwrap();
    assert!(state.equivocation.is_accused(&node_id("auth-1")));

    handle.abort();
}

// ---------------------------------------------------------------
// Exclusion flag (opt-in enforcement-lite) and default behaviour
// ---------------------------------------------------------------

async fn run_exclusion_scenario(exclude: bool) -> (CertificationStatus, bool) {
    let s1 = make_signer("auth-1", 51);
    let s2 = make_signer("auth-2", 52);
    let s3 = make_signer("auth-3", 53);
    let (state, addr, handle) = spawn_node(
        "node-1",
        NodeOpts {
            registry: Some(full_registry(&[&s1, &s2, &s3])),
            exclude_accused: exclude,
            persist_path: None,
        },
    )
    .await;
    let client = FrontierSyncClient::with_token(TOKEN.to_string());

    // A pending certified write.
    let write_ts = {
        let mut api = state.certified.lock().await;
        let mut counter = PnCounter::new();
        counter.increment(&node_id("writer"));
        api.certified_write(
            "user/carol".into(),
            CrdtValue::Counter(counter),
            OnTimeout::Pending,
        )
        .unwrap();
        api.pending_writes()[0].timestamp.physical
    };

    // auth-1 equivocates at an old checkpoint (before the write) — it is
    // now accused, but the earlier attestations cannot certify the write.
    let old_hlc = (write_ts / 1000) * 1000 - 5_000;
    let f_a = make_frontier("auth-1", old_hlc, "digest-a");
    let f_b = make_frontier("auth-1", old_hlc, "digest-b");
    for f in [&f_a, &f_b] {
        client
            .push_signed_frontiers(&addr.to_string(), vec![f.clone()], vec![Some(sign(&s1, f))])
            .await
            .unwrap();
    }

    // Both auth-1 and auth-2 report past the write's checkpoint. auth-1's
    // frontier must still advance (accepted) even when accused.
    let report_ts = (write_ts / 1000 + 1) * 1000 + 100;
    for signer in [&s1, &s2] {
        let f = make_frontier(&signer.node_id().0, report_ts, "digest-ok");
        let resp = client
            .push_signed_frontiers(
                &addr.to_string(),
                vec![f.clone()],
                vec![Some(sign(signer, &f))],
            )
            .await
            .unwrap();
        assert_eq!(
            resp.accepted, 1,
            "frontier advancement is never blocked by an accusation"
        );
    }

    let (status, has_certificate) = {
        let mut api = state.certified.lock().await;
        api.process_certifications();
        let read = api.get_certified("user/carol");
        let has_cert = read.proof.as_ref().is_some_and(|p| p.certificate.is_some());
        (read.status, has_cert)
    };

    assert!(state.equivocation.is_accused(&node_id("auth-1")));
    handle.abort();
    (status, has_certificate)
}

#[tokio::test]
async fn exclusion_flag_drops_accused_attestations_from_certificates() {
    // Default (detect-only): the accused authority's attestation still
    // contributes, so the 2-of-3 certificate assembles as before.
    let (status, has_certificate) = run_exclusion_scenario(false).await;
    assert_eq!(status, CertificationStatus::Certified);
    assert!(
        has_certificate,
        "default behaviour must be unchanged by detection"
    );

    // Opt-in exclusion: auth-1's attestation is dropped, leaving 1 of 3
    // attestations — below the unchanged majority threshold, so no
    // certificate. The write still certifies via frontier majority
    // (frontier advancement is not blocked).
    let (status, has_certificate) = run_exclusion_scenario(true).await;
    assert_eq!(status, CertificationStatus::Certified);
    assert!(
        !has_certificate,
        "excluded attestation must not contribute to certificate assembly"
    );
}

// ---------------------------------------------------------------
// Evidence persistence across restarts
// ---------------------------------------------------------------

#[tokio::test]
async fn evidence_survives_detector_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("equivocation_evidence.json");

    let s1 = make_signer("auth-1", 61);
    let (_state, addr, handle) = spawn_node(
        "node-1",
        NodeOpts {
            registry: Some(full_registry(&[&s1])),
            exclude_accused: false,
            persist_path: Some(path.clone()),
        },
    )
    .await;
    let client = FrontierSyncClient::with_token(TOKEN.to_string());

    let hlc = wall_ms();
    for digest in ["digest-a", "digest-b"] {
        let f = make_frontier("auth-1", hlc, digest);
        client
            .push_signed_frontiers(
                &addr.to_string(),
                vec![f.clone()],
                vec![Some(sign(&s1, &f))],
            )
            .await
            .unwrap();
    }

    // The handler persists on a background blocking task; wait for the file.
    let mut persisted = false;
    for _ in 0..100 {
        if path.exists() {
            persisted = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(persisted, "evidence file must be written after detection");
    handle.abort();

    // A restarted detector (fresh process) restores accusations + evidence.
    let restored = EquivocationDetector::new(Some(path));
    assert!(restored.is_accused(&node_id("auth-1")));
    let evidence = restored.evidence();
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].first.frontier.digest_hash, "digest-a");
    assert_eq!(evidence[0].second.frontier.digest_hash, "digest-b");

    // And the restored evidence is still third-party verifiable.
    let registry = full_registry(&[&s1]);
    for obs in [&evidence[0].first, &evidence[0].second] {
        verify_frontier_signature(
            &obs.frontier,
            &obs.signature,
            &registry,
            0,
            &EpochConfig::default(),
        )
        .expect("restored evidence must verify");
    }
}
