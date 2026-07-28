//! M-17: observer (non-voter) namespace pull sync + silent-fence
//! observability.
//!
//! Raft never replicates to non-voters, so before M-17 an observer's
//! namespace froze at its join-time snapshot: an observer authority kept
//! signing the OLD policy version after a bump, its reports were fenced
//! silently, and the certification quorum shrank without any counter or
//! log moving. These tests cover:
//!
//! - T-0: the reproduction of that failure (pull disabled = the old
//!   world), proving the real harm — the observer authority disappears
//!   from `contributing_authorities` and certified writes Time out once
//!   one more voter authority stops — and that the drop is now VISIBLE on
//!   the voter (`attestation_stale_version_total` /
//!   `attestation_rejected_fenced_total`, both of which stayed 0 before
//!   this change);
//! - T-1: the fix end to end — the observer pulls committed control-plane
//!   state over real HTTP, follows a multi-bump (crossing the M-4
//!   admission window), and returns to `contributing_authorities`;
//! - T-2: the adoption guard (lexicographic `(version_counter, index)`
//!   monotonicity, voter-set membership, voters never adopt) and
//!   `committed_snapshot` correctness;
//! - T-3: pull persistence — a restart restores the adopted state exactly
//!   like the InstallSnapshot path would;
//! - T-4: partition behaviour — pull failures are counted while the
//!   voters are unreachable, recovery is automatic, and the observer's
//!   own NodeRunner fences the old version after catch-up;
//! - the single-voter regression (no pull loop on a voter).

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::authority::ack_frontier::AckFrontier;
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::raft::node::{AdoptOutcome, RaftConfig, RaftNode};
use asteroidb_poc::control_plane::raft::spawn_raft_driver;
use asteroidb_poc::control_plane::raft::storage::{FileRaftStorage, MemRaftStorage, RaftStorage};
use asteroidb_poc::control_plane::raft::transport::{
    NoopTransport, RaftTransport, TransportError, TransportFuture,
};
use asteroidb_poc::control_plane::raft::types::{
    AppendEntriesRequest, AuthoritySpec, ControlPlaneCommand, ControlPlaneState, LogEntry,
    NamespaceSnapshotRequest, NamespaceSnapshotResponse, PolicySpec, VersionedPolicy,
};
use asteroidb_poc::control_plane::system_namespace::SystemNamespace;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::network::frontier_sync::FrontierPushRequest;
use asteroidb_poc::network::raft_transport::HttpRaftTransport;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::runtime::{NodeRunner, NodeRunnerConfig};
use asteroidb_poc::types::{KeyRange, NodeId, PolicyVersion};

use tokio::sync::{Mutex, watch};

fn node_id(s: &str) -> NodeId {
    NodeId(s.into())
}

fn kr(prefix: &str) -> KeyRange {
    KeyRange {
        prefix: prefix.into(),
    }
}

fn wrap_ns(ns: SystemNamespace) -> Arc<RwLock<SystemNamespace>> {
    Arc::new(RwLock::new(ns))
}

fn fast_config() -> RaftConfig {
    RaftConfig {
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(400),
        heartbeat_interval: Duration::from_millis(50),
        propose_timeout: Duration::from_millis(3_000),
        log_max: 4096,
        ..RaftConfig::default()
    }
}

fn policy_spec(prefix: &str, replica_count: usize) -> PolicySpec {
    PolicySpec {
        prefix: prefix.into(),
        replica_count,
        required_tags: BTreeSet::new(),
        forbidden_tags: BTreeSet::new(),
        allow_local_write_on_partition: false,
        certified: true,
        max_read_latency_ms: None,
        preferred_cost_tier: None,
    }
}

/// AppState wired for HTTP tests; returns the certified handle so tests
/// can drive certification ticks deterministically.
fn app_state(
    id: &NodeId,
    namespace: Arc<RwLock<SystemNamespace>>,
    consensus: ControlPlaneConsensus,
) -> (Arc<AppState>, Arc<Mutex<CertifiedApi>>) {
    let certified = Arc::new(Mutex::new(CertifiedApi::new(
        id.clone(),
        Arc::clone(&namespace),
    )));
    let state = Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(id.clone()))),
        certified: Arc::clone(&certified),
        namespace,
        metrics: Arc::new(RuntimeMetrics::default()),
        peers: None,
        peer_persist_path: None,
        namespace_persist_path: None,
        consensus: Arc::new(Mutex::new(consensus)),
        internal_token: None,
        self_node_id: Some(id.clone()),
        self_addr: None,
        latency_model: None,
        cluster_nodes: None,
        slo_tracker: Arc::new(asteroidb_poc::ops::slo::SloTracker::new()),
        keyset_registry: None,
        epoch_config: asteroidb_poc::authority::certificate::EpochConfig::default(),
        current_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        require_signed_frontiers: false,
        equivocation: Arc::new(
            asteroidb_poc::authority::equivocation::EquivocationDetector::new(None),
        ),
        exclude_accused_authorities: false,
        eventual_wal: None,
        certified_wal: None,
    });
    (state, certified)
}

/// Propose a policy until committed (absorbing leadership races); returns
/// the applied policy.
async fn propose_policy(
    nodes: &[Arc<RaftNode>],
    spec: PolicySpec,
) -> asteroidb_poc::placement::PlacementPolicy {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let leader = loop {
            if let Some(n) = nodes.iter().find(|n| n.is_leader()) {
                break Arc::clone(n);
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no raft leader elected within 15s"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let consensus = ControlPlaneConsensus::with_raft(leader);
        match consensus.propose_policy_update(spec.clone()).await {
            Ok(policy) => break policy,
            Err(asteroidb_poc::error::CrdtError::NotLeader { .. }) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "proposal kept losing leadership within 15s"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(other) => panic!("policy proposal failed: {other:?}"),
        }
    }
}

/// Propose an authority definition until committed.
async fn propose_authority(nodes: &[Arc<RaftNode>], spec: AuthoritySpec) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let leader = loop {
            if let Some(n) = nodes.iter().find(|n| n.is_leader()) {
                break Arc::clone(n);
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no raft leader elected within 15s"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let consensus = ControlPlaneConsensus::with_raft(leader);
        match consensus.propose_authority_update(spec.clone()).await {
            Ok(_) => break,
            Err(asteroidb_poc::error::CrdtError::NotLeader { .. }) => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(other) => panic!("authority proposal failed: {other:?}"),
        }
    }
}

/// Push one unsigned frontier report for `authority` at `policy_version`
/// to the node at `addr` (exercises the real voter receive path:
/// admission, stale/fenced instrumentation, metrics export).
async fn push_frontier(
    client: &reqwest::Client,
    addr: &str,
    authority: &str,
    prefix: &str,
    policy_version: u64,
    physical: u64,
) {
    let req = FrontierPushRequest {
        frontiers: vec![AckFrontier {
            authority_id: node_id(authority),
            frontier_hlc: HlcTimestamp {
                physical,
                logical: 0,
                node_id: authority.into(),
            },
            key_range: kr(prefix),
            policy_version: PolicyVersion(policy_version),
            digest_hash: format!("digest-{authority}-{physical}"),
        }],
        signatures: Vec::new(),
        observed: Vec::new(),
    };
    let resp = client
        .post(format!("http://{addr}/api/internal/frontiers"))
        .json(&req)
        .send()
        .await
        .expect("frontier push must reach the node");
    assert!(
        resp.status().is_success(),
        "frontier push rejected: {}",
        resp.status()
    );
}

async fn metrics_json(client: &reqwest::Client, addr: &str) -> serde_json::Value {
    client
        .get(format!("http://{addr}/api/metrics"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Bind listeners for the given ids and return (listeners, peer map).
async fn bind_all(ids: &[NodeId]) -> (Vec<tokio::net::TcpListener>, HashMap<String, String>) {
    let mut listeners = Vec::new();
    let mut peer_map = HashMap::new();
    for id in ids {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        peer_map.insert(id.0.clone(), listener.local_addr().unwrap().to_string());
        listeners.push(listener);
    }
    (listeners, peer_map)
}

// ===========================================================================
// T-0: silent fence reproduction (pull disabled = pre-M-17 world)
// ===========================================================================

/// Reproduce the pre-M-17 failure with the pull disabled
/// (`observer_pull_interval = 0`), and prove both the harm and the new
/// visibility:
/// 1. authority set {cp-1, cp-2, obs-1} (majority 2), observer namespace
///    seeded at join time; a certified write succeeds WITH obs-1
///    contributing;
/// 2. after a policy bump the observer namespace stays frozen and its
///    reports carry the old version: the voter now counts them
///    (`attestation_stale_version_total`, `attestation_rejected_fenced_total`
///    — both of which were structurally 0 before M-17, i.e. the drop was
///    silent);
/// 3. with cp-2 also unavailable, a certified write times out — the
///    quorum quietly shrank from 3 to 1 live contributors;
/// 4. once cp-2 recovers, writes certify again but obs-1 is GONE from
///    `contributing_authorities`.
#[tokio::test(flavor = "multi_thread")]
async fn t0_silent_fence_reproduction_without_pull() {
    let voter_ids: Vec<NodeId> = (1..=3).map(|i| node_id(&format!("cp-{i}"))).collect();
    let voters: BTreeSet<NodeId> = voter_ids.iter().cloned().collect();
    let obs_id = node_id("obs-1");

    let (listeners, peer_map) = bind_all(&voter_ids).await;
    let (shutdown_tx, _) = watch::channel(false);

    let mut raft_nodes = Vec::new();
    let mut namespaces = Vec::new();
    let mut certified_apis = Vec::new();
    let mut addrs = Vec::new();

    for (id, listener) in voter_ids.iter().zip(listeners) {
        let namespace = wrap_ns(SystemNamespace::new());
        let transport = Arc::new(HttpRaftTransport::new(peer_map.clone(), None, None));
        let raft = RaftNode::new(
            id.clone(),
            voters.clone(),
            fast_config(),
            Arc::new(MemRaftStorage::new()),
            transport,
            Arc::clone(&namespace),
            None,
        )
        .unwrap();
        spawn_raft_driver(Arc::clone(&raft), shutdown_tx.subscribe());
        let (state, certified) = app_state(
            id,
            Arc::clone(&namespace),
            ControlPlaneConsensus::with_raft(Arc::clone(&raft)),
        );
        addrs.push(peer_map[&id.0].clone());
        namespaces.push(namespace);
        certified_apis.push(certified);
        raft_nodes.push(raft);
        let app = router(state);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
    }

    // Observer: NOT in the voter set, pull DISABLED (the pre-M-17 world).
    let obs_ns = wrap_ns(SystemNamespace::new());
    let obs_raft = RaftNode::new(
        obs_id.clone(),
        voters.clone(),
        RaftConfig {
            observer_pull_interval: Duration::ZERO,
            ..fast_config()
        },
        Arc::new(MemRaftStorage::new()),
        Arc::new(HttpRaftTransport::new(peer_map.clone(), None, None)),
        Arc::clone(&obs_ns),
        None,
    )
    .unwrap();
    spawn_raft_driver(Arc::clone(&obs_raft), shutdown_tx.subscribe());

    // Commit the policy and the {cp-1, cp-2, obs-1} authority definition.
    let policy = propose_policy(&raft_nodes, policy_spec("user/", 1)).await;
    let v1 = policy.version.0;
    propose_authority(
        &raft_nodes,
        AuthoritySpec {
            prefix: "user/".into(),
            authority_nodes: vec![voter_ids[0].clone(), voter_ids[1].clone(), obs_id.clone()],
        },
    )
    .await;

    // Wait until cp-1 (our write/report target) has both applied.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        {
            let ns = namespaces[0].read().unwrap();
            if ns.get_placement_policy("user/").is_some()
                && ns.get_authority_definition("user/").is_some()
            {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "policy/authority did not apply on cp-1 within 15s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Join-time snapshot: seed the observer namespace ONCE, then freeze
    // (no pull). This is exactly the pre-M-17 lifecycle.
    {
        let src = namespaces[0].read().unwrap();
        let mut dst = obs_ns.write().unwrap();
        dst.set_placement_policy(src.get_placement_policy("user/").unwrap().clone())
            .unwrap();
        dst.set_authority_definition(src.get_authority_definition("user/").unwrap().clone());
    }

    let client = reqwest::Client::new();
    let cp1 = &addrs[0];

    // Certified write on cp-1, reported by all three authorities at v1:
    // obs-1 CONTRIBUTES while its namespace is fresh.
    let resp = client
        .post(format!("http://{cp1}/api/certified/write"))
        .json(&serde_json::json!({
            "key": "user/x",
            "value": {"type": "register", "value": "a"}
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "certified write must be accepted"
    );

    let report_ts = wall_ms() + 60_000;
    for authority in ["cp-1", "cp-2", "obs-1"] {
        // The observer's report derives its policy version from the
        // observer's OWN namespace — that binding is the failure vector.
        let pv = obs_ns
            .read()
            .unwrap()
            .get_placement_policy("user/")
            .unwrap()
            .version
            .0;
        assert_eq!(pv, v1);
        push_frontier(&client, cp1, authority, "user/", pv, report_ts).await;
    }
    {
        let mut api = certified_apis[0].lock().await;
        api.process_certifications();
        let read = api.get_certified("user/x");
        assert_eq!(
            read.status,
            asteroidb_poc::types::CertificationStatus::Certified,
            "3/3 fresh authorities must certify"
        );
        let proof = read.proof.expect("certified read carries a proof");
        assert!(
            proof.contributing_authorities.contains(&obs_id),
            "the observer authority must contribute while its namespace is fresh"
        );
    }

    // The silent-fence counters are ZERO before the bump (and, pre-M-17,
    // they did not exist at all — the PR description's control).
    let m = metrics_json(&client, cp1).await;
    assert_eq!(m["attestation_stale_version_total"], 0);
    assert_eq!(m["attestation_rejected_fenced_total"], 0);

    // Policy bump: voters move to v2, the observer namespace stays FROZEN
    // at v1 (pull disabled). v2 is taken from the actually-applied policy
    // rather than computed as v1+1: a NotLeader-after-replication retry
    // inside propose_policy can legally advance the version by more than
    // one (the first proposal still commits under the new leader).
    let bumped = propose_policy(&raft_nodes, policy_spec("user/", 1)).await;
    let v2 = bumped.version.0;
    assert!(v2 > v1, "the bump must supersede v1 (got v1={v1}, v2={v2})");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if namespaces[0]
            .read()
            .unwrap()
            .get_placement_policy("user/")
            .unwrap()
            .version
            .0
            >= v2
        {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        obs_ns
            .read()
            .unwrap()
            .get_placement_policy("user/")
            .unwrap()
            .version
            .0,
        v1,
        "without the pull the observer namespace must stay frozen (repro)"
    );

    // What NodeRunner::detect_version_changes does on every voter at the
    // next cert tick: fence the old version.
    {
        let mut api = certified_apis[0].lock().await;
        api.fence_version(&kr("user/"), PolicyVersion(v1));
    }

    // The frozen observer keeps reporting v1 (derived from its own
    // namespace): stale-but-admissible AND fenced — both now counted.
    let obs_pv = obs_ns
        .read()
        .unwrap()
        .get_placement_policy("user/")
        .unwrap()
        .version
        .0;
    assert_eq!(obs_pv, v1);
    push_frontier(&client, cp1, "obs-1", "user/", obs_pv, report_ts + 1_000).await;
    let m = metrics_json(&client, cp1).await;
    assert!(
        m["attestation_stale_version_total"].as_u64().unwrap() >= 1,
        "stale-version reports must be counted from the FIRST bump: {m}"
    );
    assert!(
        m["attestation_rejected_fenced_total"].as_u64().unwrap() >= 1,
        "fenced drops must be counted (pre-M-17 this path was silent): {m}"
    );

    // Real harm, phase 1: cp-2 is down too. Only cp-1 reports v2; the
    // observer's stale report contributes nothing. Majority (2 of 3) is
    // unreachable -> the certified write TIMES OUT.
    let resp = client
        .post(format!("http://{cp1}/api/certified/write"))
        .json(&serde_json::json!({
            "key": "user/z",
            "value": {"type": "register", "value": "c"}
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let report_ts2 = wall_ms() + 60_000;
    push_frontier(&client, cp1, "cp-1", "user/", v2, report_ts2).await;
    push_frontier(&client, cp1, "obs-1", "user/", obs_pv, report_ts2).await;
    {
        let mut api = certified_apis[0].lock().await;
        api.process_certifications_with_timeout(wall_ms() + 120_000);
        let read = api.get_certified("user/z");
        assert_eq!(
            read.status,
            asteroidb_poc::types::CertificationStatus::Timeout,
            "with the observer silently fenced and one voter down, the \
             certification quorum is quietly gone (the M-17 harm)"
        );
    }

    // Real harm, phase 2: cp-2 recovers -> writes certify again, but the
    // observer authority is GONE from contributing_authorities.
    push_frontier(&client, cp1, "cp-2", "user/", v2, report_ts2).await;
    let resp = client
        .post(format!("http://{cp1}/api/certified/write"))
        .json(&serde_json::json!({
            "key": "user/y",
            "value": {"type": "register", "value": "b"}
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    push_frontier(&client, cp1, "cp-1", "user/", v2, wall_ms() + 120_000).await;
    push_frontier(&client, cp1, "cp-2", "user/", v2, wall_ms() + 120_000).await;
    {
        let mut api = certified_apis[0].lock().await;
        api.process_certifications();
        let read = api.get_certified("user/y");
        assert_eq!(
            read.status,
            asteroidb_poc::types::CertificationStatus::Certified
        );
        let proof = read.proof.expect("certified read carries a proof");
        assert!(
            proof.contributing_authorities.contains(&voter_ids[0])
                && proof.contributing_authorities.contains(&voter_ids[1]),
            "the two live voters contribute"
        );
        assert!(
            !proof.contributing_authorities.contains(&obs_id),
            "the frozen observer must be absent from contributing_authorities \
             (the quorum shrank from 3 to 2 without any error surfacing \
             anywhere before M-17)"
        );
    }

    let _ = shutdown_tx.send(true);
}

// ===========================================================================
// T-1 / T-6: pull sync end-to-end + propose-time warning
// ===========================================================================

/// With the pull enabled the observer follows a multi-bump (crossing the
/// M-4 admission window), re-enters `contributing_authorities`, and the
/// voter-side counters show the documented stage A (stale) -> stage B
/// (window-rejected) progression for reports stuck at old versions.
/// Also covers T-6: `PUT /api/control-plane/authorities` naming a
/// non-voter returns 200 with a `warnings` entry (and none for
/// voter-only definitions).
#[tokio::test(flavor = "multi_thread")]
async fn t1_observer_pull_follows_bumps_and_restores_contribution() {
    let voter_ids: Vec<NodeId> = (1..=3).map(|i| node_id(&format!("cp-{i}"))).collect();
    let voters: BTreeSet<NodeId> = voter_ids.iter().cloned().collect();
    let obs_id = node_id("obs-1");

    let (listeners, peer_map) = bind_all(&voter_ids).await;
    let (shutdown_tx, _) = watch::channel(false);

    let mut raft_nodes = Vec::new();
    let mut namespaces = Vec::new();
    let mut certified_apis = Vec::new();
    let mut addrs = Vec::new();

    for (id, listener) in voter_ids.iter().zip(listeners) {
        let namespace = wrap_ns(SystemNamespace::new());
        let transport = Arc::new(HttpRaftTransport::new(peer_map.clone(), None, None));
        let raft = RaftNode::new(
            id.clone(),
            voters.clone(),
            fast_config(),
            Arc::new(MemRaftStorage::new()),
            transport,
            Arc::clone(&namespace),
            None,
        )
        .unwrap();
        spawn_raft_driver(Arc::clone(&raft), shutdown_tx.subscribe());
        let (state, certified) = app_state(
            id,
            Arc::clone(&namespace),
            ControlPlaneConsensus::with_raft(Arc::clone(&raft)),
        );
        addrs.push(peer_map[&id.0].clone());
        namespaces.push(namespace);
        certified_apis.push(certified);
        raft_nodes.push(raft);
        let app = router(state);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
    }

    // Observer with an ACTIVE pull loop.
    let obs_ns = wrap_ns(SystemNamespace::new());
    let obs_raft = RaftNode::new(
        obs_id.clone(),
        voters.clone(),
        RaftConfig {
            observer_pull_interval: Duration::from_millis(150),
            ..fast_config()
        },
        Arc::new(MemRaftStorage::new()),
        Arc::new(HttpRaftTransport::new(peer_map.clone(), None, None)),
        Arc::clone(&obs_ns),
        None,
    )
    .unwrap();
    spawn_raft_driver(Arc::clone(&obs_raft), shutdown_tx.subscribe());

    // Commit v1 + the mixed authority definition — via HTTP so the T-6
    // warnings surface. Retry across nodes/leader changes.
    let v1 = propose_policy(&raft_nodes, policy_spec("user/", 1))
        .await
        .version
        .0;

    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let body = loop {
        let mut done = None;
        for addr in &addrs {
            let resp = client
                .put(format!("http://{addr}/api/control-plane/authorities"))
                .json(&serde_json::json!({
                    "key_range_prefix": "user/",
                    "authority_nodes": ["cp-1", "cp-2", "obs-1"]
                }))
                .send()
                .await
                .unwrap();
            if resp.status() == reqwest::StatusCode::OK {
                done = Some(resp.json::<serde_json::Value>().await.unwrap());
                break;
            }
        }
        if let Some(b) = done {
            break b;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no leader accepted the authority PUT within 15s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    // T-6: the non-voter authority is accepted (200) but flagged.
    let warnings = body["warnings"]
        .as_array()
        .expect("authority PUT naming a non-voter must carry warnings");
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().unwrap().contains("obs-1")),
        "the warning must name the non-voter authority: {body}"
    );

    // Voter-only definition: no warnings field at all (backward compat).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut done = None;
        for addr in &addrs {
            let resp = client
                .put(format!("http://{addr}/api/control-plane/authorities"))
                .json(&serde_json::json!({
                    "key_range_prefix": "vonly/",
                    "authority_nodes": ["cp-1", "cp-2", "cp-3"]
                }))
                .send()
                .await
                .unwrap();
            if resp.status() == reqwest::StatusCode::OK {
                done = Some(resp.json::<serde_json::Value>().await.unwrap());
                break;
            }
        }
        if let Some(b) = done {
            assert!(
                b.get("warnings").is_none(),
                "voter-only definitions must not carry warnings: {b}"
            );
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The observer catches up from SCRATCH via the pull (no join seed
    // needed at all — the pull is a full committed-state transfer).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if obs_ns
            .read()
            .unwrap()
            .get_placement_policy("user/")
            .is_some_and(|p| p.version.0 >= v1)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "observer did not pull the committed namespace within 15s"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let status = obs_raft.status();
    assert!(
        status.observer_ns_pull_success_total >= 1,
        "successful pulls must be counted"
    );
    assert!(
        status.observer_ns_last_pull_unix_ms > 0,
        "the last-pull timestamp must be recorded"
    );

    // Multi-bump: at least three bumps so v1 falls OUT of the cur-2..=cur+1
    // admission window at the end; the observer must follow all the way.
    // v4 is taken from the LAST applied policy instead of the arithmetic
    // v1+3: a NotLeader-after-replication retry inside propose_policy can
    // legally commit an extra bump (which only pushes v1 further out of
    // the window — the stage-B assertion below stays valid).
    let mut v4 = v1;
    for _ in 0..3 {
        v4 = propose_policy(&raft_nodes, policy_spec("user/", 1))
            .await
            .version
            .0;
    }
    assert!(
        v4 >= v1 + 3,
        "three committed bumps from v1={v1} (got v4={v4})"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if obs_ns
            .read()
            .unwrap()
            .get_placement_policy("user/")
            .is_some_and(|p| p.version.0 >= v4)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "observer did not follow the multi-bump within 15s"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    // The observer's replicated version counter converged on a voter's.
    let leader_counter = raft_nodes
        .iter()
        .find(|n| n.is_leader())
        .map(|n| n.status().observer_ns_version_counter);
    if let Some(lc) = leader_counter {
        assert_eq!(obs_raft.status().observer_ns_version_counter, lc);
    }

    // Wait for cp-1 to be at v4 too, then verify the stage A -> stage B
    // counter progression for reports stuck at old versions (what a
    // frozen reporter would send): v4-1 is stale-but-admissible, v1 is
    // out of the admission window (M-4 regression check).
    let cp1 = &addrs[0];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if namespaces[0]
            .read()
            .unwrap()
            .get_placement_policy("user/")
            .is_some_and(|p| p.version.0 >= v4)
        {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let ts = wall_ms() + 60_000;
    push_frontier(&client, cp1, "obs-1", "user/", v4 - 1, ts).await; // stage A
    push_frontier(&client, cp1, "obs-1", "user/", v1, ts).await; // stage B
    let m = metrics_json(&client, cp1).await;
    assert!(
        m["attestation_stale_version_total"].as_u64().unwrap() >= 1,
        "stage A (stale-but-admissible) must be counted: {m}"
    );
    assert!(
        m["attestation_rejected_version_window_total"]
            .as_u64()
            .unwrap()
            >= 1,
        "stage B (window reject) must be counted: {m}"
    );

    // Contribution restored: a certified write at v4 carries obs-1 in
    // contributing_authorities again (its reports now derive the CURRENT
    // version from its pulled namespace).
    let resp = client
        .post(format!("http://{cp1}/api/certified/write"))
        .json(&serde_json::json!({
            "key": "user/a",
            "value": {"type": "register", "value": "restored"}
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let obs_pv = obs_ns
        .read()
        .unwrap()
        .get_placement_policy("user/")
        .unwrap()
        .version
        .0;
    assert_eq!(
        obs_pv, v4,
        "the observer report version comes from its own, now fresh, namespace"
    );
    let ts2 = wall_ms() + 60_000;
    push_frontier(&client, cp1, "obs-1", "user/", obs_pv, ts2).await;
    push_frontier(&client, cp1, "cp-2", "user/", v4, ts2).await;
    {
        let mut api = certified_apis[0].lock().await;
        api.process_certifications();
        let read = api.get_certified("user/a");
        assert_eq!(
            read.status,
            asteroidb_poc::types::CertificationStatus::Certified
        );
        let proof = read.proof.expect("certified read carries a proof");
        assert!(
            proof.contributing_authorities.contains(&obs_id),
            "after the pull catches up, the observer authority contributes again"
        );
    }

    let _ = shutdown_tx.send(true);
}

// ===========================================================================
// T-2: adoption guard + committed_snapshot
// ===========================================================================

fn cp_state(counter: u64, prefix: &str, authority_prefixes: &[&str]) -> ControlPlaneState {
    let mut state = ControlPlaneState {
        bootstrapped: true,
        version_counter: counter,
        ..Default::default()
    };
    state.policies.insert(
        prefix.to_string(),
        VersionedPolicy {
            version: counter,
            spec: policy_spec(prefix, 1),
        },
    );
    for ap in authority_prefixes {
        state.authorities.insert(
            (*ap).to_string(),
            AuthoritySpec {
                prefix: (*ap).to_string(),
                authority_nodes: vec![node_id("cp-1")],
            },
        );
    }
    state
}

fn snapshot_resp(
    from: &str,
    counter: u64,
    index: u64,
    state: ControlPlaneState,
) -> NamespaceSnapshotResponse {
    let mut state = state;
    state.version_counter = counter;
    NamespaceSnapshotResponse {
        node_id: node_id(from),
        term: 1,
        last_applied_index: index,
        last_applied_term: 1,
        state,
    }
}

/// A fresh observer node (self NOT in `voters`).
fn observer_node(ns: Arc<RwLock<SystemNamespace>>) -> Arc<RaftNode> {
    let voters: BTreeSet<NodeId> = [node_id("cp-1"), node_id("cp-2")].into_iter().collect();
    RaftNode::new(
        node_id("obs-1"),
        voters,
        fast_config(),
        Arc::new(MemRaftStorage::new()),
        Arc::new(NoopTransport),
        ns,
        None,
    )
    .unwrap()
}

#[tokio::test]
async fn t2_adopt_guard_lexicographic_monotonicity() {
    let ns = wrap_ns(SystemNamespace::new());
    let node = observer_node(Arc::clone(&ns));

    // (a) counter increase: adopted, namespace projection updated.
    let outcome = node
        .adopt_pulled_snapshot(snapshot_resp(
            "cp-1",
            5,
            3,
            cp_state(5, "user/", &["user/"]),
        ))
        .unwrap();
    assert_eq!(
        outcome,
        AdoptOutcome::Adopted,
        "a strictly newer (counter, index) must be adopted"
    );
    assert!(ns.read().unwrap().get_placement_policy("user/").is_some());
    assert_eq!(node.status().last_applied, 3);
    assert_eq!(node.status().observer_ns_version_counter, 5);

    // (b) counter equal, index increase (authority-only change): adopted.
    let outcome = node
        .adopt_pulled_snapshot(snapshot_resp(
            "cp-1",
            5,
            4,
            cp_state(5, "user/", &["user/", "extra/"]),
        ))
        .unwrap();
    assert_eq!(
        outcome,
        AdoptOutcome::Adopted,
        "authority-only updates advance the index at an equal counter and must be adopted"
    );
    assert!(
        ns.read()
            .unwrap()
            .get_authority_definition("extra/")
            .is_some()
    );

    // (c) counter equal, index equal-or-lower: rejected (healthy no-op).
    for idx in [4, 3] {
        let outcome = node
            .adopt_pulled_snapshot(snapshot_resp("cp-1", 5, idx, cp_state(5, "stale/", &[])))
            .unwrap();
        assert_eq!(
            outcome,
            AdoptOutcome::NotNewer,
            "counter-equal index<=local must be rejected (idx={idx})"
        );
    }
    assert!(ns.read().unwrap().get_placement_policy("stale/").is_none());

    // (d) zombie: counter LOWER, index higher — the case an OR-combined
    // guard would wrongly adopt (rolling the policy versions back).
    let outcome = node
        .adopt_pulled_snapshot(snapshot_resp("cp-1", 4, 99, cp_state(4, "zombie/", &[])))
        .unwrap();
    assert_eq!(
        outcome,
        AdoptOutcome::NotNewer,
        "a high-index / low-counter zombie voter must never roll the observer back"
    );
    assert!(ns.read().unwrap().get_placement_policy("zombie/").is_none());
    assert_eq!(node.status().observer_ns_version_counter, 5);

    // (e) responder outside the local voter set: rejected even if newer —
    // and DISTINGUISHABLE from the healthy no-op (the pull loop counts
    // this as a failure).
    let outcome = node
        .adopt_pulled_snapshot(snapshot_resp("rogue", 50, 50, cp_state(50, "rogue/", &[])))
        .unwrap();
    assert_eq!(
        outcome,
        AdoptOutcome::RejectedResponder,
        "snapshots from outside the voter set must be rejected"
    );
    assert!(ns.read().unwrap().get_placement_policy("rogue/").is_none());
}

#[tokio::test]
async fn t2_voters_never_adopt_pulled_snapshots() {
    // Single-voter node: is a voter (and instantly the leader).
    let ns = wrap_ns(SystemNamespace::new());
    let voters: BTreeSet<NodeId> = [node_id("cp-1")].into_iter().collect();
    let node = RaftNode::new(
        node_id("cp-1"),
        voters,
        fast_config(),
        Arc::new(MemRaftStorage::new()),
        Arc::new(NoopTransport),
        Arc::clone(&ns),
        None,
    )
    .unwrap();

    let outcome = node
        .adopt_pulled_snapshot(snapshot_resp("cp-1", 50, 50, cp_state(50, "user/", &[])))
        .unwrap();
    assert_eq!(
        outcome,
        AdoptOutcome::VoterRefusal,
        "voters only ingest state via raft replication"
    );
    assert!(ns.read().unwrap().get_placement_policy("user/").is_none());
}

#[tokio::test]
async fn t2_committed_snapshot_serves_applied_state() {
    // Single-voter leader: proposals commit + apply synchronously.
    let ns = wrap_ns(SystemNamespace::new());
    let voters: BTreeSet<NodeId> = [node_id("cp-1")].into_iter().collect();
    let node = RaftNode::new(
        node_id("cp-1"),
        voters,
        fast_config(),
        Arc::new(MemRaftStorage::new()),
        Arc::new(NoopTransport),
        Arc::clone(&ns),
        None,
    )
    .unwrap();
    let consensus = ControlPlaneConsensus::with_raft(Arc::clone(&node));
    let policy = consensus
        .propose_policy_update(policy_spec("user/", 1))
        .await
        .unwrap();

    let snap = node.committed_snapshot();
    let status = node.status();
    assert_eq!(snap.node_id, node_id("cp-1"));
    assert_eq!(
        snap.last_applied_index, status.last_applied,
        "the snapshot must cover exactly the applied prefix"
    );
    assert_eq!(snap.term, status.term);
    assert_eq!(
        snap.last_applied_term, status.term,
        "the last applied entry was written in the current term"
    );
    let vp = snap
        .state
        .policies
        .get("user/")
        .expect("the applied policy must be in the served state");
    assert_eq!(vp.version, policy.version.0);
    assert_eq!(snap.state.version_counter, policy.version.0);
}

/// T-2(g) proper: `committed_snapshot` serves ONLY the applied (committed)
/// prefix — never uncommitted log entries. A follower holding replicated
/// but uncommitted entries (leader_commit below the tail) must not leak
/// them to a pulling observer: a later leader change may overwrite that
/// tail, and the observer's monotonicity guard could not catch the
/// rollback (the counter would appear to have advanced legitimately).
#[tokio::test]
async fn t2_committed_snapshot_excludes_uncommitted_log_tail() {
    let ns = wrap_ns(SystemNamespace::new());
    let voters: BTreeSet<NodeId> = [node_id("cp-1"), node_id("cp-2")].into_iter().collect();
    // cp-2 is a FOLLOWER: entries arrive via AppendEntries with a
    // leader_commit that trails the appended tail.
    let node = RaftNode::new(
        node_id("cp-2"),
        voters,
        fast_config(),
        Arc::new(MemRaftStorage::new()),
        Arc::new(NoopTransport),
        Arc::clone(&ns),
        None,
    )
    .unwrap();

    let entry = |index: u64, term: u64, prefix: &str| LogEntry {
        index,
        term,
        command: ControlPlaneCommand::PutPolicy(policy_spec(prefix, 1)),
    };
    // Entry 1 was written in term 1; the term-2 leader replicates it plus
    // two entries of its own, committing only through index 1.
    let resp = node
        .handle_append_entries(AppendEntriesRequest {
            term: 2,
            leader_id: node_id("cp-1"),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![entry(1, 1, "a/"), entry(2, 2, "b/"), entry(3, 2, "c/")],
            leader_commit: 1,
        })
        .unwrap();
    assert!(resp.success, "the append must be accepted");
    assert_eq!(node.status().last_log_index, 3);
    assert_eq!(node.status().last_applied, 1);

    let snap = node.committed_snapshot();
    assert_eq!(
        snap.last_applied_index, 1,
        "the snapshot boundary must be the APPLIED prefix, not the log tail"
    );
    assert_eq!(
        snap.last_applied_term, 1,
        "the boundary term is the applied entry's term (1), not the tail's (2)"
    );
    assert!(
        snap.state.policies.contains_key("a/"),
        "the committed entry's effect is served"
    );
    assert!(
        !snap.state.policies.contains_key("b/") && !snap.state.policies.contains_key("c/"),
        "uncommitted entries must never leak into a served snapshot"
    );
    assert_eq!(snap.state.version_counter, 1);
}

// ===========================================================================
// T-3: pull persistence across a restart
// ===========================================================================

/// An adopted pull persists exactly like the InstallSnapshot path: after
/// a restart the namespace, the snapshot meta, and the replicated state
/// are all restored (a later voter promotion would start from the same
/// durable state as a snapshot-caught-up follower).
#[tokio::test]
async fn t3_adopted_snapshot_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let raft_dir = dir.path().join("raft");
    let ns_path = dir.path().join("system_namespace.json");
    let voters: BTreeSet<NodeId> = [node_id("cp-1"), node_id("cp-2")].into_iter().collect();

    {
        let ns = wrap_ns(SystemNamespace::new());
        let node = RaftNode::new(
            node_id("obs-1"),
            voters.clone(),
            fast_config(),
            Arc::new(FileRaftStorage::new(raft_dir.clone())),
            Arc::new(NoopTransport),
            Arc::clone(&ns),
            Some(ns_path.clone()),
        )
        .unwrap();
        let outcome = node
            .adopt_pulled_snapshot(snapshot_resp(
                "cp-1",
                7,
                4,
                cp_state(7, "user/", &["user/"]),
            ))
            .unwrap();
        assert_eq!(outcome, AdoptOutcome::Adopted);
        assert!(
            ns_path.exists(),
            "the namespace must be persisted on adoption"
        );
        // Node dropped here (crash-style: everything was persisted on the
        // adoption path itself).
    }

    // Restart: load the persisted namespace (as main.rs does), then the
    // raft state over it.
    let restored_ns = SystemNamespace::load(&ns_path)
        .unwrap()
        .expect("persisted namespace must load");
    let ns = wrap_ns(restored_ns);
    let node = RaftNode::new(
        node_id("obs-1"),
        voters,
        fast_config(),
        Arc::new(FileRaftStorage::new(raft_dir)),
        Arc::new(NoopTransport),
        Arc::clone(&ns),
        Some(ns_path),
    )
    .unwrap();

    assert!(
        ns.read().unwrap().get_placement_policy("user/").is_some(),
        "the pulled policy must survive the restart"
    );
    let status = node.status();
    assert_eq!(status.last_applied, 4, "snapshot meta must be restored");
    assert_eq!(status.last_log_index, 4);
    assert_eq!(status.observer_ns_version_counter, 7);

    // The restored state is the adopted state: re-offering the same
    // snapshot is a no-op (not-newer).
    let outcome = node
        .adopt_pulled_snapshot(snapshot_resp(
            "cp-1",
            7,
            4,
            cp_state(7, "user/", &["user/"]),
        ))
        .unwrap();
    assert_eq!(
        outcome,
        AdoptOutcome::NotNewer,
        "the restart must restore the exact adopted (counter, index)"
    );
}

// ===========================================================================
// Adopt floor: a demoted ex-voter must not roll back its persisted
// namespace view (the in-memory guard basis is restored from the
// compaction snapshot, which can trail the persisted view)
// ===========================================================================

/// Build the durable state of a single-voter node whose persisted
/// namespace + apply marker ended up AHEAD of its raft compaction
/// snapshot (the steady state of any voter: the namespace persists on
/// every apply, compaction only folds periodically). Returns the last
/// applied policy version (== the replicated version counter here).
async fn seed_demotable_voter_state(
    raft_dir: std::path::PathBuf,
    ns_path: std::path::PathBuf,
    log_max: usize,
) -> u64 {
    let ns = wrap_ns(SystemNamespace::new());
    let voters: BTreeSet<NodeId> = [node_id("cp-1")].into_iter().collect();
    let node = RaftNode::new(
        node_id("cp-1"),
        voters,
        RaftConfig {
            log_max,
            ..fast_config()
        },
        Arc::new(FileRaftStorage::new(raft_dir)),
        Arc::new(NoopTransport),
        Arc::clone(&ns),
        Some(ns_path),
    )
    .unwrap();
    let consensus = ControlPlaneConsensus::with_raft(Arc::clone(&node));
    let mut last = 0;
    for _ in 0..2 {
        last = consensus
            .propose_policy_update(policy_spec("user/", 1))
            .await
            .unwrap()
            .version
            .0;
    }
    last
}

/// Restart the seeded node as an OBSERVER (demoted: self removed from the
/// voter set — the M-17-legitimized non-voter-authority operation) and
/// verify the pull guard is floored at the KEPT persisted namespace view,
/// not at the older compaction snapshot the in-memory state restores
/// from. Without the floor, a pull answered by a lagging voter (minority
/// side of a partition, or one restarted at its own snapshot boundary)
/// landing between the two would durably roll the namespace — and the
/// policy version this node's authority signatures carry — back.
#[tokio::test]
async fn adopt_floor_protects_kept_namespace_view_after_demotion() {
    let dir = tempfile::tempdir().unwrap();
    let raft_dir = dir.path().join("raft");
    let ns_path = dir.path().join("system_namespace.json");
    // log_max = 2 forces a compaction BEFORE the last apply: the snapshot
    // boundary folds up to the first policy while the persisted view (and
    // its marker) carries the second — the voter steady state, scaled down.
    let last = seed_demotable_voter_state(raft_dir.clone(), ns_path.clone(), 2).await;
    assert!(last >= 2, "two applied policies advance the counter twice");

    // Demotion restart: cp-1 is no longer in the voter set.
    let restored_ns = SystemNamespace::load(&ns_path)
        .unwrap()
        .expect("persisted namespace must load");
    let ns = wrap_ns(restored_ns);
    let voters: BTreeSet<NodeId> = [node_id("cp-2"), node_id("cp-3")].into_iter().collect();
    let node = RaftNode::new(
        node_id("cp-1"),
        voters,
        fast_config(),
        Arc::new(FileRaftStorage::new(raft_dir)),
        Arc::new(NoopTransport),
        Arc::clone(&ns),
        Some(ns_path),
    )
    .unwrap();

    // Setup sanity: the live guard basis (restored from the compaction
    // snapshot) genuinely trails the kept persisted view — the gap the
    // floor exists for. The namespace itself kept the newer view.
    let status = node.status();
    assert!(
        status.observer_ns_version_counter < last,
        "setup must reproduce the compaction lag (snapshot counter {} \
         vs persisted counter {last})",
        status.observer_ns_version_counter,
    );
    assert_eq!(
        ns.read()
            .unwrap()
            .get_placement_policy("user/")
            .unwrap()
            .version
            .0,
        last,
        "the persisted (newer) namespace view must be kept at startup"
    );

    // A lagging voter answers with a state strictly NEWER than the
    // compaction snapshot (same counter, higher index — the "adopt" shape
    // of an authority-only update) but OLDER than the kept view: without
    // the marker floor this was adopted, durably rolling user/ back.
    let lagging_counter = status.observer_ns_version_counter;
    let outcome = node
        .adopt_pulled_snapshot(snapshot_resp(
            "cp-2",
            lagging_counter,
            status.last_applied + 2,
            cp_state(lagging_counter, "user/", &[]),
        ))
        .unwrap();
    assert_eq!(
        outcome,
        AdoptOutcome::NotNewer,
        "a pull older than the kept persisted view must be rejected"
    );
    assert_eq!(
        ns.read()
            .unwrap()
            .get_placement_policy("user/")
            .unwrap()
            .version
            .0,
        last,
        "the namespace must NOT roll back below the persisted view"
    );

    // The floor must not over-block: genuinely newer state (counter
    // beyond the kept view — e.g. the voters moved on, or a
    // disaster-recovery Bootstrap re-floored and re-imported) is adopted.
    let outcome = node
        .adopt_pulled_snapshot(snapshot_resp(
            "cp-2",
            last + 1,
            6,
            cp_state(last + 1, "user/", &[]),
        ))
        .unwrap();
    assert_eq!(outcome, AdoptOutcome::Adopted);
    assert_eq!(
        ns.read()
            .unwrap()
            .get_placement_policy("user/")
            .unwrap()
            .version
            .0,
        last + 1
    );
}

/// Same protection when NO compaction ever happened (snapshot index 0):
/// the in-memory guard basis restores to (0, 0) while the persisted view
/// carries all applied entries — the whole history is in the gap.
#[tokio::test]
async fn adopt_floor_protects_kept_namespace_view_without_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let raft_dir = dir.path().join("raft");
    let ns_path = dir.path().join("system_namespace.json");
    let last = seed_demotable_voter_state(raft_dir.clone(), ns_path.clone(), 4096).await;
    assert!(last >= 2, "two applied policies advance the counter twice");

    let restored_ns = SystemNamespace::load(&ns_path)
        .unwrap()
        .expect("persisted namespace must load");
    let ns = wrap_ns(restored_ns);
    let voters: BTreeSet<NodeId> = [node_id("cp-2"), node_id("cp-3")].into_iter().collect();
    let node = RaftNode::new(
        node_id("cp-1"),
        voters,
        fast_config(),
        Arc::new(FileRaftStorage::new(raft_dir)),
        Arc::new(NoopTransport),
        Arc::clone(&ns),
        Some(ns_path),
    )
    .unwrap();
    assert_eq!(
        node.status().observer_ns_version_counter,
        0,
        "no compaction -> the live guard basis restores to zero"
    );

    // Pre-floor, ANY non-empty pulled state beat (0, 0) — including one
    // older than everything this node had persisted.
    let outcome = node
        .adopt_pulled_snapshot(snapshot_resp("cp-2", 1, 1, cp_state(1, "user/", &[])))
        .unwrap();
    assert_eq!(
        outcome,
        AdoptOutcome::NotNewer,
        "a pull older than the kept persisted view must be rejected"
    );
    assert_eq!(
        ns.read()
            .unwrap()
            .get_placement_policy("user/")
            .unwrap()
            .version
            .0,
        last
    );

    let outcome = node
        .adopt_pulled_snapshot(snapshot_resp(
            "cp-2",
            last + 1,
            9,
            cp_state(last + 1, "user/", &[]),
        ))
        .unwrap();
    assert_eq!(outcome, AdoptOutcome::Adopted);
}

// ===========================================================================
// Pull accounting: what counts as a successful vs failed round
// ===========================================================================

/// Transport stub returning a canned namespace snapshot (and failing all
/// other RPCs — observers never send them).
struct CannedSnapshotTransport {
    resp: std::sync::Mutex<NamespaceSnapshotResponse>,
}

impl CannedSnapshotTransport {
    fn new(resp: NamespaceSnapshotResponse) -> Self {
        Self {
            resp: std::sync::Mutex::new(resp),
        }
    }

    fn set(&self, resp: NamespaceSnapshotResponse) {
        *self.resp.lock().unwrap() = resp;
    }
}

impl RaftTransport for CannedSnapshotTransport {
    fn request_vote(
        &self,
        to: NodeId,
        _req: asteroidb_poc::control_plane::raft::types::RequestVoteRequest,
    ) -> TransportFuture<'_, asteroidb_poc::control_plane::raft::types::RequestVoteResponse> {
        Box::pin(async move { Err(TransportError(format!("no transport to {}", to.0))) })
    }

    fn append_entries(
        &self,
        to: NodeId,
        _req: AppendEntriesRequest,
    ) -> TransportFuture<'_, asteroidb_poc::control_plane::raft::types::AppendEntriesResponse> {
        Box::pin(async move { Err(TransportError(format!("no transport to {}", to.0))) })
    }

    fn install_snapshot(
        &self,
        to: NodeId,
        _req: asteroidb_poc::control_plane::raft::types::InstallSnapshotRequest,
    ) -> TransportFuture<'_, asteroidb_poc::control_plane::raft::types::InstallSnapshotResponse>
    {
        Box::pin(async move { Err(TransportError(format!("no transport to {}", to.0))) })
    }

    fn fetch_namespace_snapshot(
        &self,
        _to: NodeId,
        _req: NamespaceSnapshotRequest,
    ) -> TransportFuture<'_, NamespaceSnapshotResponse> {
        let resp = self.resp.lock().unwrap().clone();
        Box::pin(async move { Ok(resp) })
    }

    fn resolve_addr(&self, _id: &NodeId) -> Option<String> {
        None
    }
}

/// A guard-rejected pull (responder outside the voter set — the address
/// misconfiguration the guard exists for) must count as a FAILURE and
/// must NOT refresh the freshness timestamp: otherwise the documented
/// pull-age alert and `observer_ns_pull_failure_total` alert both stay
/// green while the namespace silently re-freezes. Healthy no-op rounds
/// (not-newer) still count as success.
#[tokio::test]
async fn pull_accounting_counts_guard_rejection_as_failure() {
    let ns = wrap_ns(SystemNamespace::new());
    let transport = Arc::new(CannedSnapshotTransport::new(snapshot_resp(
        "rogue",
        50,
        50,
        cp_state(50, "rogue/", &[]),
    )));
    let voters: BTreeSet<NodeId> = [node_id("cp-1"), node_id("cp-2")].into_iter().collect();
    let node = RaftNode::new(
        node_id("obs-1"),
        voters,
        fast_config(),
        Arc::new(MemRaftStorage::new()),
        Arc::clone(&transport) as Arc<dyn RaftTransport>,
        Arc::clone(&ns),
        None,
    )
    .unwrap();

    // Round 1: the fetch "succeeds" (HTTP 200 analogue) but the responder
    // is outside the voter set -> failed round, freshness NOT refreshed.
    let result = node.pull_namespace_once(&node_id("cp-1")).await;
    assert!(
        result.is_err(),
        "a guard-rejected pull must surface as an error to the driver"
    );
    let status = node.status();
    assert_eq!(status.observer_ns_pull_failure_total, 1);
    assert_eq!(status.observer_ns_pull_success_total, 0);
    assert_eq!(
        status.observer_ns_last_pull_unix_ms, 0,
        "a guard-rejected pull must not refresh the pull-age signal"
    );
    assert!(
        ns.read().unwrap().get_placement_policy("rogue/").is_none(),
        "nothing may be adopted from outside the voter set"
    );

    // Round 2: a healthy voter answers with not-newer state -> success.
    transport.set(snapshot_resp("cp-1", 0, 0, ControlPlaneState::default()));
    let adopted = node.pull_namespace_once(&node_id("cp-1")).await.unwrap();
    assert!(!adopted);
    let status = node.status();
    assert_eq!(status.observer_ns_pull_success_total, 1);
    assert_eq!(status.observer_ns_pull_failure_total, 1);
    assert!(
        status.observer_ns_last_pull_unix_ms > 0,
        "healthy no-op rounds keep the freshness signal alive"
    );
}

/// Storage stub whose hard-state save always fails (read-only / full
/// disk), failing the adoption AFTER a successful fetch.
struct FailingHardStateStorage(MemRaftStorage);

impl RaftStorage for FailingHardStateStorage {
    fn save_hard_state(
        &self,
        _hard: &asteroidb_poc::control_plane::raft::core::HardState,
    ) -> Result<(), String> {
        Err("injected: disk full".into())
    }

    fn save_log(
        &self,
        meta: &asteroidb_poc::control_plane::raft::core::SnapshotMeta,
        state: &ControlPlaneState,
        entries: &[LogEntry],
    ) -> Result<(), String> {
        self.0.save_log(meta, state, entries)
    }

    fn load(
        &self,
    ) -> Result<Option<asteroidb_poc::control_plane::raft::storage::PersistedRaft>, String> {
        self.0.load()
    }
}

/// Local adoption/persistence failures (the fetch worked, the durable
/// install did not) must count in `observer_ns_pull_failure_total` — the
/// counter docs/ops-guide tells operators to alert on — and must not
/// refresh the freshness timestamp.
#[tokio::test]
async fn pull_accounting_counts_adoption_persistence_failure() {
    let ns = wrap_ns(SystemNamespace::new());
    let transport = Arc::new(CannedSnapshotTransport::new(snapshot_resp(
        "cp-1",
        5,
        3,
        cp_state(5, "user/", &[]),
    )));
    let voters: BTreeSet<NodeId> = [node_id("cp-1"), node_id("cp-2")].into_iter().collect();
    let node = RaftNode::new(
        node_id("obs-1"),
        voters,
        fast_config(),
        Arc::new(FailingHardStateStorage(MemRaftStorage::new())),
        transport,
        Arc::clone(&ns),
        None,
    )
    .unwrap();

    let result = node.pull_namespace_once(&node_id("cp-1")).await;
    assert!(
        result.is_err(),
        "an adoption persistence failure must surface as an error"
    );
    let status = node.status();
    assert_eq!(
        status.observer_ns_pull_failure_total, 1,
        "persistence failures must count as failed pulls (ops-guide alert)"
    );
    assert_eq!(status.observer_ns_pull_success_total, 0);
    assert_eq!(status.observer_ns_last_pull_unix_ms, 0);
}

// ===========================================================================
// T-4: partition -> failure counters -> automatic recovery -> fence
// ===========================================================================

/// While its pull targets are unreachable the observer counts failures
/// and keeps its last state (its reports would keep carrying the old —
/// still valid — version); once the voter becomes reachable it catches
/// up within a few pull intervals, and the observer's own NodeRunner
/// fences the superseded version exactly like on a voter.
#[tokio::test(flavor = "multi_thread")]
async fn t4_partition_failure_counters_recovery_and_fence() {
    let voter_id = node_id("cp-1");
    let obs_id = node_id("obs-1");
    let voters: BTreeSet<NodeId> = [voter_id.clone()].into_iter().collect();

    // Pick the voter's address, then CLOSE the listener: connections are
    // refused (fast failures) until the "heal" phase re-binds it. A
    // bound-but-unserved listener would instead park each pull in the
    // accept backlog for the full 5s HTTP timeout.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);
    let mut peer_map = HashMap::new();
    peer_map.insert(voter_id.0.clone(), addr.clone());

    let (shutdown_tx, _) = watch::channel(false);

    // Voter: single-voter cluster, elects itself and commits instantly.
    let voter_ns = wrap_ns(SystemNamespace::new());
    let voter = RaftNode::new(
        voter_id.clone(),
        voters.clone(),
        fast_config(),
        Arc::new(MemRaftStorage::new()),
        Arc::new(NoopTransport),
        Arc::clone(&voter_ns),
        None,
    )
    .unwrap();
    let consensus = ControlPlaneConsensus::with_raft(Arc::clone(&voter));
    let p1 = consensus
        .propose_policy_update(policy_spec("user/", 1))
        .await
        .unwrap();

    // Observer with a fast pull aimed at the (still dark) voter address.
    let obs_ns = wrap_ns(SystemNamespace::new());
    let obs_raft = RaftNode::new(
        obs_id.clone(),
        voters,
        RaftConfig {
            observer_pull_interval: Duration::from_millis(100),
            ..fast_config()
        },
        Arc::new(MemRaftStorage::new()),
        Arc::new(HttpRaftTransport::new(peer_map, None, None)),
        Arc::clone(&obs_ns),
        None,
    )
    .unwrap();
    spawn_raft_driver(Arc::clone(&obs_raft), shutdown_tx.subscribe());

    // Partition phase: failures accumulate, nothing is adopted.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let status = obs_raft.status();
        if status.observer_ns_pull_failure_total >= 2 {
            assert_eq!(status.observer_ns_pull_success_total, 0);
            assert_eq!(status.observer_ns_last_pull_unix_ms, 0);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pull failures must be counted while the voter is unreachable"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        obs_ns
            .read()
            .unwrap()
            .get_placement_policy("user/")
            .is_none(),
        "nothing must be adopted while partitioned"
    );

    // Heal: re-bind the voter's address and start serving its HTTP API.
    let listener = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => break l,
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => panic!("could not re-bind the voter address {addr}: {e}"),
            }
        }
    };
    let (state, _certified) = app_state(
        &voter_id,
        Arc::clone(&voter_ns),
        ControlPlaneConsensus::with_raft(Arc::clone(&voter)),
    );
    let app = router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Recovery: the observer catches up automatically (bounded by the
    // backoff cap, well within the deadline for the few failures above).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if obs_ns
            .read()
            .unwrap()
            .get_placement_policy("user/")
            .is_some_and(|p| p.version.0 == p1.version.0)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the observer must catch up automatically after the partition heals"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(obs_raft.status().observer_ns_pull_success_total >= 1);

    // Observer-side fencing: run the observer's NodeRunner (sharing its
    // namespace via the certified API), bump on the voter, and verify the
    // superseded version is fenced on the OBSERVER after the next pull —
    // the same detect_version_changes chain as on voters.
    let obs_api = Arc::new(Mutex::new(CertifiedApi::new(
        obs_id.clone(),
        Arc::clone(&obs_ns),
    )));
    let runner_config = NodeRunnerConfig {
        certification_interval: Duration::from_millis(20),
        cleanup_interval: Duration::from_secs(60),
        compaction_check_interval: Duration::from_secs(60),
        frontier_report_interval: Duration::from_secs(60),
        sync_interval: None,
        ping_interval: None,
        ..NodeRunnerConfig::default()
    };
    let mut runner = NodeRunner::new(
        obs_id.clone(),
        Arc::clone(&obs_api),
        CompactionEngine::with_defaults(),
        runner_config,
        Arc::new(RuntimeMetrics::default()),
    )
    .await;
    let runner_shutdown = runner.shutdown_handle();
    let runner_task = tokio::spawn(async move {
        runner.run().await;
    });

    // Let the runner establish its version tracking at v1, then bump.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let p2 = consensus
        .propose_policy_update(policy_spec("user/", 1))
        .await
        .unwrap();
    assert_eq!(p2.version.0, p1.version.0 + 1);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        {
            let api = obs_api.lock().await;
            if api.is_version_fenced(&kr("user/"), &p1.version) {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the observer's runner must fence the superseded version after the pull"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(
        obs_ns
            .read()
            .unwrap()
            .get_placement_policy("user/")
            .is_some_and(|p| p.version.0 == p2.version.0),
        "the observer namespace must be at the bumped version"
    );

    let _ = runner_shutdown.send(true);
    let _ = runner_task.await;
    let _ = shutdown_tx.send(true);
}

// ===========================================================================
// Regression: voters never run the pull loop
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn single_voter_node_runs_no_pull_loop() {
    let ns = wrap_ns(SystemNamespace::new());
    let voters: BTreeSet<NodeId> = [node_id("cp-1")].into_iter().collect();
    let node = RaftNode::new(
        node_id("cp-1"),
        voters,
        RaftConfig {
            observer_pull_interval: Duration::from_millis(10),
            ..fast_config()
        },
        Arc::new(MemRaftStorage::new()),
        Arc::new(NoopTransport),
        ns,
        None,
    )
    .unwrap();
    let (shutdown_tx, _) = watch::channel(false);
    spawn_raft_driver(Arc::clone(&node), shutdown_tx.subscribe());

    tokio::time::sleep(Duration::from_millis(300)).await;
    let status = node.status();
    assert_eq!(status.role, "leader", "single voter elects itself");
    assert_eq!(
        status.observer_ns_pull_success_total + status.observer_ns_pull_failure_total,
        0,
        "a voter must never run the observer pull loop"
    );
    let _ = shutdown_tx.send(true);
}
