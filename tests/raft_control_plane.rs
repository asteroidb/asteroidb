//! Control-plane Raft integration tests.
//!
//! Covers, end to end:
//! - three real HTTP servers electing a leader over `HttpRaftTransport`
//!   (bincode internal endpoints; the rolling-upgrade JSON fallback is
//!   unit-tested against a bincode-rejecting peer in
//!   `src/network/raft_transport.rs`) and replicating a policy update to
//!   every voter with an identical commit-order version;
//! - NOT_LEADER semantics on followers (503 + leader hint headers) while
//!   eventual-path writes on the same follower keep working (CP control
//!   plane / AP data plane split);
//! - durable restart: term / votedFor / log survive via `FileRaftStorage`,
//!   committed entries are never lost, and a restarted node cannot
//!   double-vote in a term it already voted in;
//! - leader crash → re-election → committed entries preserved, applied
//!   exactly once (no duplicate version bumps);
//! - log compaction + `InstallSnapshot` catch-up for a lagging follower.
//!
//! Election timeouts are shortened via `RaftConfig` for test speed; the
//! production defaults stay conservative (high-latency links).

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::raft::node::{RaftConfig, RaftNode};
use asteroidb_poc::control_plane::raft::spawn_raft_driver;
use asteroidb_poc::control_plane::raft::storage::{FileRaftStorage, MemRaftStorage, RaftStorage};
use asteroidb_poc::control_plane::raft::transport::ChannelNetwork;
use asteroidb_poc::control_plane::raft::types::{PolicySpec, RequestVoteRequest};
use asteroidb_poc::control_plane::system_namespace::SystemNamespace;
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::network::raft_transport::HttpRaftTransport;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::types::NodeId;

use tokio::sync::{Mutex, watch};

fn node_id(s: &str) -> NodeId {
    NodeId(s.into())
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
    }
}

fn policy_spec(prefix: &str, replica_count: usize) -> PolicySpec {
    PolicySpec {
        prefix: prefix.into(),
        replica_count,
        required_tags: BTreeSet::new(),
        forbidden_tags: BTreeSet::new(),
        allow_local_write_on_partition: false,
        certified: false,
        max_read_latency_ms: None,
        preferred_cost_tier: None,
    }
}

/// Propose until committed, absorbing the poll-then-propose race: a
/// freshly-won leader can step down again immediately when a concurrent
/// higher-term candidate campaigns (the prevote-lite guard deliberately
/// exempts leaders), so `is_leader()` polling followed by a single
/// propose is inherently racy — retry `NotLeader` against the current
/// leader among `candidates`. Returns the committing leader's index and
/// the applied policy.
async fn propose_committed(
    nodes: &[Arc<RaftNode>],
    candidates: &[usize],
    spec: PolicySpec,
) -> (usize, asteroidb_poc::placement::PlacementPolicy) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let leader = loop {
            if let Some(&i) = candidates.iter().find(|&&i| nodes[i].is_leader()) {
                break i;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no leader elected among {candidates:?} within 15s"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let consensus = ControlPlaneConsensus::with_raft(Arc::clone(&nodes[leader]));
        match consensus.propose_policy_update(spec.clone()).await {
            Ok(policy) => break (leader, policy),
            Err(asteroidb_poc::error::CrdtError::NotLeader { .. }) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "proposal kept losing leadership within 15s"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(other) => panic!("proposal failed with a non-leadership error: {other:?}"),
        }
    }
}

fn app_state(
    id: &NodeId,
    namespace: Arc<RwLock<SystemNamespace>>,
    consensus: ControlPlaneConsensus,
) -> Arc<AppState> {
    Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(id.clone()))),
        certified: Arc::new(Mutex::new(CertifiedApi::new(
            id.clone(),
            Arc::clone(&namespace),
        ))),
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
    })
}

// ===========================================================================
// HTTP-level: 3 real servers, HttpRaftTransport, election + replication
// ===========================================================================

/// Spin up three HTTP servers wired with `HttpRaftTransport`, wait for a
/// leader, PUT a placement policy to the leader, and verify it replicates
/// (same version) to every voter. Then exercise NOT_LEADER on a follower
/// and confirm the eventual write path there still works.
#[tokio::test(flavor = "multi_thread")]
async fn three_node_http_cluster_replicates_policy_updates() {
    let ids: Vec<NodeId> = (1..=3).map(|i| node_id(&format!("cp-{i}"))).collect();
    let voters: BTreeSet<NodeId> = ids.iter().cloned().collect();

    // Bind first so the static peer map is known before nodes start.
    let mut listeners = Vec::new();
    let mut peer_map = HashMap::new();
    for id in &ids {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        peer_map.insert(id.0.clone(), listener.local_addr().unwrap().to_string());
        listeners.push(listener);
    }

    let (shutdown_tx, _) = watch::channel(false);
    let mut namespaces = Vec::new();
    let mut addrs = Vec::new();
    let mut raft_nodes = Vec::new();

    for (id, listener) in ids.iter().zip(listeners) {
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
        let state = app_state(
            id,
            Arc::clone(&namespace),
            ControlPlaneConsensus::with_raft(Arc::clone(&raft)),
        );
        addrs.push(peer_map[&id.0].clone());
        namespaces.push(namespace);
        raft_nodes.push(Arc::clone(&raft));
        let app = router(state);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
    }

    let client = reqwest::Client::new();

    // PUT a placement policy to the current leader (approvals-free
    // new-style body). Rediscover the leader via the public status endpoint
    // and retry on 503 NOT_LEADER: a freshly-observed leader can step down
    // again before the PUT lands (a concurrent higher-term candidate — the
    // poll-then-propose race), so a single-shot PUT is inherently flaky.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let (leader_idx, put_body) = loop {
        let mut found = None;
        for (i, addr) in addrs.iter().enumerate() {
            let url = format!("http://{addr}/api/control-plane/raft/status");
            if let Ok(resp) = client.get(&url).send().await
                && let Ok(status) = resp.json::<serde_json::Value>().await
                && status["role"] == "leader"
            {
                found = Some(i);
                break;
            }
        }
        if let Some(i) = found {
            let resp = client
                .put(format!("http://{}/api/control-plane/policies", addrs[i]))
                .json(&serde_json::json!({
                    "key_range_prefix": "user/",
                    "replica_count": 3,
                    "certified": true
                }))
                .send()
                .await
                .unwrap();
            match resp.status() {
                reqwest::StatusCode::OK => {
                    break (i, resp.json::<serde_json::Value>().await.unwrap());
                }
                reqwest::StatusCode::SERVICE_UNAVAILABLE => {
                    // Lost leadership between the status poll and the PUT.
                }
                other => panic!("leader PUT failed with unexpected status {other}"),
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no HTTP raft leader accepted the PUT within 15s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let committed_version = put_body["version"].as_u64().unwrap();
    assert!(committed_version >= 1);

    // The policy replicates to every node with the identical version.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut replicated = 0;
        for addr in &addrs {
            let url = format!("http://{addr}/api/control-plane/policies/user%2F");
            if let Ok(resp) = client.get(&url).send().await
                && resp.status() == reqwest::StatusCode::OK
            {
                let body: serde_json::Value = resp.json().await.unwrap();
                assert_eq!(
                    body["version"].as_u64().unwrap(),
                    committed_version,
                    "every voter must hold the identical commit-order version"
                );
                replicated += 1;
            }
        }
        if replicated == addrs.len() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "policy did not replicate to all nodes within 15s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A follower answers control-plane mutations with 503 NOT_LEADER and
    // hint headers pointing at the actual leader...
    let follower_idx = (0..addrs.len()).find(|i| *i != leader_idx).unwrap();
    let follower_addr = &addrs[follower_idx];
    let resp = client
        .put(format!("http://{follower_addr}/api/control-plane/policies"))
        .json(&serde_json::json!({
            "key_range_prefix": "denied/",
            "replica_count": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        resp.headers()
            .get("x-asteroidb-leader-id")
            .and_then(|v| v.to_str().ok()),
        Some(ids[leader_idx].0.as_str()),
        "leader hint header must name the leader"
    );
    assert_eq!(
        resp.headers()
            .get("x-asteroidb-leader-addr")
            .and_then(|v| v.to_str().ok()),
        Some(addrs[leader_idx].as_str()),
        "leader hint header must carry the leader's address"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error_code"], "NOT_LEADER");

    // ... while the follower's EVENTUAL write path keeps working: the CP
    // control plane never blocks AP data-plane availability.
    let resp = client
        .post(format!("http://{follower_addr}/api/eventual/write"))
        .json(&serde_json::json!({"type": "counter_inc", "key": "hits"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "eventual writes must not depend on control-plane leadership"
    );

    let _ = shutdown_tx.send(true);
}

// ===========================================================================
// Durable restart (FileRaftStorage)
// ===========================================================================

/// Restart recovery: hard state (term / votedFor), the log, and committed
/// entries survive a full-cluster stop/start on `FileRaftStorage`; after
/// restart a leader re-emerges and every namespace re-converges on the
/// committed policy (no committed-entry loss).
#[tokio::test(flavor = "multi_thread")]
async fn restart_restores_state_and_committed_entries() {
    let ids: Vec<NodeId> = (1..=3).map(|i| node_id(&format!("cp-{i}"))).collect();
    let voters: BTreeSet<NodeId> = ids.iter().cloned().collect();
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();

    let committed_version;
    let term_before;

    // --- First incarnation: elect, commit, stop. ---
    {
        let network = ChannelNetwork::new();
        let (shutdown_tx, _) = watch::channel(false);
        let mut nodes = Vec::new();
        for (id, dir) in ids.iter().zip(&dirs) {
            let raft = RaftNode::new(
                id.clone(),
                voters.clone(),
                fast_config(),
                Arc::new(FileRaftStorage::new(dir.path().join("raft"))),
                network.transport_for(id.clone()),
                wrap_ns(SystemNamespace::new()),
                None,
            )
            .unwrap();
            network.register(id.clone(), &raft);
            spawn_raft_driver(Arc::clone(&raft), shutdown_tx.subscribe());
            nodes.push(raft);
        }

        let (leader, policy) =
            propose_committed(&nodes, &[0, 1, 2], policy_spec("durable/", 3)).await;
        committed_version = policy.version;
        term_before = nodes[leader].status().term;

        // Let the commit index propagate to the followers, then stop.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = shutdown_tx.send(true);
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Nodes dropped here (crash: no graceful shutdown persistence —
        // everything needed was fsynced on the write path).
    }

    // --- Second incarnation: same storage dirs, fresh namespaces. ---
    let network = ChannelNetwork::new();
    let (shutdown_tx, _) = watch::channel(false);
    let mut nodes = Vec::new();
    let mut namespaces = Vec::new();
    for (id, dir) in ids.iter().zip(&dirs) {
        let ns = wrap_ns(SystemNamespace::new());
        let raft = RaftNode::new(
            id.clone(),
            voters.clone(),
            fast_config(),
            Arc::new(FileRaftStorage::new(dir.path().join("raft"))),
            network.transport_for(id.clone()),
            Arc::clone(&ns),
            None,
        )
        .unwrap();
        assert!(
            raft.status().term >= term_before,
            "current term must be restored from disk (no term regression)"
        );
        assert!(
            raft.status().last_log_index >= 1,
            "log entries must be restored from disk"
        );
        network.register(id.clone(), &raft);
        spawn_raft_driver(Arc::clone(&raft), shutdown_tx.subscribe());
        nodes.push(raft);
        namespaces.push(ns);
    }

    // A leader re-emerges and the committed policy replays into every
    // (fresh) namespace with its original version — committed entries are
    // never lost across restarts.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let restored = namespaces.iter().all(|ns| {
            ns.read()
                .unwrap()
                .get_placement_policy("durable/")
                .is_some_and(|p| p.version == committed_version)
        });
        if restored {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "committed policy was not restored on all nodes within 10s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = shutdown_tx.send(true);
}

/// Restart with a COMPACTED log must not roll the namespace back behind
/// newer committed state: `system_namespace.json` is persisted after every
/// apply (state at M), while the raft snapshot sits at the last compaction
/// point N <= M. The apply marker proves the JSON view is at-or-beyond the
/// snapshot, so startup keeps it instead of installing the snapshot over it
/// — policies committed in N+1..=M must not vanish until a leader
/// re-advances the commit index (indefinitely, without quorum).
#[tokio::test]
async fn restart_with_compacted_log_keeps_newer_persisted_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let ns_path = dir.path().join("system_namespace.json");
    let raft_dir = dir.path().join("raft");

    let last_version;
    {
        // Single voter: elects itself synchronously and commits + applies
        // every proposal immediately; log_max=4 forces a mid-stream
        // compaction, leaving applied entries beyond the snapshot.
        let single: BTreeSet<NodeId> = [node_id("cp-1")].into_iter().collect();
        let ns = wrap_ns(SystemNamespace::new());
        let node = RaftNode::new(
            node_id("cp-1"),
            single,
            RaftConfig {
                log_max: 4,
                ..fast_config()
            },
            Arc::new(FileRaftStorage::new(raft_dir.clone())),
            Arc::new(asteroidb_poc::control_plane::raft::transport::NoopTransport),
            Arc::clone(&ns),
            Some(ns_path.clone()),
        )
        .unwrap();
        let consensus = ControlPlaneConsensus::with_raft(Arc::clone(&node));
        let mut v = asteroidb_poc::types::PolicyVersion(0);
        for i in 0..7 {
            let policy = consensus
                .propose_policy_update(policy_spec(&format!("compacted-{i}/"), 3))
                .await
                .expect("single-voter proposals commit synchronously");
            v = policy.version;
        }
        last_version = v;
    }
    assert!(
        ns_path.exists(),
        "namespace JSON must have been persisted after applies"
    );
    {
        // Sanity: the durable log must hold a compacted snapshot at N with
        // applied entries beyond it (N < M) — otherwise this test would not
        // exercise the rollback hazard.
        let persisted = FileRaftStorage::new(raft_dir.clone())
            .load()
            .unwrap()
            .unwrap();
        assert!(
            persisted.snapshot_meta.last_included_index > 0,
            "compaction must have happened"
        );
        assert!(
            !persisted.entries.is_empty(),
            "applied entries beyond the snapshot must remain in the tail"
        );
    }

    // Restart as a FOLLOWER of a 3-voter cluster (no self-election, no
    // leader reachable): the commit index resets to the snapshot, so
    // nothing re-applies — the JSON-restored namespace view must stand.
    let restored = SystemNamespace::load(&ns_path)
        .expect("namespace JSON must parse")
        .expect("namespace JSON must exist");
    let ns = wrap_ns(restored);
    let voters: BTreeSet<NodeId> = ["cp-1", "cp-2", "cp-3"]
        .iter()
        .map(|s| node_id(s))
        .collect();
    let node = RaftNode::new(
        node_id("cp-1"),
        voters,
        fast_config(),
        Arc::new(FileRaftStorage::new(raft_dir)),
        Arc::new(asteroidb_poc::control_plane::raft::transport::NoopTransport),
        Arc::clone(&ns),
        Some(ns_path),
    )
    .unwrap();

    let status = node.status();
    assert!(
        status.last_applied < status.last_log_index,
        "restart must reset apply progress to the snapshot boundary"
    );
    {
        let ns = ns.read().unwrap();
        for i in 0..7 {
            assert!(
                ns.get_placement_policy(&format!("compacted-{i}/"))
                    .is_some(),
                "policy compacted-{i}/ (committed and persisted at state M) \
                 must not be rolled back to the snapshot at N"
            );
        }
        assert_eq!(
            ns.get_placement_policy("compacted-6/").unwrap().version,
            last_version,
            "the newest committed policy keeps its commit-order version"
        );
    }
}

/// Election safety across restarts: a node that granted its vote in term T
/// must refuse a different candidate in term T after restarting from disk
/// (votedFor is fsynced BEFORE the grant is sent).
#[tokio::test]
async fn restarted_node_cannot_double_vote_in_same_term() {
    let dir = tempfile::tempdir().unwrap();
    let voters: BTreeSet<NodeId> = ["cp-1", "cp-2", "cp-3"]
        .iter()
        .map(|s| node_id(s))
        .collect();
    let ns = wrap_ns(SystemNamespace::new());

    let make_node = || {
        RaftNode::new(
            node_id("cp-3"),
            voters.clone(),
            fast_config(),
            Arc::new(FileRaftStorage::new(dir.path().join("raft"))),
            Arc::new(asteroidb_poc::control_plane::raft::transport::NoopTransport),
            Arc::clone(&ns),
            None,
        )
        .unwrap()
    };

    let voter = make_node();
    let resp = voter
        .handle_request_vote(RequestVoteRequest {
            term: 5,
            candidate_id: node_id("cp-1"),
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(resp.vote_granted, "first vote in term 5 is granted");
    drop(voter);

    // "Crash" and restart from the same directory: the persisted votedFor
    // must survive and block a second grant in the same term.
    let restarted = make_node();
    let resp = restarted
        .handle_request_vote(RequestVoteRequest {
            term: 5,
            candidate_id: node_id("cp-2"),
            last_log_index: 10,
            last_log_term: 5,
        })
        .unwrap();
    assert!(
        !resp.vote_granted,
        "restart must not allow a second vote in term 5 (split-brain enabler)"
    );
    // The original candidate is still re-granted idempotently.
    let resp = restarted
        .handle_request_vote(RequestVoteRequest {
            term: 5,
            candidate_id: node_id("cp-1"),
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(resp.vote_granted);
}

/// Persistence failure means NO response: with failing storage a vote is
/// never granted and a proposal fails with a storage error — the "respond
/// only after fsync" invariant, verified end to end at node level.
#[tokio::test]
async fn persistence_failure_suppresses_votes_and_proposals() {
    let storage = Arc::new(MemRaftStorage::new());
    let ns = wrap_ns(SystemNamespace::new());
    let voters: BTreeSet<NodeId> = ["cp-1", "cp-2", "cp-3"]
        .iter()
        .map(|s| node_id(s))
        .collect();
    let node = RaftNode::new(
        node_id("cp-1"),
        voters,
        fast_config(),
        Arc::clone(&storage) as Arc<dyn RaftStorage>,
        Arc::new(asteroidb_poc::control_plane::raft::transport::NoopTransport),
        ns,
        None,
    )
    .unwrap();

    storage.set_fail(true);
    let err = node
        .handle_request_vote(RequestVoteRequest {
            term: 3,
            candidate_id: node_id("cp-2"),
            last_log_index: 0,
            last_log_term: 0,
        })
        .expect_err("a vote must never be produced without durable votedFor");
    assert!(
        matches!(err, asteroidb_poc::error::CrdtError::Storage(_)),
        "unexpected error: {err:?}"
    );
    assert!(
        storage.persisted_hard().is_none(),
        "nothing may have been recorded"
    );

    // Recovered storage: the retried vote goes through.
    storage.set_fail(false);
    let resp = node
        .handle_request_vote(RequestVoteRequest {
            term: 3,
            candidate_id: node_id("cp-2"),
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(resp.vote_granted);
    assert_eq!(storage.persisted_hard().unwrap().current_term, 3);
}

// ===========================================================================
// Leader crash → re-election; snapshot catch-up
// ===========================================================================

/// Crash the leader (permanent isolation), re-elect among the survivors,
/// and verify committed entries survive and are applied exactly once
/// (stable versions, no duplicates).
#[tokio::test(flavor = "multi_thread")]
async fn leader_crash_reelection_preserves_committed_entries() {
    let ids: Vec<NodeId> = (1..=3).map(|i| node_id(&format!("cp-{i}"))).collect();
    let voters: BTreeSet<NodeId> = ids.iter().cloned().collect();
    let network = ChannelNetwork::new();
    let (shutdown_tx, _) = watch::channel(false);

    let mut nodes = Vec::new();
    let mut namespaces = Vec::new();
    for id in &ids {
        let ns = wrap_ns(SystemNamespace::new());
        let raft = RaftNode::new(
            id.clone(),
            voters.clone(),
            fast_config(),
            Arc::new(MemRaftStorage::new()),
            network.transport_for(id.clone()),
            Arc::clone(&ns),
            None,
        )
        .unwrap();
        network.register(id.clone(), &raft);
        spawn_raft_driver(Arc::clone(&raft), shutdown_tx.subscribe());
        nodes.push(raft);
        namespaces.push(ns);
    }

    let (leader, before_crash) =
        propose_committed(&nodes, &[0, 1, 2], policy_spec("survives/", 3)).await;

    // "Crash" the leader: cut it off permanently.
    network.isolate(&ids[leader]);

    // A survivor takes over and keeps committing.
    let survivors: Vec<usize> = (0..3).filter(|i| *i != leader).collect();
    propose_committed(&nodes, &survivors, policy_spec("after-crash/", 2)).await;

    // Both policies present on both survivors; the pre-crash policy keeps
    // its exact version (applied exactly once, no duplicate re-apply).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let converged = survivors.iter().all(|&i| {
            let ns = namespaces[i].read().unwrap();
            ns.get_placement_policy("survives/")
                .is_some_and(|p| p.version == before_crash.version)
                && ns.get_placement_policy("after-crash/").is_some()
        });
        if converged {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "survivors did not converge within 10s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = shutdown_tx.send(true);
}

/// Log compaction + InstallSnapshot: with a tiny `log_max`, a follower that
/// slept through many commits falls behind the leader's compacted log and
/// must be caught up via a single-message snapshot, converging on the same
/// namespace contents.
#[tokio::test(flavor = "multi_thread")]
async fn lagging_follower_catches_up_via_install_snapshot() {
    let ids: Vec<NodeId> = (1..=3).map(|i| node_id(&format!("cp-{i}"))).collect();
    let voters: BTreeSet<NodeId> = ids.iter().cloned().collect();
    let network = ChannelNetwork::new();
    let (shutdown_tx, _) = watch::channel(false);

    let config = RaftConfig {
        log_max: 4, // force compaction quickly
        ..fast_config()
    };

    let mut nodes = Vec::new();
    let mut namespaces = Vec::new();
    for id in &ids {
        let ns = wrap_ns(SystemNamespace::new());
        let raft = RaftNode::new(
            id.clone(),
            voters.clone(),
            config.clone(),
            Arc::new(MemRaftStorage::new()),
            network.transport_for(id.clone()),
            Arc::clone(&ns),
            None,
        )
        .unwrap();
        network.register(id.clone(), &raft);
        spawn_raft_driver(Arc::clone(&raft), shutdown_tx.subscribe());
        nodes.push(raft);
        namespaces.push(ns);
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let leader = loop {
        if let Some(i) = (0..3).find(|&i| nodes[i].is_leader()) {
            break i;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no leader within 10s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    // Put a follower to sleep.
    let lagger = (0..3).find(|i| *i != leader).unwrap();
    network.isolate(&ids[lagger]);

    // Commit enough entries to trigger compaction past the lagger's log.
    let awake: Vec<usize> = (0..3).filter(|i| *i != lagger).collect();
    let mut last_version = 0;
    let mut last_leader = leader;
    for i in 0..12 {
        let (li, policy) =
            propose_committed(&nodes, &awake, policy_spec(&format!("bulk-{i}/"), 3)).await;
        last_leader = li;
        last_version = policy.version.0;
    }
    let leader_status = nodes[last_leader].status();
    assert!(
        leader_status.last_log_index > 12,
        "leader has the committed entries"
    );

    // Wake the lagger: it must catch up (via InstallSnapshot when the
    // leader's log was compacted past its next index) and converge.
    network.heal_all();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let caught_up = {
            let ns = namespaces[lagger].read().unwrap();
            (0..12).all(|i| ns.get_placement_policy(&format!("bulk-{i}/")).is_some())
                && ns
                    .get_placement_policy("bulk-11/")
                    .is_some_and(|p| p.version.0 == last_version)
        };
        if caught_up {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "lagging follower did not catch up within 15s"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let _ = shutdown_tx.send(true);
}
