//! P0-1: the certified replication lane.
//!
//! Before this lane existed, `CertifiedApi` owned a private store that no
//! anti-entropy path ever touched (`internal_sync` / `internal_delta_sync` /
//! `internal_digest_sync` / `internal_keys` all lock `state.eventual`). A
//! certified write therefore lived on exactly ONE disk: losing the writer
//! lost the value permanently, while the same key written through the
//! EVENTUAL path would have survived — the strongly-consistent plane was
//! strictly less durable than the weak one.
//!
//! These tests pin the contract of the pull-only certified delta lane that
//! closes that hole, and — just as importantly — that replication does NOT
//! smuggle a value past the FR-009 policy fence (#342): every replicated
//! entry carries the ORIGIN policy version it was written under, and the
//! receiver pins it there.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::authority::ack_frontier::AckFrontier;
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::network::sync::SyncClient;
use asteroidb_poc::network::{PeerConfig, PeerRegistry};
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::runtime::{NodeRunner, NodeRunnerConfig};
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::Mutex;
use tower::ServiceExt;

fn node_id(s: &str) -> NodeId {
    NodeId(s.into())
}

fn user_range() -> KeyRange {
    KeyRange {
        prefix: "user/".into(),
    }
}

/// A single-authority `user/` scope at the given policy version
/// (majority = 1, so one frontier report certifies).
fn user_namespace(version: u64) -> SystemNamespace {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: user_range(),
        authority_nodes: vec![node_id("auth-1")],
        auto_generated: false,
    });
    ns.set_placement_policy(PlacementPolicy::new(
        PolicyVersion(version),
        user_range(),
        1,
    ))
    .unwrap();
    ns
}

/// Bump the placement policy version in place (operator version bump).
fn bump_policy(ns: &Arc<RwLock<SystemNamespace>>, version: u64) {
    let mut guard = ns.write().unwrap();
    guard
        .set_placement_policy(PlacementPolicy::new(
            PolicyVersion(version),
            user_range(),
            1,
        ))
        .unwrap();
}

fn user_frontier(version: u64, cover_physical: u64) -> AckFrontier {
    AckFrontier {
        authority_id: node_id("auth-1"),
        frontier_hlc: HlcTimestamp {
            physical: cover_physical,
            logical: 0,
            node_id: "auth-1".into(),
        },
        key_range: user_range(),
        policy_version: PolicyVersion(version),
        digest_hash: "auth-1-cover".into(),
    }
}

struct Node {
    state: Arc<AppState>,
    namespace: Arc<RwLock<SystemNamespace>>,
}

fn build_node(name: &str, version: u64) -> Node {
    let namespace = Arc::new(RwLock::new(user_namespace(version)));
    let state = Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(node_id(name)))),
        certified: Arc::new(Mutex::new(CertifiedApi::new(
            node_id(name),
            Arc::clone(&namespace),
        ))),
        namespace: Arc::clone(&namespace),
        metrics: Arc::new(RuntimeMetrics::default()),
        peers: None,
        peer_persist_path: None,
        namespace_persist_path: None,
        consensus: Arc::new(Mutex::new(ControlPlaneConsensus::new(vec![]))),
        internal_token: None,
        self_node_id: None,
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
    Node { state, namespace }
}

async fn serve(app: axum::Router) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (addr, handle)
}

/// A `NodeRunner` whose certified sync lane targets `peer_addr`.
/// Sync/ping intervals are effectively off so tests drive
/// `run_certified_sync` explicitly — no background races.
async fn lane_runner(name: &str, node: &Node, peer_name: &str, peer_addr: &str) -> NodeRunner {
    lane_runner_with(name, node, peer_name, peer_addr, true).await
}

async fn lane_runner_with(
    name: &str,
    node: &Node,
    peer_name: &str,
    peer_addr: &str,
    certified_sync_enabled: bool,
) -> NodeRunner {
    let registry = PeerRegistry::new(
        node_id(name),
        vec![PeerConfig {
            node_id: node_id(peer_name),
            addr: peer_addr.to_string(),
        }],
    )
    .unwrap();
    let config = NodeRunnerConfig {
        certification_interval: Duration::from_millis(50),
        cleanup_interval: Duration::from_secs(3600),
        compaction_check_interval: Duration::from_secs(3600),
        frontier_report_interval: Duration::from_secs(3600),
        sync_interval: None,
        ping_interval: None,
        certified_sync_enabled,
        ..NodeRunnerConfig::default()
    };
    NodeRunner::with_sync(
        node_id(name),
        Arc::clone(&node.state.certified),
        CompactionEngine::with_defaults(),
        config,
        SyncClient::new(Arc::new(Mutex::new(registry))),
        Arc::clone(&node.state.eventual),
        Arc::clone(&node.state.metrics),
    )
    .await
}

async fn certified_write(state: &Arc<AppState>, key: &str, value: i64) -> StatusCode {
    let body = serde_json::json!({
        "key": key,
        "value": { "type": "counter", "value": value },
        "on_timeout": "pending"
    });
    let resp = router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/certified/write")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    resp.status()
}

async fn certified_read(state: &Arc<AppState>, key: &str) -> (StatusCode, serde_json::Value) {
    let resp = router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .uri(format!("/api/certified/{key}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn status_read(state: &Arc<AppState>, key: &str) -> serde_json::Value {
    let resp = router(Arc::clone(state))
        .oneshot(
            Request::builder()
                .uri(format!("/api/status/{key}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

// ---------------------------------------------------------------
// RED-1: the P0-1 contract itself.
// ---------------------------------------------------------------

/// A certified write must survive the total loss of the node that took
/// it. B pulls the value over the certified lane; A is then destroyed
/// (server aborted, state dropped) and B still serves the value.
///
/// `status: Pending` on B is asserted deliberately: certification state
/// is node-local and deliberately volatile (fail-closed). Replication
/// conveys the VALUE and its origin version, never a certification
/// verdict — B must re-certify from attestations it verifies itself.
#[tokio::test]
async fn certified_value_survives_writer_loss() {
    let a = build_node("cert-a", 1);
    let b = build_node("cert-b", 1);
    let (addr_a, server_a) = serve(router(Arc::clone(&a.state))).await;

    assert_eq!(
        certified_write(&a.state, "user/x", 7).await,
        StatusCode::OK,
        "certified write to A must succeed"
    );

    let mut runner_b = lane_runner("cert-b", &b, "cert-a", &addr_a.to_string()).await;
    runner_b.run_certified_sync().await;

    // Total loss of the writer: stop serving and drop every handle to A.
    server_a.abort();
    drop(a);

    let (code, body) = certified_read(&b.state, "user/x").await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(
        body["value"]["value"], 7,
        "the certified value must survive the loss of the node that accepted it; \
         found {body}"
    );
    assert_eq!(
        body["status"], "Pending",
        "certification status is node-local and volatile: replication carries the \
         value, never the verdict"
    );
}

// ---------------------------------------------------------------
// RED-2: replication must not smuggle a value past the FR-009 fence.
// ---------------------------------------------------------------

/// A value written under v1 and replicated to B keeps its ORIGIN version
/// on B. After B bumps to v2 and a v2 authority frontier covers the write
/// HLC, B must still NOT report `Certified` — the write is pinned to the
/// (now fenced) v1 scope, exactly as #342 requires on the writer.
///
/// Without the wire-carried origin, B would evaluate the v1 write against
/// its v2 frontier and certify it: the #342 hole reopened on every
/// non-writer node, with no crash or restart needed.
#[tokio::test]
async fn replicated_certified_write_keeps_its_origin_version() {
    let a = build_node("fence-a", 1);
    let b = build_node("fence-b", 1);
    let (addr_a, server_a) = serve(router(Arc::clone(&a.state))).await;

    assert_eq!(certified_write(&a.state, "user/x", 3).await, StatusCode::OK);
    let write_ts = {
        let api = a.state.certified.lock().await;
        api.pending_writes()[0].timestamp.clone()
    };

    let mut runner_b = lane_runner("fence-b", &b, "fence-a", &addr_a.to_string()).await;
    runner_b.run_certified_sync().await;
    {
        let api = b.state.certified.lock().await;
        assert!(
            api.store().get("user/x").is_some(),
            "test premise: B must have received the value"
        );
    }

    // Operator bumps the policy version on B, then a v2 authority
    // frontier arrives that covers the (v1) write's HLC.
    bump_policy(&b.namespace, 2);
    {
        let mut api = b.state.certified.lock().await;
        assert!(api.update_frontier(user_frontier(2, write_ts.physical + 10_000)));
    }

    // Run B's certification loop: it detects the version change (fencing
    // v1) and re-evaluates every tracked write.
    let stop = runner_b.shutdown_handle();
    let task = tokio::spawn(async move {
        runner_b.run().await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = stop.send(true);
    let _ = task.await;

    assert_eq!(
        status_read(&b.state, "user/x").await["status"],
        "Pending",
        "a v1 write replicated to B must NOT certify off B's v2 frontier"
    );
    {
        let api = b.state.certified.lock().await;
        let pw = api
            .pending_writes()
            .iter()
            .find(|p| p.key == "user/x")
            .expect("replicated write must be tracked on B");
        assert_eq!(
            pw.policy_version,
            PolicyVersion(1),
            "B must pin the replicated write to the ORIGIN version it was written under"
        );
        assert!(
            api.is_version_fenced(&user_range(), &PolicyVersion(1)),
            "B must fence the superseded v1 scope"
        );
        assert_ne!(
            api.get_certification_status("user/x"),
            CertificationStatus::Certified
        );
    }

    server_a.abort();
}

// ---------------------------------------------------------------
// RED-5: the lane must quiesce.
// ---------------------------------------------------------------

/// Once converged, repeated certified pulls must transfer nothing and
/// must not re-stamp anything: per-key HLCs stay byte-identical on both
/// nodes. A lane that re-stamps ingested entries with the local clock
/// never stops moving (each round makes the entry "new" for the peer).
///
/// The batch limit is exercised too: enough keys are written to force a
/// truncated first response, so the truncation frontier (the LAST
/// INCLUDED entry's HLC, never the sender's full frontier) is proven not
/// to skip the remainder.
#[tokio::test]
async fn certified_delta_pull_quiesces_after_convergence() {
    let a = build_node("quiesce-a", 1);
    let b = build_node("quiesce-b", 1);
    let (addr_a, server_a) = serve(router(Arc::clone(&a.state))).await;

    // More than one batch: the lane must need several rounds and must
    // still deliver every key.
    let total = asteroidb_poc::network::sync::CERTIFIED_DELTA_MAX_ENTRIES + 7;
    for i in 0..total {
        assert_eq!(
            certified_write(&a.state, &format!("user/k{i}"), 1).await,
            StatusCode::OK
        );
    }

    let mut runner_b = lane_runner("quiesce-b", &b, "quiesce-a", &addr_a.to_string()).await;
    for _ in 0..6 {
        runner_b.run_certified_sync().await;
    }

    let ts_a: std::collections::BTreeMap<String, HlcTimestamp> = {
        let api = a.state.certified.lock().await;
        api.store()
            .keys()
            .into_iter()
            .map(|k| (k.clone(), api.store().timestamp_for(k).unwrap().clone()))
            .collect()
    };
    let ts_b: std::collections::BTreeMap<String, HlcTimestamp> = {
        let api = b.state.certified.lock().await;
        api.store()
            .keys()
            .into_iter()
            .map(|k| (k.clone(), api.store().timestamp_for(k).unwrap().clone()))
            .collect()
    };
    assert_eq!(ts_a.len(), total, "A must hold every write");
    assert_eq!(
        ts_a, ts_b,
        "every key must have replicated with the WRITER's HLC, not a re-stamp"
    );

    // Three further rounds must be completely silent.
    let before = b
        .state
        .metrics
        .snapshot()
        .certified_sync_entries_applied_total;
    for _ in 0..3 {
        runner_b.run_certified_sync().await;
    }
    let after = b
        .state
        .metrics
        .snapshot()
        .certified_sync_entries_applied_total;
    assert_eq!(
        before, after,
        "a converged certified lane must apply zero entries per round"
    );

    let ts_b_after: std::collections::BTreeMap<String, HlcTimestamp> = {
        let api = b.state.certified.lock().await;
        api.store()
            .keys()
            .into_iter()
            .map(|k| (k.clone(), api.store().timestamp_for(k).unwrap().clone()))
            .collect()
    };
    assert_eq!(
        ts_b, ts_b_after,
        "quiesced lane must not move any timestamp"
    );

    server_a.abort();
}

// ---------------------------------------------------------------
// RED-6: rolling upgrades.
// ---------------------------------------------------------------

/// A peer that predates the route answers 404. That must be classified
/// as `Unsupported` (not a failure): no backoff penalty, no lane error,
/// and the eventual plane untouched.
#[tokio::test]
async fn certified_sync_unsupported_peer_does_not_fail_the_lane() {
    let b = build_node("rolling-b", 1);
    // An "old" node: a router with no certified delta route at all.
    let (addr_old, server_old) = serve(axum::Router::new()).await;

    let mut runner_b = lane_runner("rolling-b", &b, "rolling-old", &addr_old.to_string()).await;
    runner_b.run_certified_sync().await;

    let snap = b.state.metrics.snapshot();
    assert_eq!(
        snap.certified_sync_unsupported_total, 1,
        "a 404 from a pre-lane peer is Unsupported, not a failure"
    );
    assert_eq!(
        snap.certified_sync_failed_total, 0,
        "an unsupported peer must not be recorded as a lane failure"
    );

    // A second immediate round must not be blocked by a backoff penalty
    // (it is skipped by the unsupported cache instead, which is cheap and
    // silent).
    runner_b.run_certified_sync().await;
    assert_eq!(
        b.state.metrics.snapshot().certified_sync_failed_total,
        0,
        "still no failures after a retry"
    );

    server_old.abort();
}

// ---------------------------------------------------------------
// RED-7: plane isolation.
// ---------------------------------------------------------------

/// The certified lane must never touch the eventual store — not with the
/// replicated value, and not with a companion/sidecar key (`__certified_origin/...`
/// and friends). Plane isolation is what keeps `eventual.rs` at zero diff
/// and the delta/digest quiescence tests untouched.
#[tokio::test]
async fn certified_sync_never_writes_the_eventual_store() {
    let a = build_node("iso-a", 1);
    let b = build_node("iso-b", 1);
    let (addr_a, server_a) = serve(router(Arc::clone(&a.state))).await;

    assert_eq!(certified_write(&a.state, "user/x", 5).await, StatusCode::OK);

    let mut runner_b = lane_runner("iso-b", &b, "iso-a", &addr_a.to_string()).await;
    runner_b.run_certified_sync().await;

    {
        let api = b.state.certified.lock().await;
        assert_eq!(
            api.store().keys().len(),
            1,
            "exactly the replicated key — no companion keys"
        );
    }
    {
        let api = b.state.eventual.lock().await;
        assert_eq!(
            api.store().len(),
            0,
            "the certified lane must not write the eventual store"
        );
    }
    {
        let api = a.state.eventual.lock().await;
        assert_eq!(
            api.store().len(),
            0,
            "serving a certified delta must not write the eventual store either"
        );
    }

    server_a.abort();
}

// ---------------------------------------------------------------
// The kill switch, and the defect it re-exposes.
// ---------------------------------------------------------------

/// `certified_sync_enabled: false` must restore the pre-lane behaviour
/// exactly — which is the P0-1 defect itself: the certified write reaches
/// exactly one disk and a second node has no copy of it.
///
/// This is the inverse of `certified_value_survives_writer_loss`: together
/// they show the lane, and nothing else in the test setup, is what makes
/// the value survive.
#[tokio::test]
async fn certified_sync_kill_switch_restores_the_unreplicated_behaviour() {
    let a = build_node("kill-a", 1);
    let b = build_node("kill-b", 1);
    let (addr_a, server_a) = serve(router(Arc::clone(&a.state))).await;

    assert_eq!(certified_write(&a.state, "user/x", 7).await, StatusCode::OK);

    let mut runner_b = lane_runner_with("kill-b", &b, "kill-a", &addr_a.to_string(), false).await;
    runner_b.run_certified_sync().await;

    let (_, body) = certified_read(&b.state, "user/x").await;
    assert!(
        body["value"].is_null(),
        "with the lane disabled the value must not replicate (today's behaviour); got {body}"
    );
    assert_eq!(
        b.state.metrics.snapshot().certified_sync_attempt_total,
        0,
        "the kill switch must issue no requests at all, not merely discard the results"
    );

    server_a.abort();
}

// ---------------------------------------------------------------
// Refusal handling: a transiently refused entry must be RETRIED,
// never stepped over.
// ---------------------------------------------------------------

/// A node whose namespace lags the writer's refuses the entry with
/// `OriginAhead`. That refusal must NOT advance the peer baseline.
///
/// `Store::delta_entries_since` filters STRICTLY above the baseline and
/// the certified plane has no digest lane, so stepping over the entry is
/// permanent for the life of the process: the value stays on the writer's
/// disk alone, which is exactly the P0-1 inversion this lane exists to
/// close. Policy versions propagate through the control plane and are
/// applied per node, so a writer running ahead of a follower is ordinary
/// operation, not an exotic failure.
#[tokio::test]
async fn origin_ahead_refusal_is_retried_after_the_namespace_converges() {
    // A is already on v2; B still on v1.
    let a = build_node("node-a", 2);
    let b = build_node("node-b", 1);
    let (a_addr, a_server) = serve(router(Arc::clone(&a.state))).await;
    let mut runner_b = lane_runner("node-b", &b, "node-a", &a_addr.to_string()).await;

    assert_eq!(
        certified_write(&a.state, "user/k1", 7).await,
        StatusCode::OK
    );

    // B pulls while it still believes the scope is v1: the v2 origin is
    // ahead of anything it can validate, so the entry is refused.
    runner_b.run_certified_sync().await;
    let (_, body) = certified_read(&b.state, "user/k1").await;
    assert!(
        body["value"].is_null(),
        "an origin-ahead entry must not be ingested; got {body}"
    );
    assert_eq!(
        b.state
            .metrics
            .snapshot()
            .certified_sync_origin_ahead_rejected_total,
        1
    );
    assert_eq!(
        b.state
            .metrics
            .snapshot()
            .certified_sync_baseline_held_total,
        1,
        "the baseline must be HELD, not advanced past the refused entry"
    );

    // The control plane catches up. The very next pull must deliver the
    // entry the earlier round refused.
    bump_policy(&b.namespace, 2);
    runner_b.run_certified_sync().await;

    let (status, body) = certified_read(&b.state, "user/k1").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the refused entry must be re-offered and ingested once the \
         namespace converges"
    );
    assert_eq!(body["value"]["value"], 7);
    assert_eq!(
        b.state
            .metrics
            .snapshot()
            .certified_sync_entries_applied_total,
        1
    );

    a_server.abort();
}

/// The lane must not be able to serve a partially-merged CRDT.
///
/// `OrMap::delta_since` is a genuinely partial payload. If an entry is
/// ever missed (refused while the namespace lagged, or below a baseline
/// adopted from another peer) and a LATER update to the same key shipped
/// only its delta, the receiver would merge that delta onto a base it
/// never received and serve a map silently missing fields — with a valid
/// certification proof and no error anywhere. Entries therefore carry the
/// key's FULL state, so any later update repairs the hole.
#[tokio::test]
async fn a_replicated_entry_carries_full_state_not_a_partial_delta() {
    let a = build_node("node-a", 2);
    let b = build_node("node-b", 1);
    let (a_addr, a_server) = serve(router(Arc::clone(&a.state))).await;
    let mut runner_b = lane_runner("node-b", &b, "node-a", &a_addr.to_string()).await;

    // First contribution, refused by B (v1 vs v2) and then stepped over by
    // hand: this simulates any reason an entry never lands.
    assert_eq!(
        certified_write(&a.state, "user/k1", 1).await,
        StatusCode::OK
    );
    runner_b.run_certified_sync().await;
    let (_, body) = certified_read(&b.state, "user/k1").await;
    assert!(body["value"].is_null(), "not ingested yet; got {body}");

    // A second contribution to the SAME key, after B's namespace caught
    // up. B has no base for this key at all, so the entry must be
    // self-sufficient.
    bump_policy(&b.namespace, 2);
    assert_eq!(
        certified_write(&a.state, "user/k1", 5).await,
        StatusCode::OK
    );
    runner_b.run_certified_sync().await;

    let (status, body) = certified_read(&b.state, "user/k1").await;
    assert_eq!(status, StatusCode::OK);
    let (_, a_body) = certified_read(&a.state, "user/k1").await;
    assert_eq!(
        body["value"], a_body["value"],
        "the replica must hold exactly the writer's state, never a \
         fragment of it"
    );

    a_server.abort();
}
