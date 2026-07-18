//! Integration tests for digest-based stepwise-diff anti-entropy.
//!
//! Covers the `/api/internal/sync/digest` endpoint contract and the
//! NodeRunner integration: root-match zero transfer, mismatched-bucket
//! partial transfer, full convergence on massive difference, fallback to
//! legacy full sync against digest-unsupported peers, and session-claim
//! adoption soundness.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::Ordering;
use std::time::Duration;

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::network::sync::{DigestSyncRequest, DigestSyncResponse, SyncClient};
use asteroidb_poc::network::{PeerConfig, PeerRegistry};
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::runtime::{NodeRunner, NodeRunnerConfig};
use asteroidb_poc::store::digest::{DIGEST_SCHEME_VERSION, bucket_of, compute_store_digest};
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{KeyRange, NodeId};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::Mutex;
use tower::ServiceExt;

fn node_id(s: &str) -> NodeId {
    NodeId(s.into())
}

fn hlc(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
    HlcTimestamp {
        physical,
        logical,
        node_id: node.into(),
    }
}

fn test_state(name: &str) -> Arc<AppState> {
    test_state_with_token(name, None)
}

fn test_state_with_token(name: &str, token: Option<String>) -> Arc<AppState> {
    let nid = node_id(name);

    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: String::new(),
        },
        authority_nodes: vec![node_id("auth-1")],
        auto_generated: false,
    });
    let namespace = Arc::new(RwLock::new(ns));

    Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(nid.clone()))),
        certified: Arc::new(Mutex::new(CertifiedApi::new(nid, Arc::clone(&namespace)))),
        namespace,
        metrics: Arc::new(RuntimeMetrics::default()),
        peers: None,
        peer_persist_path: None,
        namespace_persist_path: None,
        consensus: Arc::new(Mutex::new(ControlPlaneConsensus::new(vec![]))),
        internal_token: token,
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
    })
}

/// Snapshot a state's store data as a sorted map (digest input).
async fn snapshot_data(state: &Arc<AppState>) -> BTreeMap<String, CrdtValue> {
    snapshot_data_api(&state.eventual).await
}

/// Like [`snapshot_data`] but for a bare `EventualApi`.
async fn snapshot_data_api(api: &Arc<Mutex<EventualApi>>) -> BTreeMap<String, CrdtValue> {
    let api = api.lock().await;
    api.store()
        .all_entries()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Copy the full CRDT state of `src` into `dst`, preserving per-key HLCs
/// (the same operation a full-sync import performs). Afterwards the two
/// stores hold identical CRDT states — and identical digests.
async fn mirror_state(src: &Arc<AppState>, dst: &Arc<Mutex<EventualApi>>) {
    mirror_api(&src.eventual, dst).await;
}

/// Like [`mirror_state`] but between two bare `EventualApi`s.
async fn mirror_api(src: &Arc<Mutex<EventualApi>>, dst: &Arc<Mutex<EventualApi>>) {
    let (entries, timestamps) = {
        let api = src.lock().await;
        let store = api.store();
        let mut entries = Vec::new();
        let mut timestamps = std::collections::HashMap::new();
        for (k, v) in store.all_entries() {
            entries.push((k.clone(), v.clone()));
            if let Some(ts) = store.timestamp_for(k) {
                timestamps.insert(k.clone(), ts.clone());
            }
        }
        (entries, timestamps)
    };
    let mut api = dst.lock().await;
    for (key, value) in entries {
        match timestamps.get(&key) {
            Some(ts) => api.merge_remote_with_hlc(key, &value, ts.clone()).unwrap(),
            None => api.merge_remote(key, &value).unwrap(),
        }
    }
}

async fn post_digest_request(
    app: &axum::Router,
    req: &DigestSyncRequest,
) -> (StatusCode, Option<DigestSyncResponse>) {
    let body = serde_json::to_string(req).unwrap();
    let http_req = Request::builder()
        .method("POST")
        .uri("/api/internal/sync/digest")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(http_req).await.unwrap();
    let status = resp.status();
    if status != StatusCode::OK {
        return (status, None);
    }
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let decoded: DigestSyncResponse = serde_json::from_slice(&bytes).unwrap();
    (status, Some(decoded))
}

// ===========================================================================
// Endpoint contract
// ===========================================================================

/// (a) Identical states → root digest match, zero entries transferred,
/// session claims + frontier still included in the response.
#[tokio::test]
async fn digest_endpoint_root_match_returns_zero_entries_with_claims() {
    let server = test_state("server");
    {
        let mut api = server.eventual.lock().await;
        api.eventual_counter_inc("hits").unwrap();
        api.eventual_set_add("users", "alice".into()).unwrap();
        api.eventual_register_set("status", "online".into())
            .unwrap();
    }
    let requester = test_state("requester");
    mirror_state(&server, &requester.eventual).await;

    let digest = compute_store_digest(&snapshot_data(&requester).await);
    let req = DigestSyncRequest::from_digest("requester", &digest, true);

    let app = router(server.clone());
    let (status, resp) = post_digest_request(&app, &req).await;
    assert_eq!(status, StatusCode::OK);
    let resp = resp.unwrap();

    assert!(resp.scheme_ok);
    assert!(resp.root_matched, "identical states must match at the root");
    assert!(
        resp.entries.is_empty(),
        "root match must transfer zero keys"
    );
    assert!(resp.mismatched_buckets.is_empty());
    assert_eq!(resp.total_keys, 3);
    // Claims and frontier are still present: the receiver adopts them —
    // the transfer is complete (trivially: nothing differs).
    assert!(resp.frontier.is_some());
    assert!(
        resp.applied_origins.contains_key("server"),
        "server's own writes must appear in applied_origins"
    );
}

/// (b) Partial difference → only keys in mismatched buckets are returned;
/// keys in matched buckets are not re-transferred.
#[tokio::test]
async fn digest_endpoint_partial_mismatch_returns_only_mismatched_buckets() {
    let server = test_state("server");
    {
        let mut api = server.eventual.lock().await;
        for i in 0..20 {
            api.eventual_counter_inc(&format!("key-{i}")).unwrap();
        }
    }
    let requester = test_state("requester");
    mirror_state(&server, &requester.eventual).await;

    // Server gains one more write after the mirror: exactly one key
    // ("key-0") now differs between the stores.
    {
        let mut api = server.eventual.lock().await;
        api.eventual_counter_inc("key-0").unwrap();
    }

    let digest = compute_store_digest(&snapshot_data(&requester).await);
    let req = DigestSyncRequest::from_digest("requester", &digest, true);

    let app = router(server.clone());
    let (_, resp) = post_digest_request(&app, &req).await;
    let resp = resp.unwrap();

    assert!(resp.scheme_ok);
    assert!(!resp.root_matched);
    let changed_bucket = bucket_of("key-0") as u16;
    assert!(
        resp.mismatched_buckets.contains(&changed_bucket),
        "the changed key's bucket must be mismatched"
    );
    assert!(
        resp.entries.contains_key("key-0"),
        "changed key transferred"
    );
    // Every returned key must live in a mismatched bucket — matched
    // buckets are never re-transferred.
    for key in resp.entries.keys() {
        assert!(
            resp.mismatched_buckets.contains(&(bucket_of(key) as u16)),
            "key {key} returned from a matched bucket"
        );
    }
    // With 20 keys spread over 256 buckets, at least one other key lives
    // in a matched bucket and must be absent from the transfer.
    assert!(
        resp.entries.len() < 20,
        "matched buckets must be skipped (got {} keys)",
        resp.entries.len()
    );
    assert_eq!(resp.total_keys, 20);
    // Timestamps ride along for the transferred keys.
    for key in resp.entries.keys() {
        assert!(resp.timestamps.contains_key(key), "missing HLC for {key}");
    }
}

/// A push probe (`include_entries = false`) reports mismatched buckets
/// without shipping any entries.
#[tokio::test]
async fn digest_endpoint_probe_returns_no_entries() {
    let server = test_state("server");
    {
        let mut api = server.eventual.lock().await;
        api.eventual_counter_inc("only-on-server").unwrap();
    }

    let empty_digest = compute_store_digest(&BTreeMap::new());
    let req = DigestSyncRequest::from_digest("requester", &empty_digest, false);

    let app = router(server.clone());
    let (_, resp) = post_digest_request(&app, &req).await;
    let resp = resp.unwrap();

    assert!(resp.scheme_ok);
    assert!(!resp.root_matched);
    assert!(!resp.mismatched_buckets.is_empty());
    assert!(resp.entries.is_empty(), "probe must not ship entries");
}

/// Scheme-version mismatch → HTTP 200 with `scheme_ok = false` (the
/// requester falls back to legacy full sync; rolling-upgrade safe).
#[tokio::test]
async fn digest_endpoint_rejects_unknown_scheme_version() {
    let server = test_state("server");
    let digest = compute_store_digest(&BTreeMap::new());
    let mut req = DigestSyncRequest::from_digest("requester", &digest, true);
    req.scheme_version = DIGEST_SCHEME_VERSION + 1;

    let app = router(server);
    let (status, resp) = post_digest_request(&app, &req).await;
    assert_eq!(status, StatusCode::OK);
    let resp = resp.unwrap();
    assert!(!resp.scheme_ok);
    assert!(!resp.root_matched);
    assert!(resp.entries.is_empty());
}

/// Malformed digests (wrong length / out-of-range bucket index) are also
/// answered with `scheme_ok = false` rather than an error.
#[tokio::test]
async fn digest_endpoint_rejects_malformed_digests() {
    let server = test_state("server");
    let app = router(server);

    let digest = compute_store_digest(&BTreeMap::new());
    let mut req = DigestSyncRequest::from_digest("requester", &digest, true);
    req.root = vec![0u8; 16]; // wrong length
    let (_, resp) = post_digest_request(&app, &req).await;
    assert!(!resp.unwrap().scheme_ok);

    let mut req = DigestSyncRequest::from_digest("requester", &digest, true);
    req.buckets = vec![asteroidb_poc::network::sync::BucketDigestEntry {
        index: 300, // out of range
        digest: vec![0u8; 32],
    }];
    let (_, resp) = post_digest_request(&app, &req).await;
    assert!(!resp.unwrap().scheme_ok);
}

/// The digest route lives in the internal sub-router: with an internal
/// token configured, unauthenticated requests are rejected.
#[tokio::test]
async fn digest_endpoint_requires_bearer_token() {
    let server = test_state_with_token("server", Some("secret-token".into()));
    let app = router(server);

    let digest = compute_store_digest(&BTreeMap::new());
    let req = DigestSyncRequest::from_digest("requester", &digest, true);
    let (status, _) = post_digest_request(&app, &req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// The endpoint honours the bincode Accept/Content-Type negotiation used
/// by all internal endpoints.
#[tokio::test]
async fn digest_endpoint_speaks_bincode() {
    let server = test_state("server");
    {
        let mut api = server.eventual.lock().await;
        api.eventual_counter_inc("hits").unwrap();
    }
    let app = router(server);

    let digest = compute_store_digest(&BTreeMap::new());
    let req = DigestSyncRequest::from_digest("requester", &digest, true);
    let body = bincode::serde::encode_to_vec(&req, bincode::config::standard()).unwrap();

    let http_req = Request::builder()
        .method("POST")
        .uri("/api/internal/sync/digest")
        .header("content-type", "application/octet-stream")
        .header("accept", "application/octet-stream")
        .body(Body::from(body))
        .unwrap();
    let resp = app.oneshot(http_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/octet-stream"
    );
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let (decoded, _): (DigestSyncResponse, _) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
    assert!(decoded.scheme_ok);
    assert!(!decoded.root_matched);
    assert!(decoded.entries.contains_key("hits"));
}

/// Removes propagate through the digest path: two stores with the same
/// visible elements but different tombstone (deferred) sets mismatch, and
/// merging the transferred state applies the remove.
#[tokio::test]
async fn digest_sync_propagates_tombstoned_remove() {
    let server = test_state("server");
    {
        let mut api = server.eventual.lock().await;
        api.eventual_set_add("team", "alice".into()).unwrap();
        api.eventual_set_add("team", "bob".into()).unwrap();
    }
    let requester = test_state("requester");
    mirror_state(&server, &requester.eventual).await;

    // Server removes "bob" — the states now differ by the tombstone and
    // the removed element.
    {
        let mut api = server.eventual.lock().await;
        api.eventual_set_remove("team", "bob").unwrap();
    }

    let digest = compute_store_digest(&snapshot_data(&requester).await);
    let req = DigestSyncRequest::from_digest("requester", &digest, true);
    let app = router(server.clone());
    let (_, resp) = post_digest_request(&app, &req).await;
    let resp = resp.unwrap();
    assert!(!resp.root_matched, "tombstone difference must mismatch");
    assert!(resp.entries.contains_key("team"));

    // Apply like the sync loop does: the remove must propagate.
    {
        let mut api = requester.eventual.lock().await;
        for (key, value) in &resp.entries {
            match resp.timestamps.get(key) {
                Some(ts) => api
                    .merge_remote_with_hlc(key.clone(), value, ts.clone())
                    .unwrap(),
                None => api.merge_remote(key.clone(), value).unwrap(),
            }
        }
    }
    let api = requester.eventual.lock().await;
    match api.get_eventual("team") {
        Some(CrdtValue::Set(s)) => {
            assert!(s.contains(&"alice".to_string()));
            assert!(!s.contains(&"bob".to_string()), "remove must propagate");
        }
        other => panic!("expected Set, got {other:?}"),
    }
}

// ===========================================================================
// NodeRunner end-to-end
// ===========================================================================

struct RunnerHarness {
    runner: NodeRunner,
    metrics: Arc<RuntimeMetrics>,
    local_api: Arc<Mutex<EventualApi>>,
}

/// Build a NodeRunner ("local") whose only peer is `peer_addr`.
async fn runner_against(peer_addr: &str, digest_sync_enabled: bool) -> RunnerHarness {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: String::new(),
        },
        authority_nodes: vec![node_id("auth-1")],
        auto_generated: false,
    });
    let ns = Arc::new(RwLock::new(ns));
    let local_api = Arc::new(Mutex::new(EventualApi::new(node_id("local"))));
    let certified_api = Arc::new(Mutex::new(CertifiedApi::new(node_id("local"), ns)));

    let peer_registry = PeerRegistry::new(
        node_id("local"),
        vec![PeerConfig {
            node_id: node_id("server"),
            addr: peer_addr.to_string(),
        }],
    )
    .unwrap();
    let sync_client = SyncClient::new(Arc::new(Mutex::new(peer_registry)));

    let config = NodeRunnerConfig {
        certification_interval: Duration::from_millis(10),
        cleanup_interval: Duration::from_secs(60),
        compaction_check_interval: Duration::from_secs(60),
        frontier_report_interval: Duration::from_millis(10),
        sync_interval: Some(Duration::from_millis(20)),
        ping_interval: None,
        digest_sync_enabled,
        ..NodeRunnerConfig::default()
    };

    let metrics = Arc::new(RuntimeMetrics::default());
    let runner = NodeRunner::with_sync(
        node_id("local"),
        certified_api,
        CompactionEngine::with_defaults(),
        config,
        sync_client,
        Arc::clone(&local_api),
        metrics.clone(),
    )
    .await;

    RunnerHarness {
        runner,
        metrics,
        local_api,
    }
}

/// Spawn an HTTP listener for `app`, returning its address and handle.
async fn serve(app: axum::Router) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    // Give the listener a moment to start accepting.
    tokio::time::sleep(Duration::from_millis(20)).await;
    (addr, handle)
}

/// Router with the pre-digest sync surface MINUS the delta endpoint, so
/// every pull lands on the fallback path where digest sync runs.
fn no_delta_router(state: Arc<AppState>) -> axum::Router {
    use asteroidb_poc::http::handlers::{internal_digest_sync, internal_keys, internal_sync};
    use axum::routing::{get, post};
    axum::Router::new()
        .route("/api/internal/keys", get(internal_keys))
        .route("/api/internal/sync", post(internal_sync))
        .route("/api/internal/sync/digest", post(internal_digest_sync))
        .with_state(state)
}

/// Legacy router: keys + sync only — digest AND delta answer 404.
fn legacy_router(state: Arc<AppState>) -> axum::Router {
    use asteroidb_poc::http::handlers::{internal_keys, internal_sync};
    use axum::routing::{get, post};
    axum::Router::new()
        .route("/api/internal/keys", get(internal_keys))
        .route("/api/internal/sync", post(internal_sync))
        .with_state(state)
}

/// Router of a version-mismatched peer: the digest route exists but
/// always answers `scheme_ok = false`; keys + sync are the real
/// handlers (no delta route, so every pull reaches the fallback path).
fn scheme_mismatch_router(state: Arc<AppState>) -> axum::Router {
    use asteroidb_poc::http::handlers::{internal_keys, internal_sync};
    use axum::routing::{get, post};
    axum::Router::new()
        .route("/api/internal/keys", get(internal_keys))
        .route("/api/internal/sync", post(internal_sync))
        .route(
            "/api/internal/sync/digest",
            post(|| async { axum::Json(DigestSyncResponse::default()) }),
        )
        .with_state(state)
}

/// Router of a digest-capable peer that is transiently unhealthy: the
/// digest route always answers 503; keys + sync are the real handlers
/// (no delta route, so every pull reaches the fallback path).
fn transient_digest_failure_router(state: Arc<AppState>) -> axum::Router {
    use asteroidb_poc::http::handlers::{internal_keys, internal_sync};
    use axum::routing::{get, post};
    axum::Router::new()
        .route("/api/internal/keys", get(internal_keys))
        .route("/api/internal/sync", post(internal_sync))
        .route(
            "/api/internal/sync/digest",
            post(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        )
        .with_state(state)
}

/// Run the runner until `check` returns true (panics on a 5s timeout),
/// then shut it down. Waiter panics propagate to the test.
async fn run_until<F, Fut>(mut runner: NodeRunner, check: F)
where
    F: Fn() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = bool> + Send,
{
    let shutdown = runner.shutdown_handle();
    let waiter = tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if check().await {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                let _ = shutdown.send(true);
                panic!("timed out waiting for sync condition");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let _ = shutdown.send(true);
    });
    runner.run().await;
    waiter.await.expect("sync condition waiter failed");
}

/// (a) + (e) Identical states: the digest fallback completes with a root
/// match — zero keys transferred — while still adopting the sender's
/// session claims.
#[tokio::test]
async fn runner_digest_root_match_syncs_with_zero_transfer() {
    let server = test_state("server");
    {
        let mut api = server.eventual.lock().await;
        for i in 0..10 {
            api.eventual_counter_inc(&format!("key-{i}")).unwrap();
        }
        api.eventual_set_add("users", "alice".into()).unwrap();
    }

    let (addr, server_handle) = serve(no_delta_router(server.clone())).await;

    let mut harness = runner_against(&addr.to_string(), true).await;
    // The local node already holds the identical state.
    mirror_state(&server, &harness.local_api).await;

    // Stale frontier forces the delta attempt (404 → fallback → digest).
    harness
        .runner
        .inject_peer_frontier(&addr.to_string(), hlc(1, 0, "stale"));

    let metrics = harness.metrics.clone();
    let metrics_check = metrics.clone();
    run_until(harness.runner, move || {
        let m = metrics_check.clone();
        async move { m.digest_sync_root_match_total.load(Ordering::Relaxed) > 0 }
    })
    .await;
    server_handle.abort();

    assert!(
        metrics.digest_sync_attempt_total.load(Ordering::Relaxed) > 0,
        "digest sync must have been attempted"
    );
    assert_eq!(
        metrics
            .digest_sync_keys_transferred_total
            .load(Ordering::Relaxed),
        0,
        "root match must transfer zero keys"
    );
    assert!(
        metrics
            .digest_sync_keys_skipped_total
            .load(Ordering::Relaxed)
            >= 11,
        "all keys must be counted as skipped"
    );

    // (e) Session claims were adopted from the sender — full-dump
    // equivalent coverage; the claimed origin is exactly the sender's
    // transmitted applied_origins (never per-entry overclaims).
    let api = harness.local_api.lock().await;
    assert!(
        api.store().applied_origins().contains_key("server"),
        "digest sync must adopt the sender's applied_origins"
    );
}

/// (b) Partial difference: only mismatched buckets are transferred and
/// the stores converge completely.
#[tokio::test]
async fn runner_digest_partial_transfer_converges() {
    let server = test_state("server");
    {
        let mut api = server.eventual.lock().await;
        for i in 0..15 {
            api.eventual_counter_inc(&format!("shared-{i}")).unwrap();
        }
    }

    let (addr, server_handle) = serve(no_delta_router(server.clone())).await;
    let mut harness = runner_against(&addr.to_string(), true).await;

    // The local store mirrors the first 15 keys …
    mirror_state(&server, &harness.local_api).await;
    // … then the server gains 5 keys the local node has never seen.
    {
        let mut api = server.eventual.lock().await;
        for i in 0..5 {
            api.eventual_register_set(&format!("extra-{i}"), format!("v{i}"))
                .unwrap();
        }
    }

    harness
        .runner
        .inject_peer_frontier(&addr.to_string(), hlc(1, 0, "stale"));

    let metrics = harness.metrics.clone();
    let local_api = harness.local_api.clone();
    let local_check = local_api.clone();
    run_until(harness.runner, move || {
        let api = local_check.clone();
        async move {
            let api = api.lock().await;
            (0..5).all(|i| api.get_eventual(&format!("extra-{i}")).is_some())
        }
    })
    .await;
    server_handle.abort();

    // Converged: all 20 keys present and correct.
    {
        let api = local_api.lock().await;
        for i in 0..15 {
            match api.get_eventual(&format!("shared-{i}")) {
                Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
                other => panic!("shared-{i}: expected Counter, got {other:?}"),
            }
        }
        for i in 0..5 {
            match api.get_eventual(&format!("extra-{i}")) {
                Some(CrdtValue::Register(r)) => assert_eq!(r.get(), Some(&format!("v{i}"))),
                other => panic!("extra-{i}: expected Register, got {other:?}"),
            }
        }
        // (e) claims adopted.
        assert!(api.store().applied_origins().contains_key("server"));
    }

    // Only mismatched buckets were transferred: with 5 changed keys out
    // of 20 across 256 buckets, at least some of the 15 shared keys must
    // have been skipped.
    assert!(
        metrics.digest_sync_partial_total.load(Ordering::Relaxed) > 0,
        "digest sync must have taken the partial-transfer path"
    );
    assert!(
        metrics
            .digest_sync_keys_skipped_total
            .load(Ordering::Relaxed)
            > 0,
        "matched buckets must have been skipped"
    );
}

/// (c) Massive difference (receiver empty, everything differs): the
/// digest path still converges completely — correctness, not bandwidth,
/// is the point of this test.
#[tokio::test]
async fn runner_digest_massive_difference_converges() {
    let server = test_state("server");
    {
        let mut api = server.eventual.lock().await;
        for i in 0..50 {
            api.eventual_counter_inc(&format!("bulk-{i}")).unwrap();
            api.eventual_counter_inc(&format!("bulk-{i}")).unwrap();
        }
    }

    let (addr, server_handle) = serve(no_delta_router(server.clone())).await;
    let mut harness = runner_against(&addr.to_string(), true).await;
    harness
        .runner
        .inject_peer_frontier(&addr.to_string(), hlc(1, 0, "stale"));

    let metrics = harness.metrics.clone();
    let local_api = harness.local_api.clone();
    let local_check = local_api.clone();
    run_until(harness.runner, move || {
        let api = local_check.clone();
        async move {
            let api = api.lock().await;
            (0..50).all(|i| api.get_eventual(&format!("bulk-{i}")).is_some())
        }
    })
    .await;
    server_handle.abort();

    let api = local_api.lock().await;
    for i in 0..50 {
        match api.get_eventual(&format!("bulk-{i}")) {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2, "bulk-{i}"),
            other => panic!("bulk-{i}: expected Counter, got {other:?}"),
        }
    }
    assert!(metrics.digest_sync_partial_total.load(Ordering::Relaxed) > 0);
}

/// (d) Rolling-upgrade safety: a legacy peer without the digest route
/// (404) falls back to the legacy full key dump and still converges; the
/// peer is cached as digest-unsupported.
#[tokio::test]
async fn runner_falls_back_to_full_sync_against_legacy_peer() {
    let server = test_state("server");
    {
        let mut api = server.eventual.lock().await;
        api.eventual_counter_inc("legacy-key").unwrap();
        api.eventual_set_add("legacy-set", "v".into()).unwrap();
    }

    let (addr, server_handle) = serve(legacy_router(server.clone())).await;
    let mut harness = runner_against(&addr.to_string(), true).await;
    harness
        .runner
        .inject_peer_frontier(&addr.to_string(), hlc(1, 0, "stale"));

    let metrics = harness.metrics.clone();
    let local_api = harness.local_api.clone();
    let local_check = local_api.clone();
    run_until(harness.runner, move || {
        let api = local_check.clone();
        async move {
            let api = api.lock().await;
            api.get_eventual("legacy-key").is_some() && api.get_eventual("legacy-set").is_some()
        }
    })
    .await;
    server_handle.abort();

    assert!(
        metrics
            .digest_sync_unsupported_total
            .load(Ordering::Relaxed)
            > 0,
        "the 404 must be recorded as digest-unsupported"
    );
    assert_eq!(
        metrics.digest_sync_root_match_total.load(Ordering::Relaxed)
            + metrics.digest_sync_partial_total.load(Ordering::Relaxed),
        0,
        "no digest sync can succeed against a legacy peer"
    );

    let api = local_api.lock().await;
    match api.get_eventual("legacy-key") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
        other => panic!("expected Counter, got {other:?}"),
    }
}

/// Kill switch: with `digest_sync_enabled = false` the runner never
/// probes the digest endpoint and behaves exactly like the legacy code.
#[tokio::test]
async fn runner_kill_switch_disables_digest_sync() {
    let server = test_state("server");
    {
        let mut api = server.eventual.lock().await;
        api.eventual_counter_inc("k").unwrap();
    }

    // No delta route: the pull always lands on the fallback path, which
    // must go straight to the legacy full sync when the switch is off.
    let (addr, server_handle) = serve(no_delta_router(server.clone())).await;
    let mut harness = runner_against(&addr.to_string(), false).await;
    harness
        .runner
        .inject_peer_frontier(&addr.to_string(), hlc(1, 0, "stale"));

    let metrics = harness.metrics.clone();
    let local_api = harness.local_api.clone();
    let local_check = local_api.clone();
    run_until(harness.runner, move || {
        let api = local_check.clone();
        async move {
            let api = api.lock().await;
            api.get_eventual("k").is_some()
        }
    })
    .await;
    server_handle.abort();

    assert_eq!(
        metrics.digest_sync_attempt_total.load(Ordering::Relaxed),
        0,
        "kill switch must suppress all digest probes"
    );
    assert_eq!(metrics.digest_push_probe_total.load(Ordering::Relaxed), 0);
}

/// Prune-induced fallback (the target production scenario): the sender
/// pruned its change log, so delta pulls merge without claims
/// (claims_ok = false) and previously always escalated to a full dump.
/// The digest path now absorbs that fallback, converges, and
/// re-establishes verified coverage (claims adopted).
#[tokio::test]
async fn runner_prune_induced_fallback_uses_digest_and_restores_claims() {
    let server = test_state("server");
    {
        let mut api = server.eventual.lock().await;
        for i in 0..10 {
            api.eventual_counter_inc(&format!("pruned-{i}")).unwrap();
        }
        // Prune the whole change log: subsequent deltas cannot prove
        // completeness (pruned_floor above any zero request frontier).
        let frontier = api.store().current_frontier().unwrap();
        api.store_mut().prune_timestamps_before("", &frontier);
        // One fresh write after the prune keeps a live frontier.
        api.eventual_counter_inc("fresh").unwrap();
    }

    // Full router: delta IS available, but its pruned_floor forces
    // claims_ok = false, which routes into the digest fallback.
    let (addr, server_handle) = serve(router(server.clone())).await;
    let harness = runner_against(&addr.to_string(), true).await;

    let metrics = harness.metrics.clone();
    let local_api = harness.local_api.clone();
    let local_check = local_api.clone();
    let metrics_probe = metrics.clone();
    run_until(harness.runner, move || {
        let api = local_check.clone();
        let m = metrics_probe.clone();
        async move {
            let api = api.lock().await;
            let converged = (0..10).all(|i| api.get_eventual(&format!("pruned-{i}")).is_some())
                && api.get_eventual("fresh").is_some();
            // Verified coverage restored: the sender's claims adopted.
            let claimed = api.store().applied_origins().contains_key("server");
            converged && claimed && m.digest_sync_attempt_total.load(Ordering::Relaxed) > 0
        }
    })
    .await;
    server_handle.abort();

    assert!(
        metrics.digest_sync_attempt_total.load(Ordering::Relaxed) > 0,
        "prune-induced fallback must route through digest sync"
    );
    let api = local_api.lock().await;
    for i in 0..10 {
        match api.get_eventual(&format!("pruned-{i}")) {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("pruned-{i}: expected Counter, got {other:?}"),
        }
    }
}

/// Push side: when the change rate exceeds the full-sync threshold but
/// the peer already holds an identical state, the digest probe skips the
/// full-state push entirely.
#[tokio::test]
async fn runner_digest_push_probe_skips_full_push_on_match() {
    let server = test_state("server");
    let (addr, server_handle) = serve(router(server.clone())).await;

    let mut harness = runner_against(&addr.to_string(), true).await;
    // Local node has data; the server mirrors it exactly.
    {
        let mut api = harness.local_api.lock().await;
        for i in 0..10 {
            api.eventual_counter_inc(&format!("k-{i}")).unwrap();
        }
    }
    mirror_api(&harness.local_api, &server.eventual).await;

    // Zero frontier: every local key counts as changed → change rate 1.0
    // → the push phase lands on the high-change-rate full-sync branch.
    harness
        .runner
        .inject_peer_frontier(&addr.to_string(), hlc(0, 0, ""));

    let metrics = harness.metrics.clone();
    let metrics_check = metrics.clone();
    run_until(harness.runner, move || {
        let m = metrics_check.clone();
        async move { m.digest_push_match_total.load(Ordering::Relaxed) > 0 }
    })
    .await;
    server_handle.abort();

    assert!(
        metrics.digest_push_probe_total.load(Ordering::Relaxed) > 0,
        "high change rate must trigger a digest push probe"
    );
    assert_eq!(
        metrics
            .digest_push_keys_pushed_total
            .load(Ordering::Relaxed),
        0,
        "no keys may be pushed after a probe match"
    );
    assert_eq!(
        metrics.full_sync_fallback_count.load(Ordering::Relaxed),
        0,
        "the full-state push must have been skipped"
    );
}

/// Client-side status classification: only 404/405 prove the digest
/// route is absent (→ `Unsupported`, cacheable); transient statuses a
/// digest-capable peer can emit (503 under load, 500, 429, 401 during
/// token rotation) must map to `Failed` so the runner does not cache
/// the peer as digest-unsupported for 10 minutes.
#[tokio::test]
async fn digest_sync_client_distinguishes_missing_route_from_transient_errors() {
    use asteroidb_poc::network::sync::DigestSyncResult;
    use axum::routing::post;

    let digest = compute_store_digest(&BTreeMap::new());
    let req = DigestSyncRequest::from_digest("requester", &digest, true);

    let registry = PeerRegistry::new(
        node_id("local"),
        vec![PeerConfig {
            node_id: node_id("server"),
            addr: "127.0.0.1:1".to_string(),
        }],
    )
    .unwrap();
    let client = SyncClient::new(Arc::new(Mutex::new(registry)));

    // 404: the route does not exist (legacy node) → Unsupported.
    let (addr, handle) = serve(axum::Router::new()).await;
    let result = client.digest_sync(&addr.to_string(), &req).await;
    assert!(
        matches!(result, DigestSyncResult::Unsupported),
        "404 must classify as Unsupported, got {result:?}"
    );
    handle.abort();

    // Transient statuses → Failed (never Unsupported).
    for status in [
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::UNAUTHORIZED,
    ] {
        let app = axum::Router::new().route(
            "/api/internal/sync/digest",
            post(move || async move { status }),
        );
        let (addr, handle) = serve(app).await;
        let result = client.digest_sync(&addr.to_string(), &req).await;
        assert!(
            matches!(result, DigestSyncResult::Failed),
            "{status} must classify as Failed (transient), got {result:?}"
        );
        handle.abort();
    }
}

/// Push side, mismatch: only the local keys living in mismatched buckets
/// are pushed (matched buckets are skipped), the peer converges, and the
/// legacy full-state push never runs.
#[tokio::test]
async fn runner_digest_push_subset_pushes_only_mismatched_buckets() {
    let server = test_state("server");
    let (addr, server_handle) = serve(router(server.clone())).await;

    let mut harness = runner_against(&addr.to_string(), true).await;
    // 15 shared keys mirrored on the server …
    {
        let mut api = harness.local_api.lock().await;
        for i in 0..15 {
            api.eventual_counter_inc(&format!("shared-{i}")).unwrap();
        }
    }
    mirror_api(&harness.local_api, &server.eventual).await;
    // … plus 5 local-only keys the server has never seen.
    {
        let mut api = harness.local_api.lock().await;
        for i in 0..5 {
            api.eventual_register_set(&format!("localonly-{i}"), format!("v{i}"))
                .unwrap();
        }
    }

    // Exactly the buckets holding a local-only key mismatch, so the
    // subset push must ship every local key in those buckets — no more.
    let expected_pushed: u64 = {
        let mismatched: std::collections::HashSet<usize> = (0..5)
            .map(|i| bucket_of(&format!("localonly-{i}")))
            .collect();
        snapshot_data_api(&harness.local_api)
            .await
            .keys()
            .filter(|k| mismatched.contains(&bucket_of(k)))
            .count() as u64
    };
    assert!(
        (5..20).contains(&expected_pushed),
        "test setup: some buckets must match (expected {expected_pushed})"
    );

    // Zero frontier: change rate 1.0 → push phase lands on the
    // high-change-rate full-sync branch → digest probe → subset push.
    harness
        .runner
        .inject_peer_frontier(&addr.to_string(), hlc(0, 0, ""));

    let metrics = harness.metrics.clone();
    let metrics_check = metrics.clone();
    let server_check = server.clone();
    run_until(harness.runner, move || {
        let m = metrics_check.clone();
        let srv = server_check.clone();
        async move {
            let api = srv.eventual.lock().await;
            m.digest_push_keys_pushed_total.load(Ordering::Relaxed) > 0
                && (0..5).all(|i| api.get_eventual(&format!("localonly-{i}")).is_some())
        }
    })
    .await;
    server_handle.abort();

    assert_eq!(
        metrics
            .digest_push_keys_pushed_total
            .load(Ordering::Relaxed),
        expected_pushed,
        "exactly the keys in mismatched buckets must be pushed"
    );
    assert_eq!(
        metrics.full_sync_fallback_count.load(Ordering::Relaxed),
        0,
        "the subset push must replace the legacy full-state push"
    );

    // The peer converged on the pushed keys.
    let api = server.eventual.lock().await;
    for i in 0..5 {
        match api.get_eventual(&format!("localonly-{i}")) {
            Some(CrdtValue::Register(r)) => assert_eq!(r.get(), Some(&format!("v{i}"))),
            other => panic!("localonly-{i}: expected Register, got {other:?}"),
        }
    }
}

/// Push side, peer-only mismatch: every mismatched bucket is empty
/// locally (the peer holds data we lack), so nothing is pushed, no full
/// push runs, and the pull phase fetches the peer-only data.
#[tokio::test]
async fn runner_digest_push_peer_only_mismatch_pushes_nothing() {
    let server = test_state("server");
    let (addr, server_handle) = serve(router(server.clone())).await;

    let mut harness = runner_against(&addr.to_string(), true).await;
    // Local keys, mirrored exactly on the server.
    {
        let mut api = harness.local_api.lock().await;
        for i in 0..10 {
            api.eventual_counter_inc(&format!("k-{i}")).unwrap();
        }
    }
    mirror_api(&harness.local_api, &server.eventual).await;

    // Server-only keys in buckets holding NO local key: the mismatched
    // buckets are then empty on the local side.
    let local_buckets: std::collections::HashSet<usize> = snapshot_data_api(&harness.local_api)
        .await
        .keys()
        .map(|k| bucket_of(k))
        .collect();
    let mut server_only = Vec::new();
    let mut i = 0;
    while server_only.len() < 3 {
        let name = format!("srv-{i}");
        if !local_buckets.contains(&bucket_of(&name)) {
            server_only.push(name);
        }
        i += 1;
    }
    {
        let mut api = server.eventual.lock().await;
        for name in &server_only {
            api.eventual_register_set(name, "server-data".into())
                .unwrap();
        }
    }

    harness
        .runner
        .inject_peer_frontier(&addr.to_string(), hlc(0, 0, ""));

    let metrics = harness.metrics.clone();
    let metrics_check = metrics.clone();
    let local_check = harness.local_api.clone();
    let names = server_only.clone();
    run_until(harness.runner, move || {
        let m = metrics_check.clone();
        let api = local_check.clone();
        let names = names.clone();
        async move {
            let api = api.lock().await;
            m.digest_push_probe_total.load(Ordering::Relaxed) > 0
                && names.iter().all(|n| api.get_eventual(n).is_some())
        }
    })
    .await;
    server_handle.abort();

    assert_eq!(
        metrics
            .digest_push_keys_pushed_total
            .load(Ordering::Relaxed),
        0,
        "peer-only mismatches must push nothing"
    );
    assert_eq!(
        metrics.digest_push_match_total.load(Ordering::Relaxed),
        0,
        "the roots must NOT have matched (this is the peer-only branch)"
    );
    assert_eq!(
        metrics.full_sync_fallback_count.load(Ordering::Relaxed),
        0,
        "nothing-to-push must still count as handled (no full push)"
    );
}

/// Push side, partial failure: one key permanently fails to merge on the
/// peer (type mismatch), but every key in the batches AFTER the failing
/// batch is still delivered — a poisoned key must not starve the rest of
/// the keyspace (the legacy full push delivered them every cycle).
#[tokio::test]
async fn runner_digest_push_partial_merge_failure_still_delivers_later_batches() {
    let server = test_state("server");
    // The peer holds "0-poison" as a Set; locally it is a Counter →
    // merge_remote fails (permanently) on the peer for this key only.
    {
        let mut api = server.eventual.lock().await;
        api.eventual_set_add("0-poison", "member".into()).unwrap();
    }
    let (addr, server_handle) = serve(router(server.clone())).await;

    let mut harness = runner_against(&addr.to_string(), true).await;
    {
        let mut api = harness.local_api.lock().await;
        api.eventual_counter_inc("0-poison").unwrap();
        // 150 keys sorting AFTER "0-poison": with a batch size of 100 the
        // poisoned key fails in batch 1 while keys k-099..k-149 live in
        // batch 2 — which the old abort-on-merge-error behaviour never
        // sent.
        for i in 0..150 {
            api.eventual_counter_inc(&format!("k-{i:03}")).unwrap();
        }
    }

    harness
        .runner
        .inject_peer_frontier(&addr.to_string(), hlc(0, 0, ""));

    let metrics = harness.metrics.clone();
    let metrics_check = metrics.clone();
    let server_check = server.clone();
    run_until(harness.runner, move || {
        let m = metrics_check.clone();
        let srv = server_check.clone();
        async move {
            let api = srv.eventual.lock().await;
            // k-149 lives in the last batch: its arrival proves the
            // batches after the failing one were still attempted.
            m.digest_push_keys_pushed_total.load(Ordering::Relaxed) >= 150
                && api.get_eventual("k-149").is_some()
        }
    })
    .await;
    server_handle.abort();

    let api = server.eventual.lock().await;
    for i in [0usize, 98, 99, 149] {
        match api.get_eventual(&format!("k-{i:03}")) {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1, "k-{i:03}"),
            other => panic!("k-{i:03}: expected Counter, got {other:?}"),
        }
    }
    // The poisoned key itself was refused by the peer and stays a Set.
    match api.get_eventual("0-poison") {
        Some(CrdtValue::Set(s)) => assert!(s.contains(&"member".to_string())),
        other => panic!("0-poison: expected the peer's Set to survive, got {other:?}"),
    }
    drop(api);
    assert!(
        metrics.sync_failure_total.load(Ordering::Relaxed) >= 1,
        "the partial subset-push failure must be recorded"
    );
}

/// Pull side, rolling upgrade: a peer that answers `scheme_ok = false`
/// (digest scheme version mismatch) is cached as digest-unsupported —
/// the same cycle falls back to the legacy full sync (and converges),
/// and NO further digest probe is sent within the retry TTL.
#[tokio::test]
async fn runner_digest_scheme_mismatch_falls_back_and_caches() {
    let server = test_state("server");
    {
        let mut api = server.eventual.lock().await;
        api.eventual_counter_inc("version-key").unwrap();
    }

    let (addr, server_handle) = serve(scheme_mismatch_router(server.clone())).await;
    let mut harness = runner_against(&addr.to_string(), true).await;
    harness
        .runner
        .inject_peer_frontier(&addr.to_string(), hlc(1, 0, "stale"));

    let metrics = harness.metrics.clone();
    let metrics_check = metrics.clone();
    let local_check = harness.local_api.clone();
    run_until(harness.runner, move || {
        let m = metrics_check.clone();
        let api = local_check.clone();
        async move {
            let api = api.lock().await;
            // Wait for several full sync cycles so a (buggy) re-probe
            // would have had the chance to happen.
            api.get_eventual("version-key").is_some()
                && m.sync_attempt_total.load(Ordering::Relaxed) >= 4
        }
    })
    .await;
    server_handle.abort();

    assert!(
        metrics
            .digest_sync_unsupported_total
            .load(Ordering::Relaxed)
            >= 1,
        "scheme_ok = false must be recorded as digest-unsupported"
    );
    assert_eq!(
        metrics.digest_sync_attempt_total.load(Ordering::Relaxed),
        1,
        "the scheme rejection must be cached: exactly one probe, no re-probe within the TTL"
    );
    assert_eq!(
        metrics.digest_sync_root_match_total.load(Ordering::Relaxed)
            + metrics.digest_sync_partial_total.load(Ordering::Relaxed),
        0,
        "no digest sync can succeed against a scheme-mismatched peer"
    );

    // The same cycle converged through the legacy full sync.
    let api = harness.local_api.lock().await;
    match api.get_eventual("version-key") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
        other => panic!("expected Counter, got {other:?}"),
    }
}

/// Push side, rolling upgrade: a `scheme_ok = false` answer to the push
/// probe falls back to the legacy full-state push in the same cycle and
/// caches the peer (no digest probe of any kind afterwards).
#[tokio::test]
async fn runner_digest_push_scheme_mismatch_falls_back_to_full_push() {
    let server = test_state("server");
    let (addr, server_handle) = serve(scheme_mismatch_router(server.clone())).await;

    let mut harness = runner_against(&addr.to_string(), true).await;
    {
        let mut api = harness.local_api.lock().await;
        for i in 0..8 {
            api.eventual_counter_inc(&format!("push-{i}")).unwrap();
        }
    }
    // Zero frontier → change rate 1.0 → push probe.
    harness
        .runner
        .inject_peer_frontier(&addr.to_string(), hlc(0, 0, ""));

    let metrics = harness.metrics.clone();
    let metrics_check = metrics.clone();
    let server_check = server.clone();
    run_until(harness.runner, move || {
        let m = metrics_check.clone();
        let srv = server_check.clone();
        async move {
            let api = srv.eventual.lock().await;
            (0..8).all(|i| api.get_eventual(&format!("push-{i}")).is_some())
                && m.sync_attempt_total.load(Ordering::Relaxed) >= 4
        }
    })
    .await;
    server_handle.abort();

    assert_eq!(
        metrics.digest_push_probe_total.load(Ordering::Relaxed),
        1,
        "the scheme rejection must be cached after the first probe"
    );
    assert!(
        metrics
            .digest_sync_unsupported_total
            .load(Ordering::Relaxed)
            >= 1,
        "scheme_ok = false must be recorded as digest-unsupported"
    );
    assert!(
        metrics.full_sync_fallback_count.load(Ordering::Relaxed) >= 1,
        "the same cycle must fall back to the legacy full-state push"
    );
    assert_eq!(
        metrics.digest_sync_attempt_total.load(Ordering::Relaxed),
        0,
        "the pull phase must honour the cache set by the push probe"
    );

    let api = server.eventual.lock().await;
    match api.get_eventual("push-0") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
        other => panic!("expected Counter, got {other:?}"),
    }
}

/// Transient failure: a digest-capable peer that answers 503 must be
/// retried on EVERY cycle (`Failed` is not cached as unsupported) while
/// each cycle still converges through the legacy full sync.
#[tokio::test]
async fn runner_transient_digest_failure_falls_back_without_caching() {
    let server = test_state("server");
    {
        let mut api = server.eventual.lock().await;
        api.eventual_counter_inc("transient-key").unwrap();
    }

    let (addr, server_handle) = serve(transient_digest_failure_router(server.clone())).await;
    let mut harness = runner_against(&addr.to_string(), true).await;
    harness
        .runner
        .inject_peer_frontier(&addr.to_string(), hlc(1, 0, "stale"));

    let metrics = harness.metrics.clone();
    let metrics_check = metrics.clone();
    let local_check = harness.local_api.clone();
    run_until(harness.runner, move || {
        let m = metrics_check.clone();
        let api = local_check.clone();
        async move {
            let api = api.lock().await;
            // Multiple probes prove the 503 was NOT cached as
            // digest-unsupported (the pre-fix behaviour suppressed all
            // probes for 10 minutes after the first 503).
            api.get_eventual("transient-key").is_some()
                && m.digest_sync_attempt_total.load(Ordering::Relaxed) >= 3
        }
    })
    .await;
    server_handle.abort();

    assert!(
        metrics.digest_sync_failed_total.load(Ordering::Relaxed) >= 3,
        "every 503 must be classified as a transient failure"
    );
    assert_eq!(
        metrics
            .digest_sync_unsupported_total
            .load(Ordering::Relaxed),
        0,
        "a 503 must never mark the peer digest-unsupported"
    );

    // The legacy full sync kept converging throughout.
    let api = harness.local_api.lock().await;
    match api.get_eventual("transient-key") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
        other => panic!("expected Counter, got {other:?}"),
    }
}
