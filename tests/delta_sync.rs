//! Integration tests for HLC/frontier-based delta sync protocol (#120).
//!
//! Tests verify that delta sync correctly synchronizes only changed entries
//! between nodes, and falls back to full sync when delta sync is unavailable.

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
use asteroidb_poc::network::sync::{
    DeltaSyncRequest, DeltaSyncResponse, SyncClient, SyncRequest, SyncResponse,
};
use asteroidb_poc::network::{PeerConfig, PeerRegistry};
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::runtime::{NodeRunner, NodeRunnerConfig};
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

fn test_state() -> Arc<AppState> {
    let nid = node_id("test-node");

    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: String::new(),
        },
        authority_nodes: vec![node_id("auth-1"), node_id("auth-2"), node_id("auth-3")],
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
    })
}

async fn body_string(body: Body) -> String {
    let bytes = body.collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

// ---------------------------------------------------------------
// Delta sync endpoint basic operation
// ---------------------------------------------------------------

#[tokio::test]
async fn delta_sync_returns_empty_for_fresh_store() {
    let state = test_state();
    let app = router(state);

    let req_body = serde_json::to_string(&DeltaSyncRequest {
        sender: "node-2".into(),
        frontier: hlc(0, 0, ""),
        observed: vec![],
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
    assert!(delta.entries.is_empty());
    assert!(delta.sender_frontier.is_none());
}

#[tokio::test]
async fn delta_sync_returns_all_entries_for_zero_frontier() {
    let state = test_state();

    // Write some entries
    {
        let mut api = state.eventual.lock().await;
        api.eventual_counter_inc("key-a").unwrap();
        api.eventual_counter_inc("key-b").unwrap();
        api.eventual_counter_inc("key-c").unwrap();
    }

    let app = router(state);

    let req_body = serde_json::to_string(&DeltaSyncRequest {
        sender: "node-2".into(),
        frontier: hlc(0, 0, ""),
        observed: vec![],
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

    assert_eq!(delta.entries.len(), 3);
    assert!(delta.sender_frontier.is_some());

    let keys: Vec<&str> = delta.entries.iter().map(|e| e.key.as_str()).collect();
    assert!(keys.contains(&"key-a"));
    assert!(keys.contains(&"key-b"));
    assert!(keys.contains(&"key-c"));
}

#[tokio::test]
async fn delta_sync_returns_only_changes_after_frontier() {
    let state = test_state();

    // Write initial entries
    {
        let mut api = state.eventual.lock().await;
        api.eventual_counter_inc("old-key").unwrap();
    }

    // Capture the frontier after initial writes
    let frontier = {
        let api = state.eventual.lock().await;
        api.store().current_frontier().unwrap()
    };

    // Write new entries after the frontier
    {
        let mut api = state.eventual.lock().await;
        api.eventual_counter_inc("new-key-1").unwrap();
        api.eventual_counter_inc("new-key-2").unwrap();
    }

    let app = router(state);

    let req_body = serde_json::to_string(&DeltaSyncRequest {
        sender: "node-2".into(),
        frontier,
        observed: vec![],
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

    // Should only contain the new entries, not the old one
    assert_eq!(delta.entries.len(), 2);
    let keys: Vec<&str> = delta.entries.iter().map(|e| e.key.as_str()).collect();
    assert!(keys.contains(&"new-key-1"));
    assert!(keys.contains(&"new-key-2"));
    assert!(!keys.contains(&"old-key"));
}

// ---------------------------------------------------------------
// Two-node delta sync simulation
// ---------------------------------------------------------------

#[tokio::test]
async fn two_node_delta_sync_convergence() {
    // Simulate two nodes syncing via delta protocol.
    // Node A writes some entries, Node B pulls delta, then Node B writes,
    // Node A pulls delta.

    let state_a = test_state();
    let state_b = test_state();

    // Node A writes entries.
    {
        let mut api = state_a.eventual.lock().await;
        api.eventual_counter_inc("shared-counter").unwrap();
        api.eventual_counter_inc("shared-counter").unwrap();
        api.eventual_set_add("users", "alice".into()).unwrap();
    }

    // Node B requests delta from Node A (zero frontier = get everything).
    let app_a = router(state_a.clone());

    let req_body = serde_json::to_string(&DeltaSyncRequest {
        sender: "node-b".into(),
        frontier: hlc(0, 0, ""),
        observed: vec![],
    })
    .unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/api/internal/sync/delta")
        .header("content-type", "application/json")
        .body(Body::from(req_body))
        .unwrap();

    let resp = app_a.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_string(resp.into_body()).await;
    let delta: DeltaSyncResponse = serde_json::from_str(&body).unwrap();

    // Node B applies the delta.
    {
        let mut api = state_b.eventual.lock().await;
        for entry in &delta.entries {
            api.merge_remote_with_hlc(entry.key.clone(), &entry.value, entry.hlc.clone())
                .unwrap();
        }
    }

    // Verify Node B now has the same data.
    {
        let api = state_b.eventual.lock().await;
        match api.get_eventual("shared-counter") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
            other => panic!("expected Counter(2), got {other:?}"),
        }
        match api.get_eventual("users") {
            Some(CrdtValue::Set(s)) => assert!(s.contains(&"alice".to_string())),
            other => panic!("expected Set with alice, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------
// Three-node delta sync convergence
// ---------------------------------------------------------------

#[tokio::test]
async fn three_node_delta_sync_convergence() {
    let state_a = test_state();
    let state_b = test_state();
    let state_c = test_state();

    // Each node writes to a different key.
    {
        let mut api = state_a.eventual.lock().await;
        api.eventual_counter_inc("counter-a").unwrap();
    }
    {
        let mut api = state_b.eventual.lock().await;
        api.eventual_counter_inc("counter-b").unwrap();
    }
    {
        let mut api = state_c.eventual.lock().await;
        api.eventual_counter_inc("counter-c").unwrap();
    }

    // Helper: pull delta from source and apply to target.
    async fn sync_delta(source: &Arc<AppState>, target: &Arc<AppState>) {
        let app = router(source.clone());
        let req_body = serde_json::to_string(&DeltaSyncRequest {
            sender: "sync".into(),
            frontier: hlc(0, 0, ""),
            observed: vec![],
        })
        .unwrap();

        let req = Request::builder()
            .method("POST")
            .uri("/api/internal/sync/delta")
            .header("content-type", "application/json")
            .body(Body::from(req_body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let body = body_string(resp.into_body()).await;
        let delta: DeltaSyncResponse = serde_json::from_str(&body).unwrap();

        let mut api = target.eventual.lock().await;
        for entry in &delta.entries {
            let _ = api.merge_remote_with_hlc(entry.key.clone(), &entry.value, entry.hlc.clone());
        }
    }

    // Sync A -> B, A -> C, B -> C, C -> B.
    sync_delta(&state_a, &state_b).await;
    sync_delta(&state_a, &state_c).await;
    sync_delta(&state_b, &state_c).await;
    sync_delta(&state_c, &state_b).await;

    // All nodes should now have all three counters.
    for (name, state) in [("A", &state_a), ("B", &state_b), ("C", &state_c)] {
        let api = state.eventual.lock().await;
        // Node A only has its own writes unless synced.
        // But B and C should have everything.
        if name != "A" {
            assert!(
                api.get_eventual("counter-a").is_some(),
                "{name} should have counter-a"
            );
            assert!(
                api.get_eventual("counter-b").is_some(),
                "{name} should have counter-b"
            );
            assert!(
                api.get_eventual("counter-c").is_some(),
                "{name} should have counter-c"
            );
        }
    }
}

// ---------------------------------------------------------------
// Delta sync with frontier update
// ---------------------------------------------------------------

#[tokio::test]
async fn delta_sync_frontier_advances_correctly() {
    let state = test_state();

    // Write initial entry.
    {
        let mut api = state.eventual.lock().await;
        api.eventual_counter_inc("key-1").unwrap();
    }

    let app = router(state.clone());

    // First delta pull - get everything.
    let req_body = serde_json::to_string(&DeltaSyncRequest {
        sender: "node-2".into(),
        frontier: hlc(0, 0, ""),
        observed: vec![],
    })
    .unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/api/internal/sync/delta")
        .header("content-type", "application/json")
        .body(Body::from(req_body))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    let body = body_string(resp.into_body()).await;
    let delta1: DeltaSyncResponse = serde_json::from_str(&body).unwrap();
    assert_eq!(delta1.entries.len(), 1);
    let frontier1 = delta1.sender_frontier.unwrap();

    // Write a new entry.
    {
        let mut api = state.eventual.lock().await;
        api.eventual_counter_inc("key-2").unwrap();
    }

    // Second delta pull using the frontier from the first pull.
    let req_body = serde_json::to_string(&DeltaSyncRequest {
        sender: "node-2".into(),
        frontier: frontier1,
        observed: vec![],
    })
    .unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/api/internal/sync/delta")
        .header("content-type", "application/json")
        .body(Body::from(req_body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    let body = body_string(resp.into_body()).await;
    let delta2: DeltaSyncResponse = serde_json::from_str(&body).unwrap();

    // Should only return the new entry.
    assert_eq!(delta2.entries.len(), 1);
    assert_eq!(delta2.entries[0].key, "key-2");
}

// ---------------------------------------------------------------
// Frontier must use batch max HLC, not current_frontier (#193)
// ---------------------------------------------------------------

/// Verify that writes happening after entries_since is called are NOT
/// skipped when the peer frontier is advanced to the batch's max HLC
/// rather than the store's current_frontier.
#[tokio::test]
async fn concurrent_writes_during_push_are_not_skipped() {
    let state = test_state();

    // Phase 1: write initial entries and capture the batch.
    {
        let mut api = state.eventual.lock().await;
        api.eventual_counter_inc("batch-key-1").unwrap();
        api.eventual_counter_inc("batch-key-2").unwrap();
    }

    // Capture entries_since(zero) — this is the "batch" that would be pushed.
    let batch = {
        let api = state.eventual.lock().await;
        api.store().entries_since(&hlc(0, 0, ""))
    };
    assert_eq!(batch.len(), 2);
    let batch_max_hlc = batch.last().unwrap().2.clone();

    // Phase 2: a concurrent write happens AFTER the batch was captured
    // but BEFORE the push completes (simulated by writing now).
    {
        let mut api = state.eventual.lock().await;
        api.eventual_counter_inc("concurrent-key").unwrap();
    }

    // The store's current_frontier now includes the concurrent write.
    let current_frontier = {
        let api = state.eventual.lock().await;
        api.store().current_frontier().unwrap()
    };
    assert!(
        current_frontier > batch_max_hlc,
        "current_frontier should be ahead of batch max"
    );

    // If we advance the peer frontier to batch_max_hlc (correct behavior),
    // the concurrent write is picked up next cycle.
    let next_delta = {
        let api = state.eventual.lock().await;
        api.store().entries_since(&batch_max_hlc)
    };
    assert_eq!(
        next_delta.len(),
        1,
        "using batch max HLC should leave 1 entry for next cycle"
    );
    assert_eq!(next_delta[0].0, "concurrent-key");

    // If we had used current_frontier (the bug), the concurrent write
    // would be permanently skipped.
    let bad_delta = {
        let api = state.eventual.lock().await;
        api.store().entries_since(&current_frontier)
    };
    assert!(
        bad_delta.is_empty(),
        "using current_frontier would skip the concurrent write (the bug)"
    );
}

/// Verify that on partial push failure, advancing the frontier to the
/// last successfully pushed entry's HLC preserves unpushed entries.
#[tokio::test]
async fn partial_failure_does_not_skip_entries() {
    let state = test_state();

    // Write 4 entries with distinct timestamps.
    {
        let mut api = state.eventual.lock().await;
        api.eventual_counter_inc("entry-1").unwrap();
        api.eventual_counter_inc("entry-2").unwrap();
        api.eventual_counter_inc("entry-3").unwrap();
        api.eventual_counter_inc("entry-4").unwrap();
    }

    // Capture the batch (sorted by HLC).
    let batch = {
        let api = state.eventual.lock().await;
        api.store().entries_since(&hlc(0, 0, ""))
    };
    assert_eq!(batch.len(), 4);

    // Simulate partial failure: only first 2 entries were pushed.
    let pushed = 2;
    let partial_frontier = batch[pushed - 1].2.clone();

    // The remaining entries (3 and 4) should be returned on next cycle.
    let remaining = {
        let api = state.eventual.lock().await;
        api.store().entries_since(&partial_frontier)
    };
    assert_eq!(
        remaining.len(),
        2,
        "2 unpushed entries should remain after partial push"
    );
    let remaining_keys: Vec<&str> = remaining.iter().map(|(k, _, _)| k.as_str()).collect();
    assert!(remaining_keys.contains(&"entry-3"));
    assert!(remaining_keys.contains(&"entry-4"));
}

// ---------------------------------------------------------------
// Untracked-key compensation (M-2)
// ---------------------------------------------------------------

/// A zero-frontier ("complete") pull must include keys with NO tracked
/// per-key HLC (v1/v2-migrated stores): `delta_entries_since` scans only
/// the timestamps map, so without the `untracked_entries` compensation
/// the receiver would treat the response as complete — adopting the
/// sender's whole applied_origins map — while these keys never transfer
/// through the pull path (read-your-writes false success + permanent
/// divergence). Incremental pulls omit the field (they never adopt
/// third-origin claims).
#[tokio::test]
async fn zero_frontier_delta_includes_untracked_entries() {
    use asteroidb_poc::crdt::pn_counter::PnCounter;

    let state = test_state();

    // One tracked write plus one migrated (timestamp-less) key.
    {
        let mut api = state.eventual.lock().await;
        api.eventual_counter_inc("tracked-key").unwrap();
        let mut migrated = PnCounter::new();
        migrated.increment(&node_id("old-writer"));
        migrated.increment(&node_id("old-writer"));
        // Plain put() without record_change: exactly the shape a v1/v2
        // snapshot migration leaves behind.
        api.store_mut()
            .put("migrated-key".into(), CrdtValue::Counter(migrated));
        assert!(api.store().timestamp_for("migrated-key").is_none());
    }

    // Complete pull (zero frontier): the untracked key must ride along.
    let req_body = serde_json::to_string(&DeltaSyncRequest {
        sender: "node-2".into(),
        frontier: hlc(0, 0, ""),
        observed: vec![],
    })
    .unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/api/internal/sync/delta")
        .header("content-type", "application/json")
        .body(Body::from(req_body))
        .unwrap();
    let resp = router(state.clone()).oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let delta: DeltaSyncResponse =
        serde_json::from_str(&body_string(resp.into_body()).await).unwrap();

    assert_eq!(delta.entries.len(), 1, "tracked key rides the delta scan");
    assert_eq!(delta.entries[0].key, "tracked-key");
    match delta.untracked_entries.get("migrated-key") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
        other => panic!("untracked key must be compensated, got {other:?}"),
    }

    // Incremental pull (non-zero frontier): no compensation payload.
    let req_body = serde_json::to_string(&DeltaSyncRequest {
        sender: "node-2".into(),
        frontier: hlc(1, 0, "node-2"),
        observed: vec![],
    })
    .unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/api/internal/sync/delta")
        .header("content-type", "application/json")
        .body(Body::from(req_body))
        .unwrap();
    let resp = router(state).oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let delta: DeltaSyncResponse =
        serde_json::from_str(&body_string(resp.into_body()).await).unwrap();
    assert!(
        delta.untracked_entries.is_empty(),
        "incremental pulls carry no untracked compensation"
    );
}

/// A COMPACTED sender (`pruned_floor` set) must NOT ship its pruned
/// keyspace as `untracked_entries` on a zero-frontier pull: pruning
/// removes per-key timestamps while keeping data, so every pruned key
/// matches the untracked predicate — a full-dump-sized payload on the
/// delta path — while the receiver's floor gate (`zero >= pruned_floor`)
/// is guaranteed to reject the claims and fall back to full sync anyway.
#[tokio::test]
async fn compacted_sender_ships_no_untracked_entries() {
    let state = test_state();

    // Two tracked writes, then compaction prunes both timestamps.
    {
        let mut api = state.eventual.lock().await;
        api.eventual_counter_inc("pruned-1").unwrap();
        api.eventual_counter_inc("pruned-2").unwrap();
        let frontier = api.store().current_frontier().unwrap();
        let pruned = api.store_mut().prune_timestamps_before("", &frontier);
        assert_eq!(pruned, 2, "both timestamps pruned");
        assert!(api.store().pruned_floor().is_some());
        assert!(api.store().timestamp_for("pruned-1").is_none());
    }

    let req_body = serde_json::to_string(&DeltaSyncRequest {
        sender: "node-2".into(),
        frontier: hlc(0, 0, ""),
        observed: vec![],
    })
    .unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/api/internal/sync/delta")
        .header("content-type", "application/json")
        .body(Body::from(req_body))
        .unwrap();
    let resp = router(state).oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let delta: DeltaSyncResponse =
        serde_json::from_str(&body_string(resp.into_body()).await).unwrap();

    assert!(
        delta.pruned_floor.is_some(),
        "the floor rides the response so the receiver rejects the claims"
    );
    assert!(
        delta.untracked_entries.is_empty(),
        "a compacted sender must not dump its pruned keyspace on the delta path"
    );
}

// ---------------------------------------------------------------
// RR gate: converged keys stop retransmitting (M-6)
// ---------------------------------------------------------------

/// Snapshot the per-key HLC timestamps of a node's eventual store.
async fn key_timestamps(state: &Arc<AppState>) -> Vec<(String, HlcTimestamp)> {
    let api = state.eventual.lock().await;
    let mut snap: Vec<(String, HlcTimestamp)> = api
        .store()
        .keys()
        .into_iter()
        .filter_map(|k| {
            api.store()
                .timestamp_for(k)
                .map(|ts| (k.clone(), ts.clone()))
        })
        .collect();
    snap.sort();
    snap
}

/// One push half-round, modelled on the runner: scan the sender's delta
/// entries since `frontier`, POST them to the receiver's
/// `/api/internal/sync` (the `merge_remote` push path), and advance the
/// sender's push frontier to the batch max HLC. Returns the batch size.
async fn push_round(
    source: &Arc<AppState>,
    target: &Arc<AppState>,
    frontier: &mut HlcTimestamp,
) -> usize {
    let entries: Vec<(String, CrdtValue, HlcTimestamp)> = {
        let api = source.eventual.lock().await;
        api.store().delta_entries_since(frontier)
    };
    if entries.is_empty() {
        return 0;
    }
    *frontier = entries.last().unwrap().2.clone();

    let req_entries: std::collections::HashMap<String, CrdtValue> = entries
        .iter()
        .map(|(k, v, _)| (k.clone(), v.clone()))
        .collect();
    let req_body = serde_json::to_string(&SyncRequest {
        sender: "rr-test".into(),
        entries: req_entries,
    })
    .unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/api/internal/sync")
        .header("content-type", "application/json")
        .body(Body::from(req_body))
        .unwrap();
    let resp = router(target.clone()).oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let sync_resp: SyncResponse =
        serde_json::from_str(&body_string(resp.into_body()).await).unwrap();
    assert!(sync_resp.errors.is_empty(), "push merge must not error");
    entries.len()
}

/// M-6 ping-pong reproduction (RED before the RR gate): once two nodes
/// have converged, bidirectional push sync must go quiet. The old
/// `merge_remote` re-stamped EVERY received entry with a fresh local
/// HLC, so each push re-injected the converged keys into the receiver's
/// delta scan and full CRDT state ping-ponged forever.
#[tokio::test]
async fn converged_key_stops_retransmitting() {
    let state_a = test_state();
    let state_b = test_state();

    // Node A writes, node B converges via a delta pull (the
    // two_node_delta_sync_convergence harness shape).
    {
        let mut api = state_a.eventual.lock().await;
        api.eventual_counter_inc("shared-counter").unwrap();
        api.eventual_counter_inc("shared-counter").unwrap();
        api.eventual_set_add("users", "alice".into()).unwrap();
    }
    let req_body = serde_json::to_string(&DeltaSyncRequest {
        sender: "node-b".into(),
        frontier: hlc(0, 0, ""),
        observed: vec![],
    })
    .unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/api/internal/sync/delta")
        .header("content-type", "application/json")
        .body(Body::from(req_body))
        .unwrap();
    let resp = router(state_a.clone()).oneshot(req).await.unwrap();
    let delta: DeltaSyncResponse =
        serde_json::from_str(&body_string(resp.into_body()).await).unwrap();
    {
        let mut api = state_b.eventual.lock().await;
        for entry in &delta.entries {
            api.merge_remote_with_hlc(entry.key.clone(), &entry.value, entry.hlc.clone())
                .unwrap();
        }
    }

    // Converged. Snapshot per-key timestamps and frontiers on both sides.
    let ts_a = key_timestamps(&state_a).await;
    let ts_b = key_timestamps(&state_b).await;
    let frontier_a = state_a.eventual.lock().await.store().current_frontier();
    let frontier_b = state_b.eventual.lock().await.store().current_frontier();

    // Bidirectional push sync, 3 rounds, runner-style frontier tracking.
    let zero = hlc(0, 0, "");
    let mut push_frontier_ab = zero.clone();
    let mut push_frontier_ba = zero;
    for round in 0..3 {
        let sent_ab = push_round(&state_a, &state_b, &mut push_frontier_ab).await;
        let sent_ba = push_round(&state_b, &state_a, &mut push_frontier_ba).await;
        if round == 0 {
            // The first round legitimately re-offers the converged state
            // (the push baseline starts at zero) — it must be absorbed as
            // a no-op on both sides.
            assert!(sent_ab > 0 && sent_ba > 0, "round 0 sends the backlog");
        } else {
            // RED before the RR gate: the receiver's re-stamp made every
            // later round re-send the converged keys forever.
            assert_eq!(
                sent_ab, 0,
                "converged A->B push must go quiet (round {round})"
            );
            assert_eq!(
                sent_ba, 0,
                "converged B->A push must go quiet (round {round})"
            );
        }
        // No round may move a per-key timestamp or a frontier.
        assert_eq!(key_timestamps(&state_a).await, ts_a, "round {round}");
        assert_eq!(key_timestamps(&state_b).await, ts_b, "round {round}");
        assert_eq!(
            state_a.eventual.lock().await.store().current_frontier(),
            frontier_a
        );
        assert_eq!(
            state_b.eventual.lock().await.store().current_frontier(),
            frontier_b
        );
    }

    // Both sides observed redundant echoes and counted them.
    assert!(state_a.eventual.lock().await.redundant_merge_skips() > 0);
    assert!(state_b.eventual.lock().await.redundant_merge_skips() > 0);
}

/// After a REAL change the echo is bounded: the write travels A->B (a
/// true inflation, re-stamped on B), the echo B->A is absorbed as a
/// no-op, and the third half-round is already empty.
#[tokio::test]
async fn bounded_echo_after_real_change() {
    let state_a = test_state();
    let state_b = test_state();

    // Converge A and B via pushes (round 0 backlog + echo absorption).
    {
        let mut api = state_a.eventual.lock().await;
        api.eventual_counter_inc("k").unwrap();
    }
    let zero = hlc(0, 0, "");
    let mut push_frontier_ab = zero.clone();
    let mut push_frontier_ba = zero;
    for _ in 0..3 {
        push_round(&state_a, &state_b, &mut push_frontier_ab).await;
        push_round(&state_b, &state_a, &mut push_frontier_ba).await;
    }
    assert_eq!(
        push_round(&state_a, &state_b, &mut push_frontier_ab).await,
        0
    );
    assert_eq!(
        push_round(&state_b, &state_a, &mut push_frontier_ba).await,
        0
    );

    // A real write on A.
    {
        let mut api = state_a.eventual.lock().await;
        api.eventual_counter_inc("k").unwrap();
    }

    // Half-round 1: the change travels A->B (B re-stamps: true inflation).
    assert_eq!(
        push_round(&state_a, &state_b, &mut push_frontier_ab).await,
        1
    );
    // Half-round 2: B echoes its re-stamped key back once; A absorbs it.
    assert_eq!(
        push_round(&state_b, &state_a, &mut push_frontier_ba).await,
        1
    );
    // Half-round 3: silence — the echo must not breed another echo.
    assert_eq!(
        push_round(&state_a, &state_b, &mut push_frontier_ab).await,
        0
    );
    assert_eq!(
        push_round(&state_b, &state_a, &mut push_frontier_ba).await,
        0
    );
}

/// A 3-node push cycle (A->B->C->A) must not amplify: after the write has
/// gone around once and the origin absorbed the echo, every later cycle
/// is empty on all three edges.
#[tokio::test]
async fn three_node_push_cycle_quiesces() {
    let state_a = test_state();
    let state_b = test_state();
    let state_c = test_state();

    {
        let mut api = state_a.eventual.lock().await;
        api.eventual_counter_inc("ring-key").unwrap();
    }

    let zero = hlc(0, 0, "");
    let mut f_ab = zero.clone();
    let mut f_bc = zero.clone();
    let mut f_ca = zero;

    // Cycle 1 carries the write around the ring; the C->A edge is the
    // origin's echo and must be absorbed (A already dominates).
    let cycle1 = [
        push_round(&state_a, &state_b, &mut f_ab).await,
        push_round(&state_b, &state_c, &mut f_bc).await,
        push_round(&state_c, &state_a, &mut f_ca).await,
    ];
    assert_eq!(cycle1, [1, 1, 1]);

    // Every later cycle must be empty on all edges (RED before the RR
    // gate: each hop's re-stamp fed the next hop forever).
    for cycle in 2..5 {
        let sent = [
            push_round(&state_a, &state_b, &mut f_ab).await,
            push_round(&state_b, &state_c, &mut f_bc).await,
            push_round(&state_c, &state_a, &mut f_ca).await,
        ];
        assert_eq!(sent, [0, 0, 0], "cycle {cycle} must be silent");
    }

    // All three converged on the same value.
    for state in [&state_a, &state_b, &state_c] {
        let api = state.eventual.lock().await;
        match api.get_eventual("ring-key") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter(1), got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------
// RR gate at the NodeRunner level (design §7(C))
// ---------------------------------------------------------------

/// AppState sharing the given eventual API (a NodeRunner and this HTTP
/// surface serve the same store), without authority definitions — the
/// data-plane-only shape of the digest_sync.rs runner harness.
fn shared_state(name: &str, eventual: Arc<Mutex<EventualApi>>) -> Arc<AppState> {
    let nid = node_id(name);
    let namespace = Arc::new(RwLock::new(SystemNamespace::new()));

    Arc::new(AppState {
        eventual,
        certified: Arc::new(Mutex::new(CertifiedApi::new(nid, Arc::clone(&namespace)))),
        namespace,
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
    })
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

/// A NodeRunner over a SHARED eventual API, running the production sync
/// loop (delta pull + delta/full push) at a fast interval. Digest sync
/// is disabled so every transfer actually carries entries through
/// `merge_remote` / `merge_remote_with_hlc` — a digest probe answering
/// "root match" would suppress the very deliveries whose RR absorption
/// this harness asserts. The fast GC tick only mirrors the in-memory RR
/// skip counter into `sync_redundant_merge_skips_total` (the hour-long
/// retention keeps actual sweeps out of the picture).
async fn quiesce_runner(
    name: &str,
    eventual: Arc<Mutex<EventualApi>>,
    peer_name: &str,
    peer_addr: &str,
) -> (NodeRunner, Arc<RuntimeMetrics>) {
    let ns = Arc::new(RwLock::new(SystemNamespace::new()));
    let certified = Arc::new(Mutex::new(CertifiedApi::new(node_id(name), ns)));
    let registry = PeerRegistry::new(
        node_id(name),
        vec![PeerConfig {
            node_id: node_id(peer_name),
            addr: peer_addr.to_string(),
        }],
    )
    .unwrap();
    let sync_client = SyncClient::new(Arc::new(Mutex::new(registry)));

    let config = NodeRunnerConfig {
        certification_interval: Duration::from_millis(500),
        cleanup_interval: Duration::from_secs(60),
        compaction_check_interval: Duration::from_secs(60),
        frontier_report_interval: Duration::from_secs(60),
        sync_interval: Some(Duration::from_millis(25)),
        ping_interval: None,
        gc_interval: Duration::from_millis(50),
        gc_retention: Duration::from_secs(3600),
        digest_sync_enabled: false,
        ..NodeRunnerConfig::default()
    };

    let metrics = Arc::new(RuntimeMetrics::default());
    let runner = NodeRunner::with_sync(
        node_id(name),
        certified,
        CompactionEngine::with_defaults(),
        config,
        sync_client,
        eventual,
        metrics.clone(),
    )
    .await;
    (runner, metrics)
}

/// Snapshot a store's data as a sorted map.
async fn snapshot_data(api: &Arc<Mutex<EventualApi>>) -> BTreeMap<String, CrdtValue> {
    let api = api.lock().await;
    api.store()
        .all_entries()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Per-key timestamp snapshot over the wire, exactly as the design
/// specifies: `GET /api/internal/keys` via the production client.
async fn dump_timestamps(client: &SyncClient, addr: &str) -> BTreeMap<String, HlcTimestamp> {
    client
        .pull_all_keys(addr)
        .await
        .expect("GET /api/internal/keys must succeed")
        .timestamps
        .into_iter()
        .collect()
}

/// Runner-level quiesce (design §7(C)): two REAL NodeRunners driving the
/// production push/pull loop over live HTTP must go fully quiet after
/// convergence — per-key timestamps observed via `GET /api/internal/keys`
/// stay frozen across at least 3 further sync cycles on each runner, and
/// the RR skip counter is exported through
/// `sync_redundant_merge_skips_total`. This guards against the runner's
/// push loop diverging from the handler-level `push_round` simulation
/// used by the tests above (e.g. switching the delta baseline to the
/// pull-advanced `peer_frontiers`, or advancing `push_frontiers` before
/// merge success) without any test noticing.
#[tokio::test]
async fn two_node_push_quiesces_after_convergence() {
    let api_a = Arc::new(Mutex::new(EventualApi::new(node_id("rr-a"))));
    let api_b = Arc::new(Mutex::new(EventualApi::new(node_id("rr-b"))));
    let state_a = shared_state("rr-a", api_a.clone());
    let state_b = shared_state("rr-b", api_b.clone());
    let (addr_a, server_a) = serve(router(state_a.clone())).await;
    let (addr_b, server_b) = serve(router(state_b.clone())).await;

    // All writes happen on A before the runners start; B converges
    // through the real sync loop only.
    {
        let mut api = api_a.lock().await;
        api.eventual_counter_inc("shared-counter").unwrap();
        api.eventual_counter_inc("shared-counter").unwrap();
        api.eventual_set_add("users", "alice".into()).unwrap();
    }

    let (runner_a, metrics_a) =
        quiesce_runner("rr-a", api_a.clone(), "rr-b", &addr_b.to_string()).await;
    let (runner_b, metrics_b) =
        quiesce_runner("rr-b", api_b.clone(), "rr-a", &addr_a.to_string()).await;
    let stop_a = runner_a.shutdown_handle();
    let stop_b = runner_b.shutdown_handle();
    let task_a = tokio::spawn(async move {
        let mut runner = runner_a;
        runner.run().await
    });
    let task_b = tokio::spawn(async move {
        let mut runner = runner_b;
        runner.run().await
    });

    // Phase 1: convergence. Once the two data states are equal, no
    // further merge can inflate either side, so (with the RR gate) every
    // per-key timestamp is frozen from this point on — the baseline
    // snapshot below is race-free.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let a = snapshot_data(&api_a).await;
        let b = snapshot_data(&api_b).await;
        if a.len() == 2 && a == b {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            let _ = stop_a.send(true);
            let _ = stop_b.send(true);
            panic!("two-node runner sync did not converge (a={a:?}, b={b:?})");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Phase 2: baseline timestamp snapshots over the wire.
    let observer_registry = PeerRegistry::new(
        node_id("rr-observer"),
        vec![
            PeerConfig {
                node_id: node_id("rr-a"),
                addr: addr_a.to_string(),
            },
            PeerConfig {
                node_id: node_id("rr-b"),
                addr: addr_b.to_string(),
            },
        ],
    )
    .unwrap();
    let observer = SyncClient::new(Arc::new(Mutex::new(observer_registry)));
    let ts_a0 = dump_timestamps(&observer, &addr_a.to_string()).await;
    let ts_b0 = dump_timestamps(&observer, &addr_b.to_string()).await;
    assert_eq!(ts_a0.len(), 2, "A's keys must be delta-tracked");
    assert_eq!(ts_b0.len(), 2, "B's keys must be delta-tracked");

    // Phase 3: at least 3 further REAL sync cycles on each runner.
    // `sync_attempt_total` ticks once per peer at the START of a cycle,
    // so waiting for base+4 guarantees >= 3 full cycles completed.
    let base_a = metrics_a.sync_attempt_total.load(Ordering::Relaxed);
    let base_b = metrics_b.sync_attempt_total.load(Ordering::Relaxed);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let a = metrics_a.sync_attempt_total.load(Ordering::Relaxed);
        let b = metrics_b.sync_attempt_total.load(Ordering::Relaxed);
        if a >= base_a + 4 && b >= base_b + 4 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            let _ = stop_a.send(true);
            let _ = stop_b.send(true);
            panic!("sync cycles stalled (a={a}/{base_a}, b={b}/{base_b})");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The quiesce assertion: not one per-key timestamp moved. RED with
    // the pre-M-6 unconditional re-stamp, which advanced a timestamp on
    // every push delivery.
    let ts_a1 = dump_timestamps(&observer, &addr_a.to_string()).await;
    let ts_b1 = dump_timestamps(&observer, &addr_b.to_string()).await;
    assert_eq!(
        ts_a1, ts_a0,
        "A's per-key timestamps must be frozen across real push cycles"
    );
    assert_eq!(
        ts_b1, ts_b0,
        "B's per-key timestamps must be frozen across real push cycles"
    );

    // Each side absorbed at least one redundant delivery (the push/pull
    // echo of already-held state), and the GC tick exported the counter.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let a = metrics_a
            .sync_redundant_merge_skips_total
            .load(Ordering::Relaxed);
        let b = metrics_b
            .sync_redundant_merge_skips_total
            .load(Ordering::Relaxed);
        if a > 0 && b > 0 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            let _ = stop_a.send(true);
            let _ = stop_b.send(true);
            panic!("sync_redundant_merge_skips_total was never exported (a={a}, b={b})");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let _ = stop_a.send(true);
    let _ = stop_b.send(true);
    let _ = task_a.await;
    let _ = task_b.await;
    server_a.abort();
    server_b.abort();
}
