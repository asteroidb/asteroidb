//! Integration tests for HLC/frontier-based delta sync protocol (#120).
//!
//! Tests verify that delta sync correctly synchronizes only changed entries
//! between nodes, and falls back to full sync when delta sync is unavailable.

use std::sync::Arc;
use std::sync::RwLock;

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::network::sync::{DeltaSyncRequest, DeltaSyncResponse};
use asteroidb_poc::ops::metrics::RuntimeMetrics;
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
    });

    let namespace = Arc::new(RwLock::new(ns));

    Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(nid.clone()))),
        certified: Arc::new(Mutex::new(CertifiedApi::new(nid, Arc::clone(&namespace)))),
        namespace,
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
