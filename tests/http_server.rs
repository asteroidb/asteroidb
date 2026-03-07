//! Integration tests for HTTP server startup, API access, and graceful shutdown.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::types::{KeyRange, NodeId};
use tokio::sync::Mutex;

fn test_state() -> Arc<AppState> {
    let node_id = NodeId("test-node".into());

    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: String::new(),
        },
        authority_nodes: vec![
            NodeId("auth-1".into()),
            NodeId("auth-2".into()),
            NodeId("auth-3".into()),
        ],
        auto_generated: false,
    });

    let namespace = Arc::new(RwLock::new(ns));

    let consensus = Arc::new(Mutex::new(ControlPlaneConsensus::new(vec![
        NodeId("auth-1".into()),
        NodeId("auth-2".into()),
        NodeId("auth-3".into()),
    ])));

    Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(node_id.clone()))),
        certified: Arc::new(Mutex::new(CertifiedApi::new(
            node_id,
            Arc::clone(&namespace),
        ))),
        namespace,
        metrics: Arc::new(RuntimeMetrics::default()),
        peers: None,
        peer_persist_path: None,
        consensus,
        internal_token: None,
        self_node_id: None,
        self_addr: None,
    })
}

#[tokio::test]
async fn server_binds_and_accepts_requests() {
    let state = test_state();
    let app = router(state);

    // Bind to port 0 to get an OS-assigned free port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give the server a moment to start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Test eventual write via HTTP.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/api/eventual/write"))
        .header("content-type", "application/json")
        .body(r#"{"type":"counter_inc","key":"hits"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Test eventual read.
    let resp = client
        .get(format!("http://{addr}/api/eventual/hits"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["key"], "hits");
    assert_eq!(body["value"]["type"], "counter");
    assert_eq!(body["value"]["value"], 1);

    // Test certified write.
    let resp = client
        .post(format!("http://{addr}/api/certified/write"))
        .header("content-type", "application/json")
        .body(r#"{"key":"sensor","value":{"type":"counter","value":5},"on_timeout":"pending"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Test certified read.
    let resp = client
        .get(format!("http://{addr}/api/certified/sensor"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["key"], "sensor");

    // Test status endpoint.
    let resp = client
        .get(format!("http://{addr}/api/status/sensor"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["key"], "sensor");
    assert_eq!(body["status"], "Pending");

    server.abort();
}

#[tokio::test]
async fn server_graceful_shutdown_via_abort() {
    let state = test_state();
    let app = router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Verify server is running.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/api/eventual/test"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Abort the server.
    server.abort();

    // Wait briefly for shutdown to propagate.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connection should fail after shutdown.
    let result = client
        .get(format!("http://{addr}/api/eventual/test"))
        .timeout(Duration::from_millis(200))
        .send()
        .await;
    assert!(result.is_err(), "expected connection error after shutdown");
}

#[tokio::test]
async fn certified_write_rejects_invalid_on_timeout() {
    let state = test_state();
    let app = router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // "bogus" should be rejected.
    let resp = client
        .post(format!("http://{addr}/api/certified/write"))
        .header("content-type", "application/json")
        .body(r#"{"key":"k","value":{"type":"counter","value":1},"on_timeout":"bogus"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error_code"], "INVALID_ARGUMENT");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("invalid on_timeout value"),
        "expected descriptive error message, got: {}",
        body["message"]
    );

    // "ERROR" (wrong case) should also be rejected.
    let resp = client
        .post(format!("http://{addr}/api/certified/write"))
        .header("content-type", "application/json")
        .body(r#"{"key":"k","value":{"type":"counter","value":1},"on_timeout":"ERROR"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error_code"], "INVALID_ARGUMENT");

    server.abort();
}

#[tokio::test]
async fn certified_write_accepts_valid_on_timeout_values() {
    let state = test_state();
    let app = router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // "error" should be accepted (returns 504 because certification is not
    // immediately available, which is the correct OnTimeout::Error behaviour).
    let resp = client
        .post(format!("http://{addr}/api/certified/write"))
        .header("content-type", "application/json")
        .body(r#"{"key":"k1","value":{"type":"counter","value":1},"on_timeout":"error"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 504);

    // "pending" should be accepted.
    let resp = client
        .post(format!("http://{addr}/api/certified/write"))
        .header("content-type", "application/json")
        .body(r#"{"key":"k2","value":{"type":"counter","value":1},"on_timeout":"pending"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Omitted on_timeout should default to "pending" and succeed.
    let resp = client
        .post(format!("http://{addr}/api/certified/write"))
        .header("content-type", "application/json")
        .body(r#"{"key":"k3","value":{"type":"counter","value":1}}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    server.abort();
}
