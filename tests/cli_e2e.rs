//! End-to-end tests for the `asteroidb-cli` binary.
//!
//! Each test spawns a real HTTP server on an ephemeral port and invokes the
//! CLI binary via `tokio::process::Command`, verifying stdout/stderr and exit
//! codes for all major sub-commands (status, get, put, metrics, slo).
//!
//! We use `tokio::process::Command` (not `std::process::Command`) because the
//! server runs as a tokio task in the same runtime. A blocking `Command::output()`
//! would park the worker thread and prevent the server from processing the CLI's
//! HTTP requests, causing a deadlock.

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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a minimal `AppState` suitable for CLI E2E tests.
fn test_state() -> Arc<AppState> {
    let node_id = NodeId("cli-test-node".into());

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
        latency_model: None,
        cluster_nodes: None,
        slo_tracker: Arc::new(asteroidb_poc::ops::slo::SloTracker::new()),
    })
}

/// Spawn an HTTP server on an ephemeral port and return the address.
/// The server task handle is returned so the caller can abort it for cleanup.
///
/// Polls the server until it responds to an HTTP request (up to 5 s)
/// before returning, so the caller can invoke the CLI immediately.
async fn spawn_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let state = test_state();
    let app = router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Poll until the server accepts connections (up to 5 s).
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(resp) = client
            .get(format!("http://{addr}/api/eventual/__ready"))
            .timeout(Duration::from_millis(200))
            .send()
            .await
        {
            if resp.status().is_success() {
                break;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("server at {addr} did not become ready within 5 s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    (addr, handle)
}

/// Return the path to the built `asteroidb-cli` binary.
///
/// We build it once via `cargo build --bin asteroidb-cli` and then reuse the
/// debug binary for all tests. The build is guarded by a `std::sync::Once` to
/// avoid redundant compilations when tests run in parallel.
fn cli_bin() -> std::path::PathBuf {
    static BUILD_ONCE: std::sync::Once = std::sync::Once::new();
    BUILD_ONCE.call_once(|| {
        let status = std::process::Command::new("cargo")
            .args(["build", "--bin", "asteroidb-cli"])
            .status()
            .expect("failed to invoke cargo build");
        assert!(status.success(), "cargo build --bin asteroidb-cli failed");
    });

    // Determine the target directory (respects CARGO_TARGET_DIR).
    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
            std::path::PathBuf::from(manifest_dir).join("target")
        });

    target_dir.join("debug").join("asteroidb-cli")
}

/// Run the CLI with the given sub-command arguments against `host`.
///
/// Uses `tokio::process::Command` so the tokio runtime can continue driving
/// the server task while the CLI process runs.
async fn run_cli(host: &str, args: &[&str]) -> std::process::Output {
    tokio::process::Command::new(cli_bin())
        .arg("--host")
        .arg(host)
        .args(args)
        .output()
        .await
        .expect("failed to execute asteroidb-cli")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cli_put_then_get() {
    let (addr, server) = spawn_server().await;
    let host = format!("127.0.0.1:{}", addr.port());

    // Put a key.
    let out = run_cli(&host, &["put", "greeting", "hello-world"]).await;
    assert!(
        out.status.success(),
        "put should succeed, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("OK"), "put stdout should contain 'OK'");

    // Get the key back.
    let out = run_cli(&host, &["get", "greeting"]).await;
    assert!(
        out.status.success(),
        "get should succeed, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let body: serde_json::Value =
        serde_json::from_str(&stdout).expect("get output should be valid JSON");
    assert_eq!(body["key"], "greeting");
    assert_eq!(body["value"]["value"], "hello-world");

    server.abort();
}

#[tokio::test]
async fn cli_get_nonexistent_key() {
    let (addr, server) = spawn_server().await;
    let host = format!("127.0.0.1:{}", addr.port());

    // Get a key that was never written.
    let out = run_cli(&host, &["get", "no-such-key"]).await;
    assert!(
        out.status.success(),
        "get of nonexistent key should succeed (returns null value), stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let body: serde_json::Value =
        serde_json::from_str(&stdout).expect("get output should be valid JSON");
    assert_eq!(body["key"], "no-such-key");
    // The value should be null for a key that doesn't exist.
    assert!(
        body["value"].is_null(),
        "nonexistent key should have null value, got: {}",
        body["value"]
    );

    server.abort();
}

#[tokio::test]
async fn cli_status_shows_node_info() {
    let (addr, server) = spawn_server().await;
    let host = format!("127.0.0.1:{}", addr.port());

    let out = run_cli(&host, &["status"]).await;
    assert!(
        out.status.success(),
        "status should succeed, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("AsteroidDB Node Status"),
        "status should print header, got: {stdout}"
    );
    // The status command reads /api/metrics and prints key fields.
    assert!(
        stdout.contains("Pending certifications:"),
        "should contain pending certifications label"
    );
    assert!(
        stdout.contains("Certified total:"),
        "should contain certified total label"
    );

    server.abort();
}

#[tokio::test]
async fn cli_metrics_returns_json() {
    let (addr, server) = spawn_server().await;
    let host = format!("127.0.0.1:{}", addr.port());

    let out = run_cli(&host, &["metrics"]).await;
    assert!(
        out.status.success(),
        "metrics should succeed, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let body: serde_json::Value =
        serde_json::from_str(&stdout).expect("metrics output should be valid JSON");
    // RuntimeMetrics::default() produces a snapshot with known fields.
    assert!(
        body.get("pending_count").is_some(),
        "metrics JSON should contain pending_count"
    );
    assert!(
        body.get("certified_total").is_some(),
        "metrics JSON should contain certified_total"
    );

    server.abort();
}

#[tokio::test]
async fn cli_slo_returns_output() {
    let (addr, server) = spawn_server().await;
    let host = format!("127.0.0.1:{}", addr.port());

    let out = run_cli(&host, &["slo"]).await;
    assert!(
        out.status.success(),
        "slo should succeed, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    // The SLO command should print either the table header or JSON output.
    assert!(
        stdout.contains("SLO Budget Status") || stdout.contains("budgets"),
        "slo should print budget info, got: {stdout}"
    );

    server.abort();
}

#[tokio::test]
async fn cli_put_then_get_multiple_keys() {
    let (addr, server) = spawn_server().await;
    let host = format!("127.0.0.1:{}", addr.port());

    // Write two distinct keys.
    let out = run_cli(&host, &["put", "key-a", "value-a"]).await;
    assert!(out.status.success());
    let out = run_cli(&host, &["put", "key-b", "value-b"]).await;
    assert!(out.status.success());

    // Read them back and verify independence.
    let out = run_cli(&host, &["get", "key-a"]).await;
    let body: serde_json::Value =
        serde_json::from_str(&String::from_utf8_lossy(&out.stdout)).unwrap();
    assert_eq!(body["key"], "key-a");
    assert_eq!(body["value"]["value"], "value-a");

    let out = run_cli(&host, &["get", "key-b"]).await;
    let body: serde_json::Value =
        serde_json::from_str(&String::from_utf8_lossy(&out.stdout)).unwrap();
    assert_eq!(body["key"], "key-b");
    assert_eq!(body["value"]["value"], "value-b");

    server.abort();
}

#[tokio::test]
async fn cli_connection_refused_exits_nonzero() {
    // Attempt to connect to a port where nothing is listening.
    let out = run_cli("127.0.0.1:1", &["status"]).await;
    assert!(
        !out.status.success(),
        "connecting to a closed port should exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("Error"),
        "stderr should contain error message, got: {stderr}"
    );
}
