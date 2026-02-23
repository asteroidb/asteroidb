use std::sync::Arc;

use tokio::sync::Mutex;

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::runtime::{NodeRunner, NodeRunnerConfig};
use asteroidb_poc::types::{KeyRange, NodeId};

#[tokio::main]
async fn main() {
    let bind_addr =
        std::env::var("ASTEROIDB_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());

    println!("AsteroidDB starting...");

    let node_id = NodeId("node-1".into());

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
    });

    // Build shared HTTP state.
    let state = Arc::new(AppState {
        eventual: Mutex::new(EventualApi::new(node_id.clone())),
        certified: Mutex::new(CertifiedApi::new(node_id.clone(), ns.clone())),
    });

    let app = router(state);

    // Build NodeRunner with its own CertifiedApi for background processing.
    let runner_api = CertifiedApi::new(node_id.clone(), ns);
    let engine = CompactionEngine::with_defaults();
    let mut runner = NodeRunner::new(node_id, runner_api, engine, NodeRunnerConfig::default());
    let shutdown_handle = runner.shutdown_handle();

    // Bind the TCP listener.
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind to {bind_addr}: {e}"));

    println!("HTTP server listening on {bind_addr}");
    println!("Node run loop started. Press Ctrl-C to stop.");

    tokio::select! {
        result = axum::serve(listener, app) => {
            if let Err(e) = result {
                eprintln!("HTTP server error: {e}");
            }
        }
        _stats = runner.run() => {
            println!("NodeRunner exited.");
        }
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutting down...");
            let _ = shutdown_handle.send(true);
        }
    }

    println!("AsteroidDB stopped.");
}
