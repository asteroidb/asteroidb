use std::sync::{Arc, RwLock};

use tokio::sync::Mutex;

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::network::sync::SyncClient;
use asteroidb_poc::network::{NodeConfig, PeerRegistry};
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::runtime::{NodeRunner, NodeRunnerConfig};
use asteroidb_poc::types::{KeyRange, NodeId};

#[tokio::main]
async fn main() {
    // Load configuration: either from a config file or from individual env vars.
    let (node_id, bind_addr, peer_registry) = match std::env::var("ASTEROIDB_CONFIG") {
        Ok(config_path) => match NodeConfig::load(&config_path) {
            Ok(config) => {
                let node_id = config.node.id;
                let bind_addr = config.bind_addr.to_string();
                let peer_registry = config.peers;
                (node_id, bind_addr, Some(peer_registry))
            }
            Err(e) => {
                eprintln!("error: failed to load config file '{config_path}': {e}");
                std::process::exit(1);
            }
        },
        Err(_) => {
            let bind_addr =
                std::env::var("ASTEROIDB_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
            let node_id_str =
                std::env::var("ASTEROIDB_NODE_ID").unwrap_or_else(|_| "node-1".into());
            let node_id = NodeId(node_id_str);
            (node_id, bind_addr, None)
        }
    };

    println!("AsteroidDB starting... (node_id={})", node_id.0);

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

    let namespace = Arc::new(RwLock::new(ns));

    // Build shared runtime metrics.
    let metrics = Arc::new(RuntimeMetrics::default());

    // Share a single CertifiedApi between HTTP handlers and NodeRunner
    // so that certification status updates are visible to both.
    let certified_api = Arc::new(Mutex::new(CertifiedApi::new(
        node_id.clone(),
        Arc::clone(&namespace),
    )));

    // Share a single EventualApi between HTTP handlers and NodeRunner
    // so that HTTP writes are visible to the anti-entropy sync loop.
    let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id.clone())));

    // Build peer registry: from config file or empty (nodes join dynamically
    // via POST /api/internal/join).
    let has_peers = peer_registry.as_ref().is_some_and(|r| r.peer_count() > 0);
    let shared_peers = Arc::new(Mutex::new(peer_registry.unwrap_or_else(|| {
        PeerRegistry::new(node_id.clone(), vec![]).expect("empty peer list is always valid")
    })));

    // Build shared HTTP state.
    let state = Arc::new(AppState {
        eventual: Arc::clone(&eventual_api),
        certified: Arc::clone(&certified_api),
        namespace: Arc::clone(&namespace),
        metrics: Arc::clone(&metrics),
        peers: Some(Arc::clone(&shared_peers)),
    });

    let app = router(state);

    // NodeRunner uses the same CertifiedApi and EventualApi instances
    // for background processing, ensuring sync sees HTTP writes.
    let engine = CompactionEngine::with_defaults();
    let mut runner = if has_peers {
        // Config file provided peers — enable anti-entropy sync.
        let sync_registry = shared_peers.lock().await.clone();
        let sync_client = SyncClient::new(sync_registry);
        NodeRunner::with_sync(
            node_id,
            Arc::clone(&certified_api),
            engine,
            NodeRunnerConfig::default(),
            sync_client,
            Arc::clone(&eventual_api),
            Arc::clone(&metrics),
        )
        .await
    } else {
        let mut r = NodeRunner::new(
            node_id,
            Arc::clone(&certified_api),
            engine,
            NodeRunnerConfig::default(),
            Arc::clone(&metrics),
        )
        .await;
        r.set_eventual_api(eventual_api);
        r
    };
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
