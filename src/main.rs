use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::runtime::{NodeRunner, NodeRunnerConfig};
use asteroidb_poc::types::{KeyRange, NodeId};

#[tokio::main]
async fn main() {
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

    let api = CertifiedApi::new(node_id.clone(), ns);
    let engine = CompactionEngine::with_defaults();

    let mut runner = NodeRunner::new(node_id, api, engine, NodeRunnerConfig::default());

    println!("Node run loop started. Press Ctrl-C to stop.");
    let stats = runner.run_with_signal().await;
    println!("Node stopped. Stats: {stats:?}");
}
