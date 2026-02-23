use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::node::Node;
use crate::types::NodeId;

/// Network-level error type for peer configuration and registry operations.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum PeerError {
    /// Two or more peers share the same [`NodeId`].
    #[error("duplicate node_id: {0}")]
    DuplicateNodeId(String),

    /// A peer address could not be parsed as a valid [`SocketAddr`].
    #[error("invalid address for node {node_id}: {reason}")]
    InvalidAddress { node_id: String, reason: String },

    /// The local node's own ID appears in the peer list.
    #[error("self_id {0} must not appear in the peer list")]
    SelfInPeerList(String),

    /// An I/O error occurred while loading or saving configuration.
    #[error("io error: {0}")]
    Io(String),

    /// A JSON (de)serialisation error occurred.
    #[error("json error: {0}")]
    Json(String),
}

/// Connection information for a single remote peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerConfig {
    /// Unique identifier of the remote node.
    pub node_id: NodeId,
    /// Socket address (ip:port) the remote node is listening on.
    pub addr: SocketAddr,
}

/// Registry that holds validated peer configurations.
///
/// `PeerRegistry` enforces the following invariants at construction time:
/// - No duplicate `node_id` values.
/// - The local node's own ID (`self_id`) must not be present.
///
/// Once constructed, the registry is immutable and can be queried by node ID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRegistry {
    self_id: NodeId,
    peers: HashMap<NodeId, PeerConfig>,
}

impl PeerRegistry {
    /// Create a new `PeerRegistry`, validating the given peer list.
    ///
    /// # Errors
    ///
    /// Returns [`PeerError::DuplicateNodeId`] if two peers share the same ID,
    /// or [`PeerError::SelfInPeerList`] if `self_id` appears in `peers`.
    pub fn new(self_id: NodeId, peers: Vec<PeerConfig>) -> Result<Self, PeerError> {
        let mut map = HashMap::with_capacity(peers.len());

        for peer in peers {
            if peer.node_id == self_id {
                return Err(PeerError::SelfInPeerList(self_id.0.clone()));
            }
            if map.contains_key(&peer.node_id) {
                return Err(PeerError::DuplicateNodeId(peer.node_id.0.clone()));
            }
            map.insert(peer.node_id.clone(), peer);
        }

        Ok(Self {
            self_id,
            peers: map,
        })
    }

    /// Look up a single peer by its node ID.
    pub fn get_peer(&self, id: &NodeId) -> Option<&PeerConfig> {
        self.peers.get(id)
    }

    /// Return all registered peers as a slice-like iterator.
    pub fn all_peers(&self) -> Vec<&PeerConfig> {
        self.peers.values().collect()
    }

    /// Return the number of registered peers.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Return the local node's own ID.
    pub fn self_id(&self) -> &NodeId {
        &self.self_id
    }
}

/// Top-level configuration for a node, combining the node definition, its
/// bind address, and the peer registry.
///
/// Supports JSON serialisation for file-based configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// The local node definition (identity, mode, tags).
    pub node: Node,
    /// Socket address this node listens on.
    pub bind_addr: SocketAddr,
    /// Registry of known remote peers.
    pub peers: PeerRegistry,
}

impl NodeConfig {
    /// Load a `NodeConfig` from a JSON file at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`PeerError::Io`] on file-system errors and [`PeerError::Json`]
    /// if the file contents are not valid JSON.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, PeerError> {
        let data = std::fs::read_to_string(path).map_err(|e| PeerError::Io(e.to_string()))?;
        serde_json::from_str(&data).map_err(|e| PeerError::Json(e.to_string()))
    }

    /// Save this `NodeConfig` to a JSON file at `path`.
    ///
    /// Creates the file (and any missing parent directories) if it does not
    /// already exist.
    ///
    /// # Errors
    ///
    /// Returns [`PeerError::Io`] on file-system errors and [`PeerError::Json`]
    /// on serialisation errors (should not happen for well-formed configs).
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), PeerError> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent).map_err(|e| PeerError::Io(e.to_string()))?;
        }
        let json =
            serde_json::to_string_pretty(self).map_err(|e| PeerError::Json(e.to_string()))?;
        std::fs::write(path, json).map_err(|e| PeerError::Io(e.to_string()))
    }
}

/// Generate a set of `NodeConfig` values for a cluster of `count` nodes,
/// all on localhost with sequential ports starting at `base_port`.
///
/// Each node is configured with every *other* node as a peer, enabling
/// full-mesh connectivity. This is intended for local development and
/// testing scenarios.
pub fn generate_cluster_configs(count: usize, base_port: u16) -> Vec<NodeConfig> {
    use crate::types::NodeMode;

    // Pre-build the (node_id, addr) pairs.
    let entries: Vec<(NodeId, SocketAddr)> = (0..count)
        .map(|i| {
            let id = NodeId(format!("node-{}", i + 1));
            let addr: SocketAddr = format!("127.0.0.1:{}", base_port + i as u16)
                .parse()
                .expect("valid socket addr");
            (id, addr)
        })
        .collect();

    entries
        .iter()
        .map(|(self_id, bind_addr)| {
            let peers: Vec<PeerConfig> = entries
                .iter()
                .filter(|(id, _)| id != self_id)
                .map(|(id, addr)| PeerConfig {
                    node_id: id.clone(),
                    addr: *addr,
                })
                .collect();

            let registry =
                PeerRegistry::new(self_id.clone(), peers).expect("generated configs must be valid");

            NodeConfig {
                node: Node::new(self_id.clone(), NodeMode::Both),
                bind_addr: *bind_addr,
                peers: registry,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NodeId, NodeMode};

    fn nid(s: &str) -> NodeId {
        NodeId(s.into())
    }

    fn peer(id: &str, addr: &str) -> PeerConfig {
        PeerConfig {
            node_id: nid(id),
            addr: addr.parse().unwrap(),
        }
    }

    // ---- PeerConfig serde ----

    #[test]
    fn peer_config_serde_roundtrip() {
        let pc = peer("node-2", "127.0.0.1:8001");
        let json = serde_json::to_string(&pc).unwrap();
        let back: PeerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(pc, back);
    }

    // ---- PeerRegistry construction ----

    #[test]
    fn registry_with_valid_peers() {
        let reg = PeerRegistry::new(
            nid("node-1"),
            vec![
                peer("node-2", "127.0.0.1:8001"),
                peer("node-3", "127.0.0.1:8002"),
            ],
        )
        .unwrap();

        assert_eq!(reg.peer_count(), 2);
        assert!(reg.get_peer(&nid("node-2")).is_some());
        assert!(reg.get_peer(&nid("node-3")).is_some());
        assert!(reg.get_peer(&nid("node-1")).is_none()); // self excluded
        assert_eq!(reg.self_id(), &nid("node-1"));
    }

    #[test]
    fn registry_empty_peer_list_is_ok() {
        let reg = PeerRegistry::new(nid("solo"), vec![]).unwrap();
        assert_eq!(reg.peer_count(), 0);
        assert!(reg.all_peers().is_empty());
    }

    #[test]
    fn registry_rejects_duplicate_node_id() {
        let result = PeerRegistry::new(
            nid("node-1"),
            vec![
                peer("node-2", "127.0.0.1:8001"),
                peer("node-2", "127.0.0.1:8002"),
            ],
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            PeerError::DuplicateNodeId(id) => assert_eq!(id, "node-2"),
            other => panic!("expected DuplicateNodeId, got {other:?}"),
        }
    }

    #[test]
    fn registry_rejects_self_in_peer_list() {
        let result = PeerRegistry::new(
            nid("node-1"),
            vec![
                peer("node-1", "127.0.0.1:8000"),
                peer("node-2", "127.0.0.1:8001"),
            ],
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            PeerError::SelfInPeerList(id) => assert_eq!(id, "node-1"),
            other => panic!("expected SelfInPeerList, got {other:?}"),
        }
    }

    #[test]
    fn registry_all_peers_returns_all() {
        let reg = PeerRegistry::new(
            nid("node-1"),
            vec![
                peer("node-2", "127.0.0.1:8001"),
                peer("node-3", "127.0.0.1:8002"),
                peer("node-4", "127.0.0.1:8003"),
            ],
        )
        .unwrap();

        let all = reg.all_peers();
        assert_eq!(all.len(), 3);

        let ids: Vec<&str> = {
            let mut v: Vec<_> = all.iter().map(|p| p.node_id.0.as_str()).collect();
            v.sort();
            v
        };
        assert_eq!(ids, vec!["node-2", "node-3", "node-4"]);
    }

    #[test]
    fn registry_serde_roundtrip() {
        let reg = PeerRegistry::new(
            nid("node-1"),
            vec![
                peer("node-2", "127.0.0.1:8001"),
                peer("node-3", "127.0.0.1:8002"),
            ],
        )
        .unwrap();

        let json = serde_json::to_string(&reg).unwrap();
        let back: PeerRegistry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.peer_count(), 2);
        assert_eq!(back.self_id(), &nid("node-1"));
    }

    // ---- NodeConfig load/save ----

    #[test]
    fn node_config_save_and_load_roundtrip() {
        let reg = PeerRegistry::new(nid("node-1"), vec![peer("node-2", "127.0.0.1:8001")]).unwrap();

        let config = NodeConfig {
            node: Node::new(nid("node-1"), NodeMode::Store),
            bind_addr: "127.0.0.1:8000".parse().unwrap(),
            peers: reg,
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.json");

        config.save(&path).unwrap();
        let loaded = NodeConfig::load(&path).unwrap();

        assert_eq!(loaded.node.id, nid("node-1"));
        assert_eq!(loaded.node.mode, NodeMode::Store);
        assert_eq!(
            loaded.bind_addr,
            "127.0.0.1:8000".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(loaded.peers.peer_count(), 1);
        assert!(loaded.peers.get_peer(&nid("node-2")).is_some());
    }

    #[test]
    fn node_config_load_nonexistent_file() {
        let result = NodeConfig::load("/nonexistent/path/node.json");
        assert!(result.is_err());
        match result.unwrap_err() {
            PeerError::Io(_) => {}
            other => panic!("expected Io error, got {other:?}"),
        }
    }

    #[test]
    fn node_config_load_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        std::fs::write(&path, "not json").unwrap();

        let result = NodeConfig::load(&path);
        assert!(result.is_err());
        match result.unwrap_err() {
            PeerError::Json(_) => {}
            other => panic!("expected Json error, got {other:?}"),
        }
    }

    // ---- generate_cluster_configs ----

    #[test]
    fn generate_three_node_cluster() {
        let configs = generate_cluster_configs(3, 9000);

        assert_eq!(configs.len(), 3);

        // Each node should have exactly 2 peers (all other nodes).
        for cfg in &configs {
            assert_eq!(cfg.peers.peer_count(), 2);
        }

        // Verify bind addresses are sequential.
        assert_eq!(
            configs[0].bind_addr,
            "127.0.0.1:9000".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            configs[1].bind_addr,
            "127.0.0.1:9001".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            configs[2].bind_addr,
            "127.0.0.1:9002".parse::<SocketAddr>().unwrap()
        );

        // Verify node IDs.
        assert_eq!(configs[0].node.id, nid("node-1"));
        assert_eq!(configs[1].node.id, nid("node-2"));
        assert_eq!(configs[2].node.id, nid("node-3"));

        // Each node references the other two as peers.
        assert!(configs[0].peers.get_peer(&nid("node-2")).is_some());
        assert!(configs[0].peers.get_peer(&nid("node-3")).is_some());
        assert!(configs[1].peers.get_peer(&nid("node-1")).is_some());
        assert!(configs[1].peers.get_peer(&nid("node-3")).is_some());
        assert!(configs[2].peers.get_peer(&nid("node-1")).is_some());
        assert!(configs[2].peers.get_peer(&nid("node-2")).is_some());

        // Cross-reference: node-1's peer entry for node-2 should have the
        // same addr as node-2's bind_addr.
        let p12 = configs[0].peers.get_peer(&nid("node-2")).unwrap();
        assert_eq!(p12.addr, configs[1].bind_addr);

        let p13 = configs[0].peers.get_peer(&nid("node-3")).unwrap();
        assert_eq!(p13.addr, configs[2].bind_addr);
    }

    #[test]
    fn generate_single_node_cluster() {
        let configs = generate_cluster_configs(1, 9000);
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].peers.peer_count(), 0);
    }

    #[test]
    fn generate_cluster_configs_save_and_reload() {
        let configs = generate_cluster_configs(3, 10000);
        let dir = tempfile::tempdir().unwrap();

        // Save all configs.
        for (i, cfg) in configs.iter().enumerate() {
            let path = dir.path().join(format!("node-{}.json", i + 1));
            cfg.save(&path).unwrap();
        }

        // Reload and verify.
        for (i, original) in configs.iter().enumerate() {
            let path = dir.path().join(format!("node-{}.json", i + 1));
            let loaded = NodeConfig::load(&path).unwrap();
            assert_eq!(loaded.node.id, original.node.id);
            assert_eq!(loaded.bind_addr, original.bind_addr);
            assert_eq!(loaded.peers.peer_count(), original.peers.peer_count());
        }
    }

    // ---- PeerError Display ----

    #[test]
    fn peer_error_display() {
        let err = PeerError::DuplicateNodeId("node-x".into());
        assert_eq!(err.to_string(), "duplicate node_id: node-x");

        let err = PeerError::InvalidAddress {
            node_id: "node-y".into(),
            reason: "bad port".into(),
        };
        assert_eq!(err.to_string(), "invalid address for node node-y: bad port");

        let err = PeerError::SelfInPeerList("node-z".into());
        assert_eq!(
            err.to_string(),
            "self_id node-z must not appear in the peer list"
        );

        let err = PeerError::Io("file not found".into());
        assert_eq!(err.to_string(), "io error: file not found");

        let err = PeerError::Json("bad json".into());
        assert_eq!(err.to_string(), "json error: bad json");
    }
}
