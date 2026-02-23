use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::network::peer::PeerRegistry;
use crate::store::kv::CrdtValue;

/// Bulk sync request payload sent to `POST /api/internal/sync`.
///
/// Contains a map of key -> serialised CRDT value that the receiving
/// node should merge into its local eventual store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRequest {
    /// The node ID of the sender, for logging/debugging.
    pub sender: String,
    /// Key-value pairs to merge. Values are JSON-serialised `CrdtValue`.
    pub entries: HashMap<String, CrdtValue>,
}

/// Response from `POST /api/internal/sync`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResponse {
    /// Number of keys successfully merged.
    pub merged: usize,
    /// Keys that failed to merge (e.g. type mismatch), with error messages.
    pub errors: Vec<SyncError>,
}

/// A single key-level sync error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncError {
    pub key: String,
    pub error: String,
}

/// Response from `GET /api/internal/keys`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyDumpResponse {
    pub entries: HashMap<String, CrdtValue>,
}

/// Anti-entropy sync client.
///
/// Periodically pushes all local CRDT values to every known peer.
/// Uses HTTP POST to `/api/internal/sync` on each peer.
pub struct SyncClient {
    peer_registry: PeerRegistry,
    http_client: reqwest::Client,
}

impl SyncClient {
    /// Create a new `SyncClient` for the given peer registry.
    pub fn new(peer_registry: PeerRegistry) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("failed to build HTTP client");
        Self {
            peer_registry,
            http_client,
        }
    }

    /// Create a `SyncClient` with a custom reqwest client (for testing).
    pub fn with_client(peer_registry: PeerRegistry, http_client: reqwest::Client) -> Self {
        Self {
            peer_registry,
            http_client,
        }
    }

    /// Push all key-value pairs from the local store to every peer.
    ///
    /// For each peer, sends a `POST /api/internal/sync` request with
    /// the full set of local entries. Failures are logged and skipped;
    /// the next sync cycle will retry.
    ///
    /// Returns the number of peers that were successfully synced.
    pub async fn push_all_keys(
        &self,
        entries: HashMap<String, CrdtValue>,
        sender_id: &str,
    ) -> usize {
        if entries.is_empty() {
            return 0;
        }

        let request = SyncRequest {
            sender: sender_id.to_string(),
            entries,
        };

        let mut success_count = 0;

        for peer in self.peer_registry.all_peers() {
            let url = format!("http://{}/api/internal/sync", peer.addr);

            match self.http_client.post(&url).json(&request).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        success_count += 1;
                        tracing::debug!(
                            peer = %peer.node_id.0,
                            "anti-entropy push succeeded"
                        );
                    } else {
                        tracing::warn!(
                            peer = %peer.node_id.0,
                            status = %resp.status(),
                            "anti-entropy push received non-success status"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        peer = %peer.node_id.0,
                        error = %e,
                        "anti-entropy push failed"
                    );
                }
            }
        }

        success_count
    }

    /// Pull all key-value pairs from a specific peer.
    ///
    /// Sends `GET /api/internal/keys` to the peer and returns the
    /// entries map. Returns `None` on failure.
    pub async fn pull_all_keys(
        &self,
        peer_addr: &std::net::SocketAddr,
    ) -> Option<HashMap<String, CrdtValue>> {
        let url = format!("http://{}/api/internal/keys", peer_addr);

        match self.http_client.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<KeyDumpResponse>().await {
                        Ok(dump) => Some(dump.entries),
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "failed to parse key dump response"
                            );
                            None
                        }
                    }
                } else {
                    tracing::warn!(
                        status = %resp.status(),
                        "pull_all_keys received non-success status"
                    );
                    None
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "pull_all_keys request failed"
                );
                None
            }
        }
    }

    /// Return a reference to the peer registry.
    pub fn peer_registry(&self) -> &PeerRegistry {
        &self.peer_registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::pn_counter::PnCounter;
    use crate::network::peer::{PeerConfig, PeerRegistry};
    use crate::types::NodeId;

    fn nid(s: &str) -> NodeId {
        NodeId(s.into())
    }

    #[test]
    fn sync_request_serde_roundtrip() {
        let mut entries = HashMap::new();
        let mut counter = PnCounter::new();
        counter.increment(&nid("node-1"));
        entries.insert("key1".to_string(), CrdtValue::Counter(counter));

        let req = SyncRequest {
            sender: "node-1".to_string(),
            entries,
        };

        let json = serde_json::to_string(&req).unwrap();
        let deserialized: SyncRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.sender, "node-1");
        assert!(deserialized.entries.contains_key("key1"));
    }

    #[test]
    fn sync_response_serde_roundtrip() {
        let resp = SyncResponse {
            merged: 3,
            errors: vec![SyncError {
                key: "bad-key".into(),
                error: "type mismatch".into(),
            }],
        };

        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: SyncResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.merged, 3);
        assert_eq!(deserialized.errors.len(), 1);
        assert_eq!(deserialized.errors[0].key, "bad-key");
    }

    #[test]
    fn key_dump_response_serde_roundtrip() {
        let mut entries = HashMap::new();
        let mut counter = PnCounter::new();
        counter.increment(&nid("node-1"));
        entries.insert("hits".to_string(), CrdtValue::Counter(counter));

        let resp = KeyDumpResponse { entries };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: KeyDumpResponse = serde_json::from_str(&json).unwrap();

        assert!(deserialized.entries.contains_key("hits"));
    }

    #[test]
    fn sync_client_creation() {
        let registry = PeerRegistry::new(
            nid("node-1"),
            vec![PeerConfig {
                node_id: nid("node-2"),
                addr: "127.0.0.1:8001".parse().unwrap(),
            }],
        )
        .unwrap();

        let client = SyncClient::new(registry);
        assert_eq!(client.peer_registry().peer_count(), 1);
    }

    #[tokio::test]
    async fn push_all_keys_empty_entries_returns_zero() {
        let registry = PeerRegistry::new(
            nid("node-1"),
            vec![PeerConfig {
                node_id: nid("node-2"),
                addr: "127.0.0.1:8001".parse().unwrap(),
            }],
        )
        .unwrap();

        let client = SyncClient::new(registry);
        let result = client.push_all_keys(HashMap::new(), "node-1").await;
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn push_all_keys_to_unreachable_peer_returns_zero() {
        let registry = PeerRegistry::new(
            nid("node-1"),
            vec![PeerConfig {
                node_id: nid("node-2"),
                // Unreachable address.
                addr: "127.0.0.1:1".parse().unwrap(),
            }],
        )
        .unwrap();

        // Use a short timeout to speed up the test.
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let client = SyncClient::with_client(registry, http_client);

        let mut entries = HashMap::new();
        let mut counter = PnCounter::new();
        counter.increment(&nid("node-1"));
        entries.insert("key1".to_string(), CrdtValue::Counter(counter));

        let result = client.push_all_keys(entries, "node-1").await;
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn pull_all_keys_from_unreachable_peer_returns_none() {
        let registry = PeerRegistry::new(nid("node-1"), vec![]).unwrap();

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let client = SyncClient::with_client(registry, http_client);

        let addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
        let result = client.pull_all_keys(&addr).await;
        assert!(result.is_none());
    }
}
