//! Membership protocol for fan-out join, fan-out leave, and periodic
//! peer list exchange (lightweight gossip via ping).
//!
//! Reduces dependency on the seed node by ensuring all peers learn
//! about membership changes directly.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::http::types::{AnnounceRequest, AnnounceResponse, PeerInfo, PingRequest, PingResponse};
use crate::network::peer::{PeerConfig, PeerRegistry};
use crate::types::NodeId;

/// Number of consecutive ping failures before a peer is automatically evicted.
const MAX_PING_FAILURES: u32 = 3;

/// Client for the membership protocol.
///
/// Provides methods for fan-out announce (join/leave) and periodic
/// peer list exchange (ping). Shares a [`PeerRegistry`] with the
/// HTTP handlers and sync client so that membership changes are
/// immediately visible to all subsystems.
pub struct MembershipClient {
    self_id: NodeId,
    self_addr: String,
    peer_registry: Arc<Mutex<PeerRegistry>>,
    http_client: reqwest::Client,
    /// Optional Bearer token for authenticating internal API requests.
    auth_token: Option<String>,
    /// Consecutive ping failure counts per peer address.
    failed_ping_counts: HashMap<String, u32>,
}

impl MembershipClient {
    /// Create a new `MembershipClient`.
    pub fn new(
        self_id: NodeId,
        self_addr: String,
        peer_registry: Arc<Mutex<PeerRegistry>>,
    ) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("failed to build HTTP client");
        Self {
            self_id,
            self_addr,
            peer_registry,
            http_client,
            auth_token: None,
            failed_ping_counts: HashMap::new(),
        }
    }

    /// Create a `MembershipClient` with Bearer token authentication.
    pub fn with_token(
        self_id: NodeId,
        self_addr: String,
        peer_registry: Arc<Mutex<PeerRegistry>>,
        token: String,
    ) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("failed to build HTTP client");
        Self {
            self_id,
            self_addr,
            peer_registry,
            http_client,
            auth_token: Some(token),
            failed_ping_counts: HashMap::new(),
        }
    }

    /// Return a POST request builder with optional Bearer token header.
    fn authorized_post(&self, url: &str) -> reqwest::RequestBuilder {
        let mut builder = self.http_client.post(url);
        if let Some(ref token) = self.auth_token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder
    }

    /// Fan-out join: announce this node's presence to all known peers.
    ///
    /// Sends `POST /api/internal/announce` with `joining: true` to
    /// every peer in the registry. Failures are logged and skipped.
    ///
    /// Returns the number of peers that accepted the announcement.
    pub async fn fan_out_join(&self) -> usize {
        self.fan_out_announce(true).await
    }

    /// Fan-out leave: announce this node's departure to all known peers.
    ///
    /// Sends `POST /api/internal/announce` with `joining: false` to
    /// every peer in the registry. Failures are logged and skipped.
    ///
    /// Returns the number of peers that accepted the announcement.
    pub async fn fan_out_leave(&self) -> usize {
        self.fan_out_announce(false).await
    }

    /// Send an announce request to all known peers.
    async fn fan_out_announce(&self, joining: bool) -> usize {
        let request = AnnounceRequest {
            node_id: self.self_id.0.clone(),
            address: self.self_addr.clone(),
            joining,
        };

        let peers = self.peer_registry.lock().await.all_peers_owned();
        let mut accepted = 0;

        for peer in &peers {
            let url = format!("http://{}/api/internal/announce", peer.addr);

            match self.authorized_post(&url).json(&request).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        if let Ok(body) = resp.json::<AnnounceResponse>().await
                            && body.accepted
                        {
                            accepted += 1;
                        }
                        tracing::debug!(
                            peer = %peer.node_id.0,
                            joining,
                            "announce sent successfully"
                        );
                    } else {
                        tracing::warn!(
                            peer = %peer.node_id.0,
                            status = %resp.status(),
                            "announce received non-success status"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        peer = %peer.node_id.0,
                        error = %e,
                        "announce request failed"
                    );
                }
            }
        }

        accepted
    }

    /// Exchange peer lists with all known peers via ping.
    ///
    /// Sends `POST /api/internal/ping` to every peer. On response,
    /// reconciles the returned peer list with the local registry,
    /// adding any unknown peers.
    ///
    /// Peers that fail to respond for [`MAX_PING_FAILURES`] consecutive
    /// rounds are automatically evicted from the registry.
    ///
    /// Returns the number of new peers discovered through this exchange.
    pub async fn ping_all(&mut self) -> usize {
        let my_peers = {
            let registry = self.peer_registry.lock().await;
            let mut list: Vec<PeerInfo> = registry
                .all_peers_owned()
                .into_iter()
                .map(|p| PeerInfo {
                    node_id: p.node_id.0,
                    address: p.addr,
                })
                .collect();

            // Include self in the known peers list.
            list.push(PeerInfo {
                node_id: self.self_id.0.clone(),
                address: self.self_addr.clone(),
            });

            list.sort_by(|a, b| a.node_id.cmp(&b.node_id));
            list
        };

        let request = PingRequest {
            sender_id: self.self_id.0.clone(),
            sender_addr: self.self_addr.clone(),
            known_peers: my_peers,
        };

        let peers = self.peer_registry.lock().await.all_peers_owned();
        let mut total_discovered = 0;

        for peer in &peers {
            let url = format!("http://{}/api/internal/ping", peer.addr);
            let peer_key = peer.node_id.0.clone();

            match self.authorized_post(&url).json(&request).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        // Reset failure count on successful response.
                        self.failed_ping_counts.remove(&peer_key);
                        if let Ok(ping_resp) = resp.json::<PingResponse>().await {
                            let discovered = self.reconcile_peers(&ping_resp.known_peers).await;
                            total_discovered += discovered;
                        }
                    } else {
                        tracing::warn!(
                            peer = %peer.node_id.0,
                            status = %resp.status(),
                            "ping received non-success status"
                        );
                        // Treat non-success as a failure for eviction purposes.
                        let count = self.failed_ping_counts.entry(peer_key).or_insert(0);
                        *count += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        peer = %peer.node_id.0,
                        error = %e,
                        "ping request failed"
                    );
                    let count = self.failed_ping_counts.entry(peer_key).or_insert(0);
                    *count += 1;
                }
            }
        }

        // Evict peers that have exceeded the failure threshold.
        let to_evict: Vec<String> = self
            .failed_ping_counts
            .iter()
            .filter(|(_, count)| **count >= MAX_PING_FAILURES)
            .map(|(key, _)| key.clone())
            .collect();

        if !to_evict.is_empty() {
            let mut registry = self.peer_registry.lock().await;
            for peer_key in &to_evict {
                let nid = NodeId(peer_key.clone());
                match registry.remove_peer(&nid) {
                    Ok(Some(_)) => {
                        tracing::info!(
                            peer = %peer_key,
                            "evicted unresponsive peer after {MAX_PING_FAILURES} consecutive ping failures"
                        );
                    }
                    Ok(None) => {
                        // Already removed by another path.
                    }
                    Err(e) => {
                        tracing::warn!(
                            peer = %peer_key,
                            error = %e,
                            "failed to evict unresponsive peer"
                        );
                    }
                }
                self.failed_ping_counts.remove(peer_key);
            }
        }

        total_discovered
    }

    /// Reconcile a received peer list with the local registry.
    ///
    /// Returns the number of newly added peers.
    async fn reconcile_peers(&self, remote_peers: &[PeerInfo]) -> usize {
        let mut registry = self.peer_registry.lock().await;
        let mut added = 0;

        for peer_info in remote_peers {
            let peer_nid = NodeId(peer_info.node_id.clone());
            if registry.get_peer(&peer_nid).is_none()
                && registry
                    .add_peer(PeerConfig {
                        node_id: peer_nid,
                        addr: peer_info.address.clone(),
                    })
                    .is_ok()
            {
                added += 1;
            }
        }

        added
    }

    /// Return a shared reference to the peer registry.
    pub fn peer_registry(&self) -> &Arc<Mutex<PeerRegistry>> {
        &self.peer_registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nid(s: &str) -> NodeId {
        NodeId(s.into())
    }

    #[tokio::test]
    async fn membership_client_creation() {
        let registry = Arc::new(Mutex::new(
            PeerRegistry::new(nid("node-1"), vec![]).unwrap(),
        ));
        let client = MembershipClient::new(nid("node-1"), "127.0.0.1:3000".to_string(), registry);
        assert_eq!(client.self_id, nid("node-1"));
        assert_eq!(client.self_addr, "127.0.0.1:3000");
    }

    #[tokio::test]
    async fn fan_out_join_with_no_peers_returns_zero() {
        let registry = Arc::new(Mutex::new(
            PeerRegistry::new(nid("node-1"), vec![]).unwrap(),
        ));
        let client = MembershipClient::new(nid("node-1"), "127.0.0.1:3000".to_string(), registry);
        let count = client.fan_out_join().await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn fan_out_leave_with_no_peers_returns_zero() {
        let registry = Arc::new(Mutex::new(
            PeerRegistry::new(nid("node-1"), vec![]).unwrap(),
        ));
        let client = MembershipClient::new(nid("node-1"), "127.0.0.1:3000".to_string(), registry);
        let count = client.fan_out_leave().await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn ping_all_with_no_peers_returns_zero() {
        let registry = Arc::new(Mutex::new(
            PeerRegistry::new(nid("node-1"), vec![]).unwrap(),
        ));
        let mut client =
            MembershipClient::new(nid("node-1"), "127.0.0.1:3000".to_string(), registry);
        let count = client.ping_all().await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn ping_all_evicts_unreachable_peers_after_threshold() {
        // Use an address that will fail to connect (port 1 is typically refused).
        let registry = Arc::new(Mutex::new(
            PeerRegistry::new(
                nid("node-1"),
                vec![PeerConfig {
                    node_id: nid("node-2"),
                    addr: "127.0.0.1:1".into(),
                }],
            )
            .unwrap(),
        ));
        let mut client = MembershipClient::new(
            nid("node-1"),
            "127.0.0.1:3000".to_string(),
            Arc::clone(&registry),
        );

        // Peer should still be present before reaching the threshold.
        for _ in 0..(MAX_PING_FAILURES - 1) {
            client.ping_all().await;
        }
        assert_eq!(registry.lock().await.peer_count(), 1);

        // One more failure should trigger eviction.
        client.ping_all().await;
        assert_eq!(registry.lock().await.peer_count(), 0);
    }

    #[tokio::test]
    async fn failed_ping_counts_initialized_empty() {
        let registry = Arc::new(Mutex::new(
            PeerRegistry::new(nid("node-1"), vec![]).unwrap(),
        ));
        let client = MembershipClient::new(nid("node-1"), "127.0.0.1:3000".to_string(), registry);
        assert!(client.failed_ping_counts.is_empty());
    }

    #[tokio::test]
    async fn reconcile_adds_unknown_peers() {
        let registry = Arc::new(Mutex::new(
            PeerRegistry::new(nid("node-1"), vec![]).unwrap(),
        ));
        let client = MembershipClient::new(
            nid("node-1"),
            "127.0.0.1:3000".to_string(),
            Arc::clone(&registry),
        );

        let remote_peers = vec![
            PeerInfo {
                node_id: "node-2".into(),
                address: "127.0.0.1:3001".into(),
            },
            PeerInfo {
                node_id: "node-3".into(),
                address: "127.0.0.1:3002".into(),
            },
        ];

        let added = client.reconcile_peers(&remote_peers).await;
        assert_eq!(added, 2);
        assert_eq!(registry.lock().await.peer_count(), 2);
    }

    #[tokio::test]
    async fn reconcile_ignores_already_known_peers() {
        let registry = Arc::new(Mutex::new(
            PeerRegistry::new(
                nid("node-1"),
                vec![PeerConfig {
                    node_id: nid("node-2"),
                    addr: "127.0.0.1:3001".into(),
                }],
            )
            .unwrap(),
        ));
        let client = MembershipClient::new(
            nid("node-1"),
            "127.0.0.1:3000".to_string(),
            Arc::clone(&registry),
        );

        let remote_peers = vec![PeerInfo {
            node_id: "node-2".into(),
            address: "127.0.0.1:3001".into(),
        }];

        let added = client.reconcile_peers(&remote_peers).await;
        assert_eq!(added, 0);
        assert_eq!(registry.lock().await.peer_count(), 1);
    }

    #[tokio::test]
    async fn reconcile_ignores_self() {
        let registry = Arc::new(Mutex::new(
            PeerRegistry::new(nid("node-1"), vec![]).unwrap(),
        ));
        let client = MembershipClient::new(
            nid("node-1"),
            "127.0.0.1:3000".to_string(),
            Arc::clone(&registry),
        );

        let remote_peers = vec![PeerInfo {
            node_id: "node-1".into(),
            address: "127.0.0.1:3000".into(),
        }];

        let added = client.reconcile_peers(&remote_peers).await;
        assert_eq!(added, 0);
        assert_eq!(registry.lock().await.peer_count(), 0);
    }
}
