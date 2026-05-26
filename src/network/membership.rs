//! Membership protocol for fan-out join, fan-out leave, and periodic
//! peer list exchange (lightweight gossip via ping).
//!
//! Reduces dependency on the seed node by ensuring all peers learn
//! about membership changes directly.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::http::types::{AnnounceRequest, AnnounceResponse, PeerInfo, PingRequest, PingResponse};
use crate::network::peer::{PeerConfig, PeerRegistry};
use crate::types::NodeId;

/// Check whether a peer address is safe to connect to.
///
/// Addresses are expected in `host:port` form (the format stored in
/// [`PeerInfo::address`]). The function rejects:
///
/// * Link-local IPv4 addresses (`169.254.0.0/16`) — includes the AWS/GCP/Azure
///   instance-metadata endpoint (`169.254.169.254`).
/// * IPv4 loopback (`127.0.0.0/8`).
/// * IPv6 loopback (`::1`).
/// * IPv6 link-local addresses (`fe80::/10`).
///
/// Hostnames that cannot be parsed as IP addresses are allowed through so
/// that legitimate DNS-based cluster addresses work; further validation
/// (e.g. DNS rebinding protection) can be added at the transport layer.
pub(crate) fn is_safe_peer_address(addr: &str) -> bool {
    // Strip an optional trailing path so callers can pass full addresses.
    // PeerInfo addresses are plain "host:port", so we only need to handle
    // the bracketed-IPv6 form for the port-split step.
    let host = if let Some(bracketed) = addr.strip_prefix('[') {
        // IPv6 literal: `[::1]:port` → `::1`
        bracketed
            .split(']')
            .next()
            .unwrap_or(addr)
    } else {
        // IPv4 / hostname: `192.0.2.1:port` or `example.com:port`
        addr.rsplit(':')
            .nth(1) // everything before the last ':'
            .map(|_| addr.rsplitn(2, ':').last().unwrap_or(addr))
            .unwrap_or(addr)
    };

    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(v4)) => {
            let o = v4.octets();
            // 127.0.0.0/8 — loopback
            if o[0] == 127 {
                return false;
            }
            // 169.254.0.0/16 — link-local / cloud metadata endpoints
            if o[0] == 169 && o[1] == 254 {
                return false;
            }
            true
        }
        Ok(IpAddr::V6(v6)) => {
            // ::1 — loopback
            if v6.is_loopback() {
                return false;
            }
            // fe80::/10 — link-local
            let segments = v6.segments();
            if (segments[0] & 0xffc0) == 0xfe80 {
                return false;
            }
            true
        }
        // Not an IP address — hostname; allow and rely on DNS resolution.
        Err(_) => true,
    }
}

/// Number of consecutive ping failures before a peer is automatically evicted.
const MAX_PING_FAILURES: u32 = 3;

/// Per-peer RTT measurement from a successful ping round.
#[derive(Debug, Clone)]
pub struct PeerRtt {
    /// The node ID of the peer.
    pub node_id: NodeId,
    /// Measured round-trip time.
    pub rtt: Duration,
}

/// Result of a `ping_all` round.
#[derive(Debug, Clone, Default)]
pub struct PingAllResult {
    /// Number of new peers discovered during this ping round.
    pub discovered: usize,
    /// Number of peers that responded successfully.
    pub successes: usize,
    /// Number of peers that failed to respond.
    pub failures: usize,
    /// Per-peer RTT measurements for successful pings.
    pub peer_rtts: Vec<PeerRtt>,
}

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
    /// Returns a [`PingAllResult`] containing discovered peers,
    /// per-peer success/failure counts, and RTT measurements for successful pings.
    pub async fn ping_all(&mut self) -> PingAllResult {
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
        let mut stats = PingAllResult::default();
        let mut peer_rtts: Vec<PeerRtt> = Vec::new();

        for peer in &peers {
            let url = format!("http://{}/api/internal/ping", peer.addr);
            let peer_key = peer.node_id.0.clone();
            let ping_start = Instant::now();

            match self.authorized_post(&url).json(&request).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        let rtt = ping_start.elapsed();
                        // Reset failure count on successful response.
                        self.failed_ping_counts.remove(&peer_key);
                        stats.successes += 1;
                        if let Ok(ping_resp) = resp.json::<PingResponse>().await {
                            let discovered = self.reconcile_peers(&ping_resp.known_peers).await;
                            stats.discovered += discovered;
                        }
                        peer_rtts.push(PeerRtt {
                            node_id: peer.node_id.clone(),
                            rtt,
                        });
                    } else {
                        tracing::warn!(
                            peer = %peer.node_id.0,
                            status = %resp.status(),
                            "ping received non-success status"
                        );
                        // Treat non-success as a failure for eviction purposes.
                        let count = self.failed_ping_counts.entry(peer_key).or_insert(0);
                        *count += 1;
                        stats.failures += 1;
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
                    stats.failures += 1;
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

        stats.peer_rtts = peer_rtts;
        stats
    }

    /// Reconcile a received peer list with the local registry.
    ///
    /// Updates addresses of known peers if changed, and adds unknown peers
    /// up to a limit of 10 per call to prevent peer-list poisoning.
    ///
    /// Returns the number of newly added peers.
    async fn reconcile_peers(&self, remote_peers: &[PeerInfo]) -> usize {
        const MAX_NEW_PEERS: usize = 10;
        let mut registry = self.peer_registry.lock().await;
        let mut added = 0;

        for peer_info in remote_peers {
            // Reject addresses that could redirect internal HTTP requests to
            // cloud metadata endpoints or other dangerous targets (SSRF).
            if !is_safe_peer_address(&peer_info.address) {
                tracing::warn!(
                    peer = %peer_info.node_id,
                    address = %peer_info.address,
                    "rejecting peer with unsafe address (possible SSRF attempt)"
                );
                continue;
            }

            let peer_nid = NodeId(peer_info.node_id.clone());
            if registry.get_peer(&peer_nid).is_some() {
                // Update address if it changed (e.g. peer restarted with new IP).
                // Re-validate the new address before accepting the update.
                registry.update_address(&peer_nid, &peer_info.address);
            } else if added < MAX_NEW_PEERS
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

    // ── is_safe_peer_address ────────────────────────────────────────────────

    #[test]
    fn safe_address_allows_public_ipv4() {
        assert!(is_safe_peer_address("203.0.113.10:4000"));
    }

    #[test]
    fn safe_address_allows_hostname() {
        assert!(is_safe_peer_address("peer.example.com:4000"));
    }

    #[test]
    fn safe_address_blocks_link_local_metadata_endpoint() {
        // AWS / GCP / Azure instance-metadata service
        assert!(!is_safe_peer_address("169.254.169.254:80"));
    }

    #[test]
    fn safe_address_blocks_link_local_ipv4_range() {
        assert!(!is_safe_peer_address("169.254.0.1:4000"));
        assert!(!is_safe_peer_address("169.254.255.255:4000"));
    }

    #[test]
    fn safe_address_blocks_ipv4_loopback() {
        assert!(!is_safe_peer_address("127.0.0.1:4000"));
        assert!(!is_safe_peer_address("127.1.2.3:4000"));
    }

    #[test]
    fn safe_address_blocks_ipv6_loopback() {
        assert!(!is_safe_peer_address("[::1]:4000"));
    }

    #[test]
    fn safe_address_blocks_ipv6_link_local() {
        assert!(!is_safe_peer_address("[fe80::1]:4000"));
        assert!(!is_safe_peer_address("[fe80::dead:beef]:4000"));
    }

    #[test]
    fn safe_address_allows_public_ipv6() {
        assert!(is_safe_peer_address("[2001:db8::1]:4000"));
    }

    #[tokio::test]
    async fn reconcile_rejects_link_local_metadata_address() {
        let registry = Arc::new(Mutex::new(
            PeerRegistry::new(nid("node-1"), vec![]).unwrap(),
        ));
        let client = MembershipClient::new(
            nid("node-1"),
            "10.0.0.1:3000".to_string(),
            Arc::clone(&registry),
        );

        let remote_peers = vec![
            PeerInfo {
                node_id: "attacker".into(),
                // AWS metadata endpoint — must be rejected
                address: "169.254.169.254:80".into(),
            },
            PeerInfo {
                node_id: "node-2".into(),
                address: "10.0.0.2:3001".into(),
            },
        ];

        let added = client.reconcile_peers(&remote_peers).await;
        // Only the legitimate peer should be added
        assert_eq!(added, 1);
        assert_eq!(registry.lock().await.peer_count(), 1);
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
        let result = client.ping_all().await;
        assert_eq!(result.discovered, 0);
        assert_eq!(result.successes, 0);
        assert_eq!(result.failures, 0);
        assert!(result.peer_rtts.is_empty());
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
