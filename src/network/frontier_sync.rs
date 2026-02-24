use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::authority::ack_frontier::AckFrontier;

/// Request body for pushing frontiers to a peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierPushRequest {
    /// The set of frontier updates to apply.
    pub frontiers: Vec<AckFrontier>,
}

/// Response from a frontier push operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierPushResponse {
    /// Number of frontiers that were accepted (advanced the peer's state).
    pub accepted: usize,
}

/// Response from a frontier pull operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierPullResponse {
    /// All frontiers tracked by the peer.
    pub frontiers: Vec<AckFrontier>,
}

/// Client for synchronising `AckFrontier` values with remote peers.
///
/// Uses HTTP POST/GET against internal API endpoints to push local
/// frontiers to peers and pull their frontier state.
///
/// This is the network transport layer for the automatic frontier
/// update pipeline. The actual frontier application (monotonicity,
/// deduplication) is handled by `AckFrontierSet::update()`.
pub struct FrontierSyncClient {
    http_client: reqwest::Client,
}

impl FrontierSyncClient {
    /// Create a new sync client with default HTTP settings.
    pub fn new() -> Self {
        Self {
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("failed to build FrontierSyncClient HTTP client"),
        }
    }

    /// Push frontier updates to a remote peer.
    ///
    /// Sends a POST request to `http://{peer_addr}/api/internal/frontiers`
    /// with the given frontiers serialised as JSON. The peer will apply
    /// each frontier via `AckFrontierSet::update()`, which handles
    /// monotonicity and deduplication.
    ///
    /// Returns the number of frontiers accepted by the peer.
    pub async fn push_frontiers(
        &self,
        peer_addr: SocketAddr,
        frontiers: Vec<AckFrontier>,
    ) -> Result<FrontierPushResponse, reqwest::Error> {
        let url = format!("http://{peer_addr}/api/internal/frontiers");
        let body = FrontierPushRequest { frontiers };

        self.http_client
            .post(&url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json::<FrontierPushResponse>()
            .await
    }

    /// Pull all frontiers from a remote peer.
    ///
    /// Sends a GET request to `http://{peer_addr}/api/internal/frontiers`.
    /// The returned frontiers can be applied locally via `AckFrontierSet::update()`.
    pub async fn pull_frontiers(
        &self,
        peer_addr: SocketAddr,
    ) -> Result<FrontierPullResponse, reqwest::Error> {
        let url = format!("http://{peer_addr}/api/internal/frontiers");

        self.http_client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<FrontierPullResponse>()
            .await
    }
}

impl Default for FrontierSyncClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::HlcTimestamp;
    use crate::types::{KeyRange, NodeId, PolicyVersion};

    fn make_frontier(authority: &str, physical: u64, prefix: &str) -> AckFrontier {
        AckFrontier {
            authority_id: NodeId(authority.into()),
            frontier_hlc: HlcTimestamp {
                physical,
                logical: 0,
                node_id: authority.into(),
            },
            key_range: KeyRange {
                prefix: prefix.into(),
            },
            policy_version: PolicyVersion(1),
            digest_hash: format!("{authority}-{physical}"),
        }
    }

    #[test]
    fn frontier_push_request_serde_roundtrip() {
        let req = FrontierPushRequest {
            frontiers: vec![
                make_frontier("auth-1", 100, "user/"),
                make_frontier("auth-2", 200, "user/"),
            ],
        };

        let json = serde_json::to_string(&req).unwrap();
        let back: FrontierPushRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.frontiers.len(), 2);
        assert_eq!(back.frontiers[0].authority_id, NodeId("auth-1".into()));
        assert_eq!(back.frontiers[1].frontier_hlc.physical, 200);
    }

    #[test]
    fn frontier_push_response_serde_roundtrip() {
        let resp = FrontierPushResponse { accepted: 3 };
        let json = serde_json::to_string(&resp).unwrap();
        let back: FrontierPushResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(back.accepted, 3);
    }

    #[test]
    fn frontier_pull_response_serde_roundtrip() {
        let resp = FrontierPullResponse {
            frontiers: vec![make_frontier("auth-1", 500, "order/")],
        };
        let json = serde_json::to_string(&resp).unwrap();
        let back: FrontierPullResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(back.frontiers.len(), 1);
        assert_eq!(back.frontiers[0].key_range.prefix, "order/");
    }

    #[test]
    fn frontier_push_request_empty_list() {
        let req = FrontierPushRequest { frontiers: vec![] };
        let json = serde_json::to_string(&req).unwrap();
        let back: FrontierPushRequest = serde_json::from_str(&json).unwrap();
        assert!(back.frontiers.is_empty());
    }

    #[test]
    fn sync_client_default_creates_instance() {
        let _client = FrontierSyncClient::default();
        // Just verify it can be constructed without error.
    }
}
