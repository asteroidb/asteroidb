use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::authority::ack_frontier::AckFrontier;
use crate::http::codec::{self, CONTENT_TYPE_BINCODE, deserialize_internal, serialize_internal};

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
    /// Optional Bearer token added to all outbound requests for internal API auth.
    auth_token: Option<String>,
}

impl FrontierSyncClient {
    /// Create a new sync client with default HTTP settings.
    pub fn new() -> Self {
        Self {
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("failed to build FrontierSyncClient HTTP client"),
            auth_token: None,
        }
    }

    /// Create a sync client that attaches a Bearer token to all requests.
    pub fn with_token(token: String) -> Self {
        Self {
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("failed to build FrontierSyncClient HTTP client"),
            auth_token: Some(token),
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

    /// Return a GET request builder with optional Bearer token header.
    fn authorized_get(&self, url: &str) -> reqwest::RequestBuilder {
        let mut builder = self.http_client.get(url);
        if let Some(ref token) = self.auth_token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder
    }

    /// Send a POST request with bincode-encoded body and Accept header.
    ///
    /// Falls back to JSON encoding if bincode serialization fails.
    fn bincode_post<T: Serialize>(
        &self,
        url: &str,
        data: &T,
    ) -> Result<reqwest::RequestBuilder, codec::SerializationError> {
        let (bytes, content_type) = serialize_internal(data, Some(CONTENT_TYPE_BINCODE))?;
        Ok(self
            .authorized_post(url)
            .header("content-type", content_type)
            .header("accept", CONTENT_TYPE_BINCODE)
            .body(bytes))
    }

    /// Build a POST request with JSON-encoded body.
    fn json_post<T: Serialize>(&self, url: &str, data: &T) -> reqwest::RequestBuilder {
        self.authorized_post(url).json(data)
    }

    /// Send a POST request preferring bincode, retrying with JSON if the peer
    /// rejects the bincode request (non-success status).
    ///
    /// This ensures backward compatibility during rolling upgrades where older
    /// nodes may not support bincode payloads.
    async fn send_with_json_fallback<T: Serialize>(
        &self,
        url: &str,
        data: &T,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let req_builder = match self.bincode_post(url, data) {
            Ok(b) => b,
            Err(_) => {
                return self.json_post(url, data).send().await;
            }
        };

        match req_builder.send().await {
            Ok(resp) if !resp.status().is_success() => {
                tracing::debug!(
                    url = %url,
                    status = %resp.status(),
                    "bincode request rejected, retrying with JSON"
                );
                self.json_post(url, data).send().await
            }
            other => other,
        }
    }

    /// Deserialize a response body based on the response's Content-Type header.
    async fn decode_response<T: for<'de> Deserialize<'de>>(
        resp: reqwest::Response,
    ) -> Result<T, String> {
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
        deserialize_internal(&bytes, content_type.as_deref()).map_err(|e| e.to_string())
    }

    /// Push frontier updates to a remote peer.
    ///
    /// Sends a POST request to `http://{peer_addr}/api/internal/frontiers`
    /// with the given frontiers serialised as bincode (with JSON fallback).
    /// The peer will apply each frontier via `AckFrontierSet::update()`,
    /// which handles monotonicity and deduplication.
    ///
    /// Returns the number of frontiers accepted by the peer.
    pub async fn push_frontiers(
        &self,
        peer_addr: &str,
        frontiers: Vec<AckFrontier>,
    ) -> Result<FrontierPushResponse, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("http://{peer_addr}/api/internal/frontiers");
        let body = FrontierPushRequest { frontiers };

        let resp = self
            .send_with_json_fallback(&url, &body)
            .await?
            .error_for_status()?;
        Self::decode_response::<FrontierPushResponse>(resp)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
    }

    /// Pull all frontiers from a remote peer.
    ///
    /// Sends a GET request to `http://{peer_addr}/api/internal/frontiers`
    /// with Accept: application/octet-stream to request bincode responses.
    /// Falls back to JSON if the peer responds with JSON. If the bincode
    /// request is rejected, retries without the bincode Accept header.
    pub async fn pull_frontiers(
        &self,
        peer_addr: &str,
    ) -> Result<FrontierPullResponse, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("http://{peer_addr}/api/internal/frontiers");

        let resp = self
            .authorized_get(&url)
            .header("accept", CONTENT_TYPE_BINCODE)
            .send()
            .await?;

        // If bincode Accept was rejected, retry without it for backward compatibility.
        let resp = if !resp.status().is_success() {
            tracing::debug!(
                url = %url,
                status = %resp.status(),
                "bincode pull_frontiers rejected, retrying without bincode Accept"
            );
            self.authorized_get(&url).send().await?.error_for_status()?
        } else {
            resp
        };

        Self::decode_response::<FrontierPullResponse>(resp)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
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
