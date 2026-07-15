//! HTTP transport for control-plane Raft RPCs.
//!
//! Endpoints (`POST`):
//! - `/api/internal/raft/vote`
//! - `/api/internal/raft/append`
//! - `/api/internal/raft/snapshot`
//!
//! Follows the internal-endpoint conventions: Bearer internal token when
//! configured, bincode body with JSON fallback for rolling upgrades (an old
//! node answers 404 for these routes — treated as "unreachable", so the
//! round is skipped and consensus simply waits; safe side).
//!
//! Address resolution: the static `ASTEROIDB_RAFT_PEERS` map
//! (`id=host:port,...`) takes precedence; unmapped IDs fall back to the
//! dynamic `PeerRegistry` (gossip). The VOTER SET is never derived from the
//! registry — only addresses are.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::control_plane::raft::transport::{RaftTransport, TransportError, TransportFuture};
use crate::control_plane::raft::types::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use crate::http::codec::{self, CONTENT_TYPE_BINCODE, serialize_internal};
use crate::types::NodeId;

use super::PeerRegistry;

/// Default per-RPC timeout, matching the membership client's HTTP timeout.
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

pub struct HttpRaftTransport {
    client: reqwest::Client,
    auth_token: Option<String>,
    /// Static `node id -> host:port` map (`ASTEROIDB_RAFT_PEERS`).
    static_peers: HashMap<String, String>,
    /// Dynamic fallback for address resolution only.
    registry: Option<Arc<tokio::sync::Mutex<PeerRegistry>>>,
}

impl HttpRaftTransport {
    pub fn new(
        static_peers: HashMap<String, String>,
        registry: Option<Arc<tokio::sync::Mutex<PeerRegistry>>>,
        auth_token: Option<String>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .expect("failed to build HTTP client for raft transport");
        Self {
            client,
            auth_token,
            static_peers,
            registry,
        }
    }

    /// Parse the `ASTEROIDB_RAFT_PEERS` format: comma-separated
    /// `id=host:port` entries. Malformed entries are logged and skipped.
    pub fn parse_static_peers(raw: &str) -> HashMap<String, String> {
        let mut map = HashMap::new();
        for entry in raw.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            match entry.split_once('=') {
                Some((id, addr)) if !id.trim().is_empty() && !addr.trim().is_empty() => {
                    map.insert(id.trim().to_string(), addr.trim().to_string());
                }
                _ => {
                    tracing::warn!(
                        entry,
                        "malformed ASTEROIDB_RAFT_PEERS entry (expected id=host:port); skipping"
                    );
                }
            }
        }
        map
    }

    async fn resolve(&self, id: &NodeId) -> Option<String> {
        if let Some(addr) = self.static_peers.get(&id.0) {
            return Some(addr.clone());
        }
        if let Some(registry) = &self.registry {
            return registry.lock().await.get_peer(id).map(|p| p.addr.clone());
        }
        None
    }

    fn authorized_post(&self, url: &str) -> reqwest::RequestBuilder {
        let mut builder = self.client.post(url);
        if let Some(token) = &self.auth_token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder
    }

    /// POST bincode first; retry as JSON when the peer rejects the bincode
    /// payload (rolling-upgrade compatibility, mirroring
    /// `SyncClient::send_with_json_fallback`).
    async fn post_internal<Req, Resp>(
        &self,
        to: &NodeId,
        path: &str,
        req: &Req,
    ) -> Result<Resp, TransportError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let addr = self
            .resolve(to)
            .await
            .ok_or_else(|| TransportError(format!("no address known for raft peer {}", to.0)))?;
        let url = format!("http://{addr}{path}");

        let first_attempt = match serialize_internal(req, Some(CONTENT_TYPE_BINCODE)) {
            Ok((bytes, content_type)) => Some(
                self.authorized_post(&url)
                    .header("content-type", content_type)
                    .header("accept", CONTENT_TYPE_BINCODE)
                    .body(bytes),
            ),
            Err(_) => None,
        };

        let response = match first_attempt {
            Some(builder) => match builder.send().await {
                Ok(resp) if !resp.status().is_success() => {
                    let status = resp.status();
                    tracing::debug!(
                        url = %url,
                        status = %status,
                        "raft bincode request rejected, retrying with JSON"
                    );
                    self.authorized_post(&url)
                        .json(req)
                        .send()
                        .await
                        .map_err(|e| TransportError(e.to_string()))?
                }
                Ok(resp) => resp,
                Err(e) => return Err(TransportError(e.to_string())),
            },
            None => self
                .authorized_post(&url)
                .json(req)
                .send()
                .await
                .map_err(|e| TransportError(e.to_string()))?,
        };

        if !response.status().is_success() {
            // Includes 404 from pre-raft nodes during rolling upgrades:
            // treated as unreachable (retried next tick, safe side).
            return Err(TransportError(format!(
                "raft rpc to {} failed with status {}",
                to.0,
                response.status()
            )));
        }

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let bytes = response
            .bytes()
            .await
            .map_err(|e| TransportError(e.to_string()))?;
        codec::deserialize_internal(&bytes, content_type.as_deref())
            .map_err(|e| TransportError(e.to_string()))
    }
}

impl RaftTransport for HttpRaftTransport {
    fn request_vote(
        &self,
        to: NodeId,
        req: RequestVoteRequest,
    ) -> TransportFuture<'_, RequestVoteResponse> {
        Box::pin(async move {
            self.post_internal(&to, "/api/internal/raft/vote", &req)
                .await
        })
    }

    fn append_entries(
        &self,
        to: NodeId,
        req: AppendEntriesRequest,
    ) -> TransportFuture<'_, AppendEntriesResponse> {
        Box::pin(async move {
            self.post_internal(&to, "/api/internal/raft/append", &req)
                .await
        })
    }

    fn install_snapshot(
        &self,
        to: NodeId,
        req: InstallSnapshotRequest,
    ) -> TransportFuture<'_, InstallSnapshotResponse> {
        Box::pin(async move {
            self.post_internal(&to, "/api/internal/raft/snapshot", &req)
                .await
        })
    }

    fn resolve_addr(&self, id: &NodeId) -> Option<String> {
        if let Some(addr) = self.static_peers.get(&id.0) {
            return Some(addr.clone());
        }
        // Best-effort synchronous resolution (NotLeader hints only): the
        // registry lives behind an async mutex, so only try_lock here.
        self.registry
            .as_ref()
            .and_then(|r| {
                r.try_lock()
                    .ok()
                    .map(|g| g.get_peer(id).map(|p| p.addr.clone()))
            })
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_static_peers_valid_and_malformed() {
        let map = HttpRaftTransport::parse_static_peers(
            " cp-1 = 10.0.0.1:3000 , cp-2=10.0.0.2:3000, broken, =oops, cp-3= ,",
        );
        assert_eq!(map.len(), 2);
        assert_eq!(map["cp-1"], "10.0.0.1:3000");
        assert_eq!(map["cp-2"], "10.0.0.2:3000");
    }

    #[test]
    fn resolve_addr_prefers_static_map() {
        let mut peers = HashMap::new();
        peers.insert("cp-1".to_string(), "10.0.0.1:3000".to_string());
        let transport = HttpRaftTransport::new(peers, None, None);
        assert_eq!(
            transport.resolve_addr(&NodeId("cp-1".into())),
            Some("10.0.0.1:3000".to_string())
        );
        assert_eq!(transport.resolve_addr(&NodeId("cp-9".into())), None);
    }

    /// The rolling-upgrade path (ops-guide §14.7): when a peer rejects the
    /// bincode body, `post_internal` must retry the SAME request as JSON —
    /// keeping the Authorization header — and succeed. Without this test a
    /// regression in the retry (e.g. dropping the Bearer token) would pass
    /// the whole suite, since every in-repo peer accepts bincode.
    #[tokio::test]
    async fn post_internal_retries_with_json_when_peer_rejects_bincode() {
        use axum::extract::State;
        use axum::http::{HeaderMap, StatusCode};
        use axum::response::IntoResponse;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone, Default)]
        struct Attempts(Arc<AtomicUsize>);

        // An "old node": 401 without the Bearer token, 415 for bincode
        // bodies, JSON accepted and answered as JSON.
        async fn vote_handler(
            State(attempts): State<Attempts>,
            headers: HeaderMap,
            body: axum::body::Bytes,
        ) -> axum::response::Response {
            attempts.0.fetch_add(1, Ordering::SeqCst);
            if headers.get("authorization").and_then(|v| v.to_str().ok())
                != Some("Bearer secret-token")
            {
                return StatusCode::UNAUTHORIZED.into_response();
            }
            let content_type = headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if !content_type.starts_with("application/json") {
                return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
            }
            let req: RequestVoteRequest = match serde_json::from_slice(&body) {
                Ok(req) => req,
                Err(_) => return StatusCode::BAD_REQUEST.into_response(),
            };
            axum::Json(RequestVoteResponse {
                term: req.term,
                vote_granted: true,
            })
            .into_response()
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let attempts = Attempts::default();
        let seen = Arc::clone(&attempts.0);
        let app = axum::Router::new()
            .route("/api/internal/raft/vote", axum::routing::post(vote_handler))
            .with_state(attempts);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut peers = HashMap::new();
        peers.insert("old-node".to_string(), addr.to_string());
        let transport = HttpRaftTransport::new(peers, None, Some("secret-token".to_string()));

        let resp = transport
            .request_vote(
                NodeId("old-node".into()),
                RequestVoteRequest {
                    term: 7,
                    candidate_id: NodeId("cp-1".into()),
                    last_log_index: 0,
                    last_log_term: 0,
                },
            )
            .await
            .expect("JSON fallback must succeed against a bincode-rejecting peer");
        assert!(resp.vote_granted);
        assert_eq!(resp.term, 7);
        assert_eq!(
            seen.load(Ordering::SeqCst),
            2,
            "exactly one bincode attempt followed by one JSON retry"
        );
    }
}
