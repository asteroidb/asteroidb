use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::hlc::HlcTimestamp;
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
    /// The responder's current frontier (highest tracked HLC).
    ///
    /// Used by the requester to correctly initialise its peer frontier
    /// tracking after a full sync, avoiding the bug where a local-only
    /// frontier would cause subsequent delta pulls to miss remote updates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frontier: Option<HlcTimestamp>,
}

// ---------------------------------------------------------------
// Delta sync types
// ---------------------------------------------------------------

/// A single entry in a delta sync payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaEntry {
    pub key: String,
    pub value: CrdtValue,
    pub hlc: HlcTimestamp,
}

/// Request for delta-based sync.
///
/// The sender provides its frontier timestamp; the receiver returns
/// all entries modified after that frontier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaSyncRequest {
    /// Node ID of the requesting node.
    pub sender: String,
    /// The requester's known frontier for the remote peer.
    /// Entries strictly after this timestamp will be returned.
    pub frontier: HlcTimestamp,
}

/// Response for delta-based sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaSyncResponse {
    /// Entries modified after the requested frontier.
    pub entries: Vec<DeltaEntry>,
    /// The responder's current frontier (highest tracked HLC).
    pub sender_frontier: Option<HlcTimestamp>,
}

/// Error returned when a batched push partially or fully fails.
#[derive(Debug, Clone)]
pub struct SyncPushError {
    /// Number of entries that were successfully pushed before the failure.
    pub pushed: usize,
    /// Human-readable reason for the failure.
    pub reason: String,
}

impl std::fmt::Display for SyncPushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sync push failed after {} entries: {}",
            self.pushed, self.reason
        )
    }
}

impl std::error::Error for SyncPushError {}

// ---------------------------------------------------------------
// Exponential backoff for per-peer retry
// ---------------------------------------------------------------

/// Per-peer exponential backoff state for sync retries.
///
/// Tracks consecutive failures and computes the next retry delay
/// using exponential backoff with jitter. The delay starts at
/// [`Self::INITIAL_BACKOFF`] and doubles on each failure up to
/// [`Self::MAX_BACKOFF`].
#[derive(Debug, Clone)]
pub struct PeerBackoff {
    /// Number of consecutive failures for this peer.
    pub consecutive_failures: u32,
    /// Instant at which the next sync attempt is allowed.
    ///
    /// Uses `tokio::time::Instant` for monotonic clock compatibility.
    pub ready_at: tokio::time::Instant,
}

impl PeerBackoff {
    /// Initial backoff delay after the first failure.
    pub const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
    /// Maximum backoff delay.
    pub const MAX_BACKOFF: Duration = Duration::from_secs(30);

    /// Create a new backoff state that is immediately ready.
    pub fn new() -> Self {
        Self {
            consecutive_failures: 0,
            ready_at: tokio::time::Instant::now(),
        }
    }

    /// Check whether the peer is ready for a sync attempt.
    pub fn is_ready(&self) -> bool {
        tokio::time::Instant::now() >= self.ready_at
    }

    /// Record a successful sync, resetting the backoff.
    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.ready_at = tokio::time::Instant::now();
    }

    /// Record a failed sync, increasing the backoff delay.
    ///
    /// Uses exponential backoff: `min(INITIAL_BACKOFF * 2^failures, MAX_BACKOFF)`
    /// with a random jitter of up to 25% of the computed delay.
    pub fn record_failure(&mut self) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let base = Self::INITIAL_BACKOFF
            .saturating_mul(1u32 << self.consecutive_failures.saturating_sub(1).min(5));
        let capped = base.min(Self::MAX_BACKOFF);

        // Add jitter: up to 25% of the capped delay.
        let jitter_ms = Self::simple_jitter(capped.as_millis() as u64 / 4);
        let with_jitter = capped + Duration::from_millis(jitter_ms);

        self.ready_at = tokio::time::Instant::now() + with_jitter;
    }

    /// Simple pseudo-random jitter based on the current instant.
    ///
    /// Not cryptographically secure, but sufficient for jitter purposes.
    fn simple_jitter(max_ms: u64) -> u64 {
        if max_ms == 0 {
            return 0;
        }
        // Use nanosecond component of current time as cheap entropy source.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64;
        nanos % (max_ms + 1)
    }

    /// Return the computed backoff delay for the current failure count
    /// (without jitter). Useful for testing.
    pub fn base_delay(&self) -> Duration {
        if self.consecutive_failures == 0 {
            return Duration::ZERO;
        }
        let base = Self::INITIAL_BACKOFF
            .saturating_mul(1u32 << self.consecutive_failures.saturating_sub(1).min(5));
        base.min(Self::MAX_BACKOFF)
    }
}

impl Default for PeerBackoff {
    fn default() -> Self {
        Self::new()
    }
}

/// Maximum number of keys per sync batch.
///
/// When pushing changed entries to a peer, large payloads are split
/// into chunks of this size to avoid oversized HTTP requests and to
/// allow partial progress on transient failures.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// Anti-entropy sync client.
///
/// Periodically pushes local CRDT values to every known peer.
/// Supports both full-store push and frontier-based delta push
/// with automatic batching for large change sets.
///
/// Holds a shared reference to the [`PeerRegistry`] so that peers
/// added or removed at runtime (via `/api/internal/join` or
/// `/api/internal/leave`) are automatically picked up by the sync loop.
pub struct SyncClient {
    peer_registry: Arc<Mutex<PeerRegistry>>,
    http_client: reqwest::Client,
    /// Optional Bearer token added to all outbound requests for internal API auth.
    auth_token: Option<String>,
}

impl SyncClient {
    /// Create a new `SyncClient` with a shared peer registry.
    pub fn new(peer_registry: Arc<Mutex<PeerRegistry>>) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("failed to build HTTP client");
        Self {
            peer_registry,
            http_client,
            auth_token: None,
        }
    }

    /// Create a `SyncClient` that attaches a Bearer token to all requests.
    pub fn with_token(peer_registry: Arc<Mutex<PeerRegistry>>, token: String) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("failed to build HTTP client");
        Self {
            peer_registry,
            http_client,
            auth_token: Some(token),
        }
    }

    /// Create a `SyncClient` with a custom reqwest client (for testing).
    pub fn with_client(
        peer_registry: Arc<Mutex<PeerRegistry>>,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            peer_registry,
            http_client,
            auth_token: None,
        }
    }

    /// Return a request builder with optional Bearer token header.
    fn authorized_get(&self, url: &str) -> reqwest::RequestBuilder {
        let mut builder = self.http_client.get(url);
        if let Some(ref token) = self.auth_token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder
    }

    /// Return a POST request builder with optional Bearer token header.
    fn authorized_post(&self, url: &str) -> reqwest::RequestBuilder {
        let mut builder = self.http_client.post(url);
        if let Some(ref token) = self.auth_token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder
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

        let peers = self.peer_registry.lock().await.all_peers_owned();
        for peer in &peers {
            let url = format!("http://{}/api/internal/sync", peer.addr);

            match self.authorized_post(&url).json(&request).send().await {
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

    /// Push only entries changed since the given frontier to a single peer.
    ///
    /// Extracts entries from `all_entries` that have a timestamp strictly
    /// after `frontier` (using the `timestamps` map), then sends them
    /// in batches of [`DEFAULT_BATCH_SIZE`] via `POST /api/internal/sync`.
    ///
    /// Returns the total number of entries successfully pushed. If any
    /// batch fails, remaining batches are skipped and the partial count
    /// is returned so the caller can decide whether to retry.
    pub async fn push_changed_keys(
        &self,
        peer_addr: &str,
        changed_entries: Vec<(String, CrdtValue)>,
        sender_id: &str,
        batch_size: usize,
    ) -> Result<usize, SyncPushError> {
        if changed_entries.is_empty() {
            return Ok(0);
        }

        let effective_batch_size = if batch_size == 0 {
            DEFAULT_BATCH_SIZE
        } else {
            batch_size
        };

        let mut total_pushed = 0usize;

        for chunk in changed_entries.chunks(effective_batch_size) {
            let entries: HashMap<String, CrdtValue> = chunk.iter().cloned().collect();
            let request = SyncRequest {
                sender: sender_id.to_string(),
                entries,
            };
            let url = format!("http://{peer_addr}/api/internal/sync");

            match self.authorized_post(&url).json(&request).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        total_pushed += chunk.len();
                        tracing::debug!(
                            peer_addr = %peer_addr,
                            batch_keys = chunk.len(),
                            "delta push batch succeeded"
                        );
                    } else {
                        tracing::warn!(
                            peer_addr = %peer_addr,
                            status = %resp.status(),
                            "delta push batch received non-success status"
                        );
                        return Err(SyncPushError {
                            pushed: total_pushed,
                            reason: format!("HTTP {}", resp.status()),
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        peer_addr = %peer_addr,
                        error = %e,
                        "delta push batch failed"
                    );
                    return Err(SyncPushError {
                        pushed: total_pushed,
                        reason: e.to_string(),
                    });
                }
            }
        }

        Ok(total_pushed)
    }

    /// Pull all key-value pairs from a specific peer.
    ///
    /// Sends `GET /api/internal/keys` to the peer and returns the
    /// full [`KeyDumpResponse`] including entries and the remote
    /// peer's frontier. Returns `None` on failure.
    pub async fn pull_all_keys(&self, peer_addr: &str) -> Option<KeyDumpResponse> {
        let url = format!("http://{}/api/internal/keys", peer_addr);

        match self.authorized_get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<KeyDumpResponse>().await {
                        Ok(dump) => Some(dump),
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

    /// Return a shared reference to the peer registry.
    pub fn peer_registry(&self) -> &Arc<Mutex<PeerRegistry>> {
        &self.peer_registry
    }

    // ---------------------------------------------------------------
    // Delta sync methods
    // ---------------------------------------------------------------

    /// Pull delta entries from a peer since the given frontier.
    ///
    /// Sends `POST /api/internal/sync/delta` with the local frontier.
    /// The peer returns entries modified after that frontier.
    /// Returns `None` on failure.
    pub async fn pull_delta(
        &self,
        peer_addr: &str,
        sender: &str,
        frontier: &HlcTimestamp,
    ) -> Option<DeltaSyncResponse> {
        let url = format!("http://{peer_addr}/api/internal/sync/delta");
        let req = DeltaSyncRequest {
            sender: sender.to_string(),
            frontier: frontier.clone(),
        };

        match self.authorized_post(&url).json(&req).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<DeltaSyncResponse>().await {
                        Ok(delta) => Some(delta),
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to parse delta sync response");
                            None
                        }
                    }
                } else {
                    tracing::warn!(
                        status = %resp.status(),
                        "delta sync request received non-success status"
                    );
                    None
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "delta sync request failed");
                None
            }
        }
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

        let resp = KeyDumpResponse {
            entries,
            frontier: Some(hlc(500, 0, "node-1")),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: KeyDumpResponse = serde_json::from_str(&json).unwrap();

        assert!(deserialized.entries.contains_key("hits"));
        assert_eq!(deserialized.frontier.unwrap().physical, 500);
    }

    #[test]
    fn key_dump_response_without_frontier_deserialises() {
        // Backwards-compatibility: older peers may omit the frontier field.
        let json = r#"{"entries":{}}"#;
        let resp: KeyDumpResponse = serde_json::from_str(json).unwrap();
        assert!(resp.entries.is_empty());
        assert!(resp.frontier.is_none());
    }

    fn shared_registry(peers: Vec<PeerConfig>) -> Arc<Mutex<PeerRegistry>> {
        Arc::new(Mutex::new(PeerRegistry::new(nid("node-1"), peers).unwrap()))
    }

    #[tokio::test]
    async fn sync_client_creation() {
        let registry = shared_registry(vec![PeerConfig {
            node_id: nid("node-2"),
            addr: "127.0.0.1:8001".to_string(),
        }]);

        let client = SyncClient::new(registry);
        assert_eq!(client.peer_registry().lock().await.peer_count(), 1);
    }

    #[tokio::test]
    async fn push_all_keys_empty_entries_returns_zero() {
        let registry = shared_registry(vec![PeerConfig {
            node_id: nid("node-2"),
            addr: "127.0.0.1:8001".to_string(),
        }]);

        let client = SyncClient::new(registry);
        let result = client.push_all_keys(HashMap::new(), "node-1").await;
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn push_all_keys_to_unreachable_peer_returns_zero() {
        let registry = shared_registry(vec![PeerConfig {
            node_id: nid("node-2"),
            // Unreachable address.
            addr: "127.0.0.1:1".to_string(),
        }]);

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
        let registry = shared_registry(vec![]);

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let client = SyncClient::with_client(registry, http_client);

        let result = client.pull_all_keys("127.0.0.1:1").await;
        assert!(result.is_none());
    }

    /// Verify that peers added after `SyncClient` creation are visible to sync.
    #[tokio::test]
    async fn sync_client_sees_dynamically_added_peers() {
        let registry = shared_registry(vec![]);
        let client = SyncClient::new(Arc::clone(&registry));

        // Initially no peers.
        assert_eq!(client.peer_registry().lock().await.peer_count(), 0);

        // Simulate a dynamic join.
        registry
            .lock()
            .await
            .add_peer(PeerConfig {
                node_id: nid("node-2"),
                addr: "127.0.0.1:8001".to_string(),
            })
            .unwrap();

        // SyncClient now sees the new peer.
        assert_eq!(client.peer_registry().lock().await.peer_count(), 1);
    }

    /// Verify that peers removed after `SyncClient` creation are no longer synced.
    #[tokio::test]
    async fn sync_client_sees_dynamically_removed_peers() {
        let registry = shared_registry(vec![PeerConfig {
            node_id: nid("node-2"),
            addr: "127.0.0.1:8001".to_string(),
        }]);
        let client = SyncClient::new(Arc::clone(&registry));

        assert_eq!(client.peer_registry().lock().await.peer_count(), 1);

        // Simulate a dynamic leave.
        registry.lock().await.remove_peer(&nid("node-2")).unwrap();

        assert_eq!(client.peer_registry().lock().await.peer_count(), 0);
    }

    // ---------------------------------------------------------------
    // Delta sync types serde
    // ---------------------------------------------------------------

    fn hlc(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    #[test]
    fn delta_sync_request_serde_roundtrip() {
        let req = DeltaSyncRequest {
            sender: "node-1".to_string(),
            frontier: hlc(100, 0, "node-1"),
        };

        let json = serde_json::to_string(&req).unwrap();
        let back: DeltaSyncRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(back.sender, "node-1");
        assert_eq!(back.frontier.physical, 100);
    }

    #[test]
    fn delta_sync_response_serde_roundtrip() {
        let mut counter = PnCounter::new();
        counter.increment(&nid("node-1"));

        let resp = DeltaSyncResponse {
            entries: vec![DeltaEntry {
                key: "key1".into(),
                value: CrdtValue::Counter(counter),
                hlc: hlc(200, 0, "node-1"),
            }],
            sender_frontier: Some(hlc(200, 0, "node-1")),
        };

        let json = serde_json::to_string(&resp).unwrap();
        let back: DeltaSyncResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(back.entries.len(), 1);
        assert_eq!(back.entries[0].key, "key1");
        assert_eq!(back.entries[0].hlc.physical, 200);
        assert_eq!(back.sender_frontier.unwrap().physical, 200);
    }

    #[test]
    fn delta_sync_response_empty_entries() {
        let resp = DeltaSyncResponse {
            entries: vec![],
            sender_frontier: None,
        };

        let json = serde_json::to_string(&resp).unwrap();
        let back: DeltaSyncResponse = serde_json::from_str(&json).unwrap();

        assert!(back.entries.is_empty());
        assert!(back.sender_frontier.is_none());
    }

    #[tokio::test]
    async fn pull_delta_from_unreachable_peer_returns_none() {
        let registry = shared_registry(vec![]);

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let client = SyncClient::with_client(registry, http_client);

        let result = client
            .pull_delta("127.0.0.1:1", "node-1", &hlc(0, 0, ""))
            .await;
        assert!(result.is_none());
    }

    // ---------------------------------------------------------------
    // push_changed_keys tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn push_changed_keys_empty_returns_zero() {
        let registry = shared_registry(vec![]);
        let client = SyncClient::new(registry);

        let result = client
            .push_changed_keys("127.0.0.1:1", vec![], "node-1", DEFAULT_BATCH_SIZE)
            .await;
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn push_changed_keys_to_unreachable_returns_error() {
        let registry = shared_registry(vec![]);
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let client = SyncClient::with_client(registry, http_client);

        let mut counter = PnCounter::new();
        counter.increment(&nid("node-1"));
        let entries = vec![("key1".to_string(), CrdtValue::Counter(counter))];

        let result = client
            .push_changed_keys("127.0.0.1:1", entries, "node-1", DEFAULT_BATCH_SIZE)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.pushed, 0);
    }

    // ---------------------------------------------------------------
    // PeerBackoff tests
    // ---------------------------------------------------------------

    #[test]
    fn backoff_new_is_immediately_ready() {
        let b = PeerBackoff::new();
        assert!(b.is_ready());
        assert_eq!(b.consecutive_failures, 0);
        assert_eq!(b.base_delay(), Duration::ZERO);
    }

    #[test]
    fn backoff_success_resets() {
        let mut b = PeerBackoff::new();
        b.record_failure();
        assert_eq!(b.consecutive_failures, 1);
        b.record_success();
        assert_eq!(b.consecutive_failures, 0);
        assert!(b.is_ready());
    }

    #[test]
    fn backoff_delay_increases_exponentially() {
        let mut b = PeerBackoff::new();

        b.record_failure(); // 1 failure
        assert_eq!(b.consecutive_failures, 1);
        let d1 = b.base_delay();

        b.record_failure(); // 2 failures
        let d2 = b.base_delay();
        assert!(d2 > d1, "delay should increase: {d2:?} > {d1:?}");

        b.record_failure(); // 3 failures
        let d3 = b.base_delay();
        assert!(d3 > d2, "delay should increase: {d3:?} > {d2:?}");
    }

    #[test]
    fn backoff_delay_capped_at_max() {
        let mut b = PeerBackoff::new();
        for _ in 0..20 {
            b.record_failure();
        }
        let delay = b.base_delay();
        assert!(
            delay <= PeerBackoff::MAX_BACKOFF,
            "delay {delay:?} should be <= {:?}",
            PeerBackoff::MAX_BACKOFF
        );
    }

    #[test]
    fn backoff_not_ready_after_failure() {
        let mut b = PeerBackoff::new();
        b.record_failure();
        // Immediately after failure, the backoff should gate retries
        // (ready_at is in the future).
        assert!(!b.is_ready());
    }

    #[test]
    fn backoff_first_failure_uses_initial_delay() {
        let mut b = PeerBackoff::new();
        b.record_failure();
        // First failure should use INITIAL_BACKOFF (1s), not 2x.
        assert_eq!(b.base_delay(), PeerBackoff::INITIAL_BACKOFF);
    }

    #[test]
    fn backoff_default_is_new() {
        let b = PeerBackoff::default();
        assert!(b.is_ready());
        assert_eq!(b.consecutive_failures, 0);
    }

    // ---------------------------------------------------------------
    // SyncPushError tests
    // ---------------------------------------------------------------

    #[test]
    fn sync_push_error_display() {
        let err = SyncPushError {
            pushed: 5,
            reason: "timeout".into(),
        };
        let msg = format!("{err}");
        assert!(msg.contains("5 entries"));
        assert!(msg.contains("timeout"));
    }

    // ---------------------------------------------------------------
    // Batch splitting tests
    // ---------------------------------------------------------------

    #[test]
    fn batch_splitting_logic() {
        // Verify that chunk splitting works correctly for various sizes.
        let entries: Vec<(String, CrdtValue)> = (0..250)
            .map(|i| {
                let mut c = PnCounter::new();
                c.increment(&nid("n"));
                (format!("key-{i}"), CrdtValue::Counter(c))
            })
            .collect();

        let batch_size = 100;
        let chunks: Vec<_> = entries.chunks(batch_size).collect();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 100);
        assert_eq!(chunks[1].len(), 100);
        assert_eq!(chunks[2].len(), 50);
    }
}
