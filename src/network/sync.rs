use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::hlc::HlcTimestamp;
use crate::http::codec::{self, CONTENT_TYPE_BINCODE, deserialize_internal, serialize_internal};
use crate::network::peer::PeerRegistry;
use crate::store::digest::{DIGEST_LEN, DIGEST_SCHEME_VERSION, StoreDigest};
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
    /// Per-key HLC timestamps for preserving original modification times
    /// during full-sync import.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub timestamps: HashMap<String, HlcTimestamp>,
    /// The responder's per-origin applied frontier (session guarantees).
    ///
    /// A full dump is a complete state, so the receiver may adopt this
    /// map unconditionally after merging all entries.
    /// No `skip_serializing_if`: bincode requires positional determinism.
    #[serde(default)]
    pub applied_origins: HashMap<String, HlcTimestamp>,
    /// Keys poisoned on the responder by failed merges; the receiver
    /// unions them when adopting `applied_origins` so the sender's
    /// dropped contributions are not claimed as present.
    #[serde(default)]
    pub merge_failed_keys: Vec<String>,
    /// The responder's per-origin VISIBLE frontier (superset of
    /// `applied_origins`). Merged unconditionally by the receiver: any
    /// merged value may embed contributions from origins the per-key HLC
    /// does not name, and response session tokens must cover them.
    #[serde(default)]
    pub visible_origins: HashMap<String, HlcTimestamp>,
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
    /// The responder's per-origin applied frontier (session guarantees).
    ///
    /// The receiver may adopt (max-merge) this map only when the delta it
    /// requested is complete relative to the responder's pruning, i.e.
    /// `request.frontier >= pruned_floor` — otherwise entries pruned on
    /// the responder are silently absent from the delta.
    #[serde(default)]
    pub applied_origins: HashMap<String, HlcTimestamp>,
    /// Keys poisoned on the responder by failed merges (adopted together
    /// with `applied_origins`).
    #[serde(default)]
    pub merge_failed_keys: Vec<String>,
    /// The responder's pruned floor: adoption guard for `applied_origins`.
    #[serde(default)]
    pub pruned_floor: Option<HlcTimestamp>,
    /// The responder's per-origin VISIBLE frontier (superset of
    /// `applied_origins`).
    ///
    /// Merged by the receiver UNCONDITIONALLY (even when the delta may be
    /// incomplete and no applied claims are made): a delta entry's CRDT
    /// value can embed contributions from origins its HLC does not name,
    /// so the receiver's visible state may reflect anything the sender
    /// could see. Over-covering only widens response session tokens
    /// (false negatives); it never fabricates an applied claim.
    #[serde(default)]
    pub visible_origins: HashMap<String, HlcTimestamp>,
}

// ---------------------------------------------------------------
// Digest sync types (stepwise diff, see crate::store::digest)
// ---------------------------------------------------------------

/// A single non-empty bucket digest in a [`DigestSyncRequest`].
///
/// The bucket list is sparse: absent indexes mean "empty bucket".
/// NOTE for maintainers: bincode is positional — new fields may only be
/// appended at the end with `#[serde(default)]` and never
/// `skip_serializing_if` (same rule as `KeyDumpResponse`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketDigestEntry {
    /// Bucket index (`0..DIGEST_BUCKET_COUNT`).
    pub index: u16,
    /// 32-byte SHA-256 bucket digest.
    pub digest: Vec<u8>,
}

/// Request payload for `POST /api/internal/sync/digest`.
///
/// The requester sends its full two-level digest; the responder compares
/// against its own state and answers in ONE round trip — either
/// `root_matched` (zero transfer) or the entries of every mismatched
/// bucket. Keeping the whole exchange in one round trip matters on
/// high-latency links (the target deployment), which is why this is a
/// fixed-depth digest rather than a Merkle-tree descent (O(log n) RTTs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DigestSyncRequest {
    /// Node ID of the requesting node.
    pub sender: String,
    /// Digest scheme version ([`DIGEST_SCHEME_VERSION`]). A responder
    /// with a different version replies `scheme_ok = false` and the
    /// requester falls back to legacy full sync.
    pub scheme_version: u32,
    /// 32-byte root digest of the requester's state.
    pub root: Vec<u8>,
    /// Sparse list of the requester's non-empty bucket digests.
    pub buckets: Vec<BucketDigestEntry>,
    /// `true` (pull): the responder returns mismatched-bucket entries.
    /// `false` (push probe): the responder returns only the mismatched
    /// bucket indexes, and the requester pushes its own keys for them.
    pub include_entries: bool,
}

impl DigestSyncRequest {
    /// Build a request from a locally computed [`StoreDigest`].
    pub fn from_digest(sender: &str, digest: &StoreDigest, include_entries: bool) -> Self {
        Self {
            sender: sender.to_string(),
            scheme_version: DIGEST_SCHEME_VERSION,
            root: digest.root.to_vec(),
            buckets: digest
                .non_empty_buckets()
                .map(|(index, d)| BucketDigestEntry {
                    index,
                    digest: d.to_vec(),
                })
                .collect(),
            include_entries,
        }
    }
}

/// Response payload for `POST /api/internal/sync/digest`.
///
/// Everything in this response — digest comparison, entries, frontier and
/// session-guarantee metadata — is taken from ONE snapshot of the
/// responder's store (single lock scope). That is what makes adopting the
/// session claims after applying the mismatched entries exactly as sound
/// as after a full dump: matched buckets are byte-identical to the
/// snapshot, mismatched buckets are fully contained in `entries`, so the
/// receiver's post-merge state dominates the responder's snapshot state.
///
/// All fields carry `#[serde(default)]` so future trailing additions stay
/// JSON-compatible; bincode remains positional (append-only, no
/// `skip_serializing_if`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DigestSyncResponse {
    /// `false` when the responder does not speak the requested digest
    /// scheme version (or the request digests were malformed). The
    /// requester must fall back to legacy full sync.
    #[serde(default)]
    pub scheme_ok: bool,
    /// `true` when the root digests matched: states are identical and no
    /// entries are transferred.
    #[serde(default)]
    pub root_matched: bool,
    /// Bucket indexes whose digests differ (bidirectional difference).
    #[serde(default)]
    pub mismatched_buckets: Vec<u16>,
    /// Entries of every mismatched bucket (only when the request had
    /// `include_entries = true` and the root did not match).
    #[serde(default)]
    pub entries: HashMap<String, CrdtValue>,
    /// Per-key HLC timestamps for `entries` (where tracked).
    #[serde(default)]
    pub timestamps: HashMap<String, HlcTimestamp>,
    /// The responder's current frontier at snapshot time.
    #[serde(default)]
    pub frontier: Option<HlcTimestamp>,
    /// The responder's per-origin applied frontier (session guarantees),
    /// snapshotted together with the digest — adoptable by the receiver
    /// after applying `entries` (full-dump-equivalent completeness).
    #[serde(default)]
    pub applied_origins: HashMap<String, HlcTimestamp>,
    /// The responder's per-origin visible frontier (superset of
    /// `applied_origins`; merged unconditionally).
    #[serde(default)]
    pub visible_origins: HashMap<String, HlcTimestamp>,
    /// Keys poisoned on the responder by failed merges (unioned when
    /// adopting `applied_origins`).
    #[serde(default)]
    pub merge_failed_keys: Vec<String>,
    /// Total number of keys in the responder's snapshot. The requester
    /// derives the transfer saving as `total_keys - entries.len()`.
    #[serde(default)]
    pub total_keys: u64,
}

/// Result of a digest sync attempt.
#[derive(Debug)]
pub enum DigestSyncResult {
    /// Response received and decoded (check `scheme_ok` before use).
    /// Boxed: the response struct is large relative to the other variants.
    Ok(Box<DigestSyncResponse>),
    /// The peer answered with a status proving the digest route is
    /// absent (404 from an older node, 405 from a router that knows the
    /// path but not the method). The caller should fall back to legacy
    /// full sync and cache the peer as digest-unsupported for a while.
    Unsupported,
    /// Network failure, undecodable response, or any other non-2xx
    /// status (500/503/429/401, ... — plausibly transient conditions on
    /// a digest-capable peer); fall back to legacy full sync for this
    /// cycle without caching the peer as unsupported.
    Failed,
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

/// Result of a delta pull attempt.
///
/// Distinguishes between successful pulls, network-level failures, and
/// deserialization errors (e.g. jitter-corrupted payloads). The caller
/// can use this to decide whether to retry delta or skip straight to
/// full sync.
#[derive(Debug)]
pub enum PullDeltaResult {
    /// Delta response was successfully received and decoded.
    /// Boxed: the response struct is large relative to the other variants.
    Ok(Box<DeltaSyncResponse>),
    /// Network error (timeout, connection refused, non-2xx status).
    /// Retry may succeed once the network heals.
    NetworkError,
    /// Response was received (2xx) but could not be deserialized.
    /// This typically indicates payload corruption from jitter; the
    /// peer's data is intact but the wire encoding was mangled.
    /// Caller should fall back to full sync immediately rather than
    /// retrying delta, which would likely fail the same way.
    DeserializationError,
}

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
    pub const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
    /// Maximum backoff delay.
    ///
    /// Capped at 2 seconds to ensure timely convergence under network
    /// fault conditions (jitter, rolling partitions). A higher cap (e.g.
    /// 8s) caused peers to miss too many sync cycles after transient
    /// failures, preventing recovery within the test convergence window.
    pub const MAX_BACKOFF: Duration = Duration::from_secs(2);

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
    /// with a random jitter of up to 25% of the computed delay. The exponent
    /// is capped at 4 (i.e. max multiplier 16x) to keep backoff growth bounded.
    pub fn record_failure(&mut self) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let base = Self::INITIAL_BACKOFF
            .saturating_mul(1u32 << self.consecutive_failures.saturating_sub(1).min(4));
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
            .saturating_mul(1u32 << self.consecutive_failures.saturating_sub(1).min(4));
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

/// Maximum serialized size (in bytes) of a delta payload before falling
/// back to full-state sync. Prevents oversized deltas that would negate
/// the bandwidth savings.
pub const MAX_DELTA_PAYLOAD_BYTES: usize = 512 * 1024; // 512 KiB

/// Default change rate threshold for triggering full sync fallback.
///
/// When the ratio `changed_keys / total_keys` exceeds this value during
/// delta sync, the delta payload is nearly as large as a full dump, so
/// the system falls back to pushing the full state instead.
pub const DEFAULT_FULL_SYNC_THRESHOLD: f64 = 0.5;

/// Check whether the change rate exceeds the threshold for full sync fallback.
///
/// Returns `true` when `changed_keys / total_keys > threshold`, indicating
/// that delta sync loses its advantage and full sync should be used.
/// Returns `false` when `total_keys` is zero (empty store, no fallback needed).
pub fn should_fallback_to_full_sync(
    changed_keys: usize,
    total_keys: usize,
    threshold: f64,
) -> bool {
    if total_keys == 0 {
        return false;
    }
    let rate = changed_keys as f64 / total_keys as f64;
    rate > threshold
}

/// Tracks per-peer delta frontiers for efficient delta sync.
///
/// Maintains the last-acknowledged HLC frontier for each peer, enabling
/// the sync layer to compute and send only changes since that frontier.
/// When a peer successfully acknowledges a sync, its frontier is advanced.
///
/// Periodically advances the minimum frontier across all peers (GC frontier)
/// so that obsolete delta tracking data can be reclaimed.
#[derive(Debug, Clone)]
pub struct PeerFrontierTracker {
    /// Last-acked frontier per peer address.
    frontiers: HashMap<String, HlcTimestamp>,
    /// The minimum frontier across all tracked peers, used for GC.
    gc_frontier: Option<HlcTimestamp>,
}

impl PeerFrontierTracker {
    /// Create a new tracker with no known peers.
    pub fn new() -> Self {
        Self {
            frontiers: HashMap::new(),
            gc_frontier: None,
        }
    }

    /// Get the last-acked frontier for a peer, if known.
    pub fn frontier_for(&self, peer_addr: &str) -> Option<&HlcTimestamp> {
        self.frontiers.get(peer_addr)
    }

    /// Update the frontier for a peer after a successful sync acknowledgement.
    pub fn advance_frontier(&mut self, peer_addr: &str, frontier: HlcTimestamp) {
        let entry = self
            .frontiers
            .entry(peer_addr.to_string())
            .or_insert_with(|| HlcTimestamp {
                physical: 0,
                logical: 0,
                node_id: String::new(),
            });
        if frontier > *entry {
            *entry = frontier;
        }
    }

    /// Remove tracking for a peer (e.g., when the peer leaves the cluster).
    pub fn remove_peer(&mut self, peer_addr: &str) {
        self.frontiers.remove(peer_addr);
    }

    /// Recompute and return the GC frontier (minimum across all peers).
    ///
    /// Delta tracking data at or below this frontier can be safely pruned,
    /// because all peers have already acknowledged it.
    pub fn advance_gc_frontier(&mut self) -> Option<&HlcTimestamp> {
        self.gc_frontier = self.frontiers.values().min().cloned();
        self.gc_frontier.as_ref()
    }

    /// Return the current GC frontier without recomputing.
    pub fn gc_frontier(&self) -> Option<&HlcTimestamp> {
        self.gc_frontier.as_ref()
    }

    /// Return the number of tracked peers.
    pub fn peer_count(&self) -> usize {
        self.frontiers.len()
    }
}

impl Default for PeerFrontierTracker {
    fn default() -> Self {
        Self::new()
    }
}

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
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
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
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
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
                // Bincode encoding failed, go directly to JSON.
                return self.json_post(url, data).send().await;
            }
        };

        match req_builder.send().await {
            Ok(resp) if !resp.status().is_success() => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_else(|_| "<unreadable>".into());
                let truncated: String = body.chars().take(500).collect();
                tracing::warn!(
                    url = %url,
                    status = %status,
                    body = %truncated,
                    "bincode request rejected, retrying with JSON"
                );
                self.json_post(url, data).send().await
            }
            other => other,
        }
    }

    /// Deserialize a response body based on the response's Content-Type header.
    ///
    /// Supports both bincode (`application/octet-stream`) and JSON responses
    /// for backward compatibility with older peers.
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

            match self.send_with_json_fallback(&url, &request).await {
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

    /// Push the full local state to a single peer by address.
    ///
    /// Sends a `POST /api/internal/sync` request with all provided entries.
    /// Returns `Some(SyncResponse)` on a 2xx response (callers must check
    /// `errors` before advancing frontiers), or `None` on transport/HTTP failure.
    pub async fn push_full_state_to_peer(
        &self,
        peer_addr: &str,
        entries: HashMap<String, CrdtValue>,
        sender_id: &str,
    ) -> Option<SyncResponse> {
        if entries.is_empty() {
            return Some(SyncResponse {
                merged: 0,
                errors: Vec::new(),
            });
        }

        let request = SyncRequest {
            sender: sender_id.to_string(),
            entries,
        };

        let url = format!("http://{peer_addr}/api/internal/sync");

        match self.send_with_json_fallback(&url, &request).await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match Self::decode_response::<SyncResponse>(resp).await {
                        Ok(sync_resp) => {
                            tracing::debug!(
                                peer_addr = %peer_addr,
                                merged = sync_resp.merged,
                                errors = sync_resp.errors.len(),
                                "initial full push to peer succeeded"
                            );
                            Some(sync_resp)
                        }
                        Err(e) => {
                            tracing::warn!(
                                peer_addr = %peer_addr,
                                error = %e,
                                "initial full push: failed to decode SyncResponse"
                            );
                            None
                        }
                    }
                } else {
                    tracing::warn!(
                        peer_addr = %peer_addr,
                        status = %resp.status(),
                        "initial full push to peer received non-success status"
                    );
                    None
                }
            }
            Err(e) => {
                tracing::warn!(
                    peer_addr = %peer_addr,
                    error = %e,
                    "initial full push to peer failed"
                );
                None
            }
        }
    }

    /// Push only entries changed since the given frontier to a single peer.
    ///
    /// Extracts entries from `all_entries` that have a timestamp strictly
    /// after `frontier` (using the `timestamps` map), then sends them
    /// in batches of [`DEFAULT_BATCH_SIZE`] via `POST /api/internal/sync`.
    ///
    /// Returns the total number of entries successfully pushed. If a
    /// batch fails at the transport/HTTP level, remaining batches are
    /// skipped and the partial count is returned so the caller can
    /// decide whether to retry. Per-key merge errors reported by the
    /// peer (e.g. permanent type mismatches) do NOT abort later batches:
    /// the failing keys are skipped, every remaining batch is still
    /// attempted, and the accumulated failures are reported through the
    /// returned error at the end — one poisoned key must not starve the
    /// keys in subsequent batches (which a full-state push would have
    /// delivered).
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
        let mut merge_error_total = 0usize;

        for chunk in changed_entries.chunks(effective_batch_size) {
            let entries: HashMap<String, CrdtValue> = chunk.iter().cloned().collect();
            let request = SyncRequest {
                sender: sender_id.to_string(),
                entries,
            };
            let url = format!("http://{peer_addr}/api/internal/sync");

            match self.send_with_json_fallback(&url, &request).await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        // Parse the response body to check for per-key merge
                        // errors. Only count entries that were actually merged.
                        let sync_resp: Option<SyncResponse> =
                            Self::decode_response(resp).await.ok();
                        let error_count = sync_resp.as_ref().map(|r| r.errors.len()).unwrap_or(0);
                        let actually_pushed = chunk.len().saturating_sub(error_count);
                        if error_count > 0 {
                            let error_keys: Vec<&str> = sync_resp
                                .as_ref()
                                .map(|r| r.errors.iter().map(|e| e.key.as_str()).collect())
                                .unwrap_or_default();
                            tracing::warn!(
                                peer_addr = %peer_addr,
                                error_count = error_count,
                                error_keys = ?error_keys,
                                "delta push batch had merge errors on remote"
                            );
                            // Per-key merge errors are typically permanent
                            // (type mismatches poisoned on the remote):
                            // aborting here would starve every key in the
                            // remaining batches without helping the failed
                            // ones. Keep pushing and report the accumulated
                            // failures once all batches were attempted.
                            merge_error_total += error_count;
                        }
                        total_pushed += actually_pushed;
                        tracing::debug!(
                            peer_addr = %peer_addr,
                            batch_keys = actually_pushed,
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

        if merge_error_total > 0 {
            // Partial success: return an error so the caller does not
            // advance its frontier past the failed keys.
            return Err(SyncPushError {
                pushed: total_pushed,
                reason: format!("{merge_error_total} keys failed to merge on remote"),
            });
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

        // Try with bincode Accept header first.
        let resp = match self
            .authorized_get(&url)
            .header("accept", CONTENT_TYPE_BINCODE)
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status().is_success() {
                    resp
                } else {
                    tracing::debug!(
                        status = %resp.status(),
                        "pull_all_keys bincode request rejected, retrying with JSON"
                    );
                    // Retry without bincode Accept header for backward compatibility.
                    match self.authorized_get(&url).send().await {
                        Ok(resp) => resp,
                        Err(e) => {
                            tracing::warn!(error = %e, "pull_all_keys JSON retry failed");
                            return None;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "pull_all_keys request failed"
                );
                return None;
            }
        };

        if resp.status().is_success() {
            match Self::decode_response::<KeyDumpResponse>(resp).await {
                Ok(dump) => Some(dump),
                Err(e) => {
                    // Mixed-version compatibility: a peer running an older
                    // build replies with a bincode payload of the OLD
                    // struct layout; bincode is positional, so decoding it
                    // into the current layout fails. JSON tolerates
                    // missing fields via serde defaults, so retry once
                    // forcing a JSON response before giving up.
                    tracing::warn!(
                        error = %e,
                        "failed to parse key dump response; retrying with JSON \
                         (mixed-version peer?)"
                    );
                    match self
                        .authorized_get(&url)
                        .header("accept", "application/json")
                        .send()
                        .await
                    {
                        Ok(resp) if resp.status().is_success() => {
                            match Self::decode_response::<KeyDumpResponse>(resp).await {
                                Ok(dump) => Some(dump),
                                Err(e) => {
                                    tracing::warn!(
                                        error = %e,
                                        "failed to parse key dump response (JSON retry)"
                                    );
                                    None
                                }
                            }
                        }
                        _ => None,
                    }
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
    ///
    /// Returns a [`PullDeltaResult`] that distinguishes network errors
    /// from deserialization errors, allowing the caller to skip straight
    /// to full sync when the payload was corrupted (e.g. by jitter).
    pub async fn pull_delta(
        &self,
        peer_addr: &str,
        sender: &str,
        frontier: &HlcTimestamp,
    ) -> PullDeltaResult {
        let url = format!("http://{peer_addr}/api/internal/sync/delta");
        let req = DeltaSyncRequest {
            sender: sender.to_string(),
            frontier: frontier.clone(),
        };

        match self.send_with_json_fallback(&url, &req).await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match Self::decode_response::<DeltaSyncResponse>(resp).await {
                        Ok(delta) => PullDeltaResult::Ok(Box::new(delta)),
                        Err(e) => {
                            // Mixed-version compatibility: an older peer
                            // replies with a bincode payload of the OLD
                            // struct layout, which fails positional
                            // decoding into the current layout. JSON
                            // tolerates missing fields via serde defaults,
                            // so retry once forcing a JSON response before
                            // declaring the payload undecodable.
                            tracing::warn!(
                                error = %e,
                                peer = %peer_addr,
                                "failed to deserialize delta sync response; \
                                 retrying with JSON (mixed-version peer?)"
                            );
                            let json_retry = self
                                .json_post(&url, &req)
                                .header("accept", "application/json")
                                .send()
                                .await;
                            match json_retry {
                                Ok(resp) if resp.status().is_success() => {
                                    match Self::decode_response::<DeltaSyncResponse>(resp).await {
                                        Ok(delta) => PullDeltaResult::Ok(Box::new(delta)),
                                        Err(e) => {
                                            tracing::warn!(
                                                error = %e,
                                                peer = %peer_addr,
                                                "delta sync JSON retry undecodable, need full sync"
                                            );
                                            PullDeltaResult::DeserializationError
                                        }
                                    }
                                }
                                _ => PullDeltaResult::DeserializationError,
                            }
                        }
                    }
                } else {
                    tracing::warn!(
                        status = %resp.status(),
                        "delta sync request received non-success status"
                    );
                    PullDeltaResult::NetworkError
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "delta sync request failed");
                PullDeltaResult::NetworkError
            }
        }
    }

    // ---------------------------------------------------------------
    // Digest sync methods
    // ---------------------------------------------------------------

    /// Exchange key-range digests with a peer via
    /// `POST /api/internal/sync/digest`.
    ///
    /// Returns [`DigestSyncResult::Unsupported`] only for 404/405
    /// responses (older nodes answer 404 for the unknown route —
    /// rolling-upgrade safe) and [`DigestSyncResult::Failed`] for
    /// transport errors, undecodable payloads, and every other non-2xx
    /// status: a 500/503/429/401 may come from a digest-capable peer
    /// that is merely overloaded or mid-token-rotation, and must not
    /// park the peer in the caller's digest-unsupported cache. Like
    /// [`pull_delta`](Self::pull_delta), an undecodable bincode body is
    /// retried once forcing a JSON response (mixed-version peers).
    pub async fn digest_sync(&self, peer_addr: &str, req: &DigestSyncRequest) -> DigestSyncResult {
        debug_assert_eq!(req.root.len(), DIGEST_LEN);
        let url = format!("http://{peer_addr}/api/internal/sync/digest");

        match self.send_with_json_fallback(&url, req).await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match Self::decode_response::<DigestSyncResponse>(resp).await {
                        Ok(digest_resp) => DigestSyncResult::Ok(Box::new(digest_resp)),
                        Err(e) => {
                            // Mixed-version compatibility: retry once
                            // forcing JSON (see pull_delta for rationale).
                            tracing::warn!(
                                error = %e,
                                peer = %peer_addr,
                                "failed to deserialize digest sync response; \
                                 retrying with JSON (mixed-version peer?)"
                            );
                            let json_retry = self
                                .json_post(&url, req)
                                .header("accept", "application/json")
                                .send()
                                .await;
                            match json_retry {
                                Ok(resp) if resp.status().is_success() => {
                                    match Self::decode_response::<DigestSyncResponse>(resp).await {
                                        Ok(digest_resp) => {
                                            DigestSyncResult::Ok(Box::new(digest_resp))
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                error = %e,
                                                peer = %peer_addr,
                                                "digest sync JSON retry undecodable"
                                            );
                                            DigestSyncResult::Failed
                                        }
                                    }
                                }
                                _ => DigestSyncResult::Failed,
                            }
                        }
                    }
                } else {
                    // send_with_json_fallback already retried with JSON on
                    // a non-success status. Only a status that proves the
                    // route itself is absent (404 on old nodes, 405 from a
                    // router that knows the path but not the method) means
                    // the peer is digest-unsupported; anything else —
                    // 503/500 under load, 429, 401 during token rotation —
                    // is plausibly transient on a digest-capable peer and
                    // must map to Failed so the caller does not cache the
                    // peer as unsupported for 10 minutes.
                    let status = resp.status();
                    if status == reqwest::StatusCode::NOT_FOUND
                        || status == reqwest::StatusCode::METHOD_NOT_ALLOWED
                    {
                        tracing::debug!(
                            peer = %peer_addr,
                            status = %status,
                            "digest sync rejected by peer (digest-unsupported node?)"
                        );
                        DigestSyncResult::Unsupported
                    } else {
                        tracing::warn!(
                            peer = %peer_addr,
                            status = %status,
                            "digest sync received non-success status (transient?)"
                        );
                        DigestSyncResult::Failed
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, peer = %peer_addr, "digest sync request failed");
                DigestSyncResult::Failed
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

        let mut applied_origins = HashMap::new();
        applied_origins.insert("node-1".to_string(), hlc(500, 0, "node-1"));
        let resp = KeyDumpResponse {
            entries,
            frontier: Some(hlc(500, 0, "node-1")),
            timestamps: HashMap::new(),
            applied_origins,
            merge_failed_keys: vec!["bad-key".into()],
            visible_origins: HashMap::new(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: KeyDumpResponse = serde_json::from_str(&json).unwrap();

        assert!(deserialized.entries.contains_key("hits"));
        assert_eq!(deserialized.frontier.unwrap().physical, 500);
        assert!(deserialized.timestamps.is_empty());
        assert_eq!(deserialized.applied_origins["node-1"].physical, 500);
        assert_eq!(deserialized.merge_failed_keys, vec!["bad-key".to_string()]);
    }

    #[test]
    fn key_dump_response_without_frontier_deserialises() {
        // Backwards-compatibility: older peers may omit the frontier field
        // (and the session-guarantee fields added later).
        let json = r#"{"entries":{}}"#;
        let resp: KeyDumpResponse = serde_json::from_str(json).unwrap();
        assert!(resp.entries.is_empty());
        assert!(resp.frontier.is_none());
        assert!(resp.applied_origins.is_empty());
        assert!(resp.merge_failed_keys.is_empty());
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

        let mut applied_origins = HashMap::new();
        applied_origins.insert("node-1".to_string(), hlc(200, 0, "node-1"));
        let resp = DeltaSyncResponse {
            entries: vec![DeltaEntry {
                key: "key1".into(),
                value: CrdtValue::Counter(counter),
                hlc: hlc(200, 0, "node-1"),
            }],
            sender_frontier: Some(hlc(200, 0, "node-1")),
            applied_origins,
            merge_failed_keys: vec![],
            pruned_floor: Some(hlc(100, 0, "node-1")),
            visible_origins: HashMap::new(),
        };

        let json = serde_json::to_string(&resp).unwrap();
        let back: DeltaSyncResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(back.entries.len(), 1);
        assert_eq!(back.entries[0].key, "key1");
        assert_eq!(back.entries[0].hlc.physical, 200);
        assert_eq!(back.sender_frontier.unwrap().physical, 200);
        assert_eq!(back.applied_origins["node-1"].physical, 200);
        assert!(back.merge_failed_keys.is_empty());
        assert_eq!(back.pruned_floor.unwrap().physical, 100);
    }

    #[test]
    fn delta_sync_response_empty_entries() {
        let resp = DeltaSyncResponse {
            entries: vec![],
            sender_frontier: None,
            applied_origins: HashMap::new(),
            merge_failed_keys: vec![],
            pruned_floor: None,
            visible_origins: HashMap::new(),
        };

        let json = serde_json::to_string(&resp).unwrap();
        let back: DeltaSyncResponse = serde_json::from_str(&json).unwrap();

        assert!(back.entries.is_empty());
        assert!(back.sender_frontier.is_none());
    }

    /// Older peers omit the session-guarantee fields entirely — the JSON
    /// path must default them (bincode peers require lock-step upgrades,
    /// same as when `timestamps` was added to `KeyDumpResponse`).
    #[test]
    fn delta_sync_response_legacy_json_deserialises_with_defaults() {
        let json = r#"{"entries":[],"sender_frontier":null}"#;
        let back: DeltaSyncResponse = serde_json::from_str(json).unwrap();
        assert!(back.entries.is_empty());
        assert!(back.applied_origins.is_empty());
        assert!(back.merge_failed_keys.is_empty());
        assert!(back.pruned_floor.is_none());
    }

    /// The new session-guarantee fields must round-trip through bincode
    /// (the internal wire encoding).
    #[test]
    fn delta_sync_response_bincode_roundtrip_with_session_fields() {
        let mut applied_origins = HashMap::new();
        applied_origins.insert("origin-a".to_string(), hlc(42, 7, "origin-a"));
        let resp = DeltaSyncResponse {
            entries: vec![],
            sender_frontier: Some(hlc(42, 7, "origin-a")),
            applied_origins,
            merge_failed_keys: vec!["poisoned".into()],
            pruned_floor: Some(hlc(10, 0, "origin-a")),
            visible_origins: HashMap::new(),
        };
        let bytes = bincode::serde::encode_to_vec(&resp, bincode::config::standard()).unwrap();
        let (back, _): (DeltaSyncResponse, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(back.applied_origins["origin-a"], hlc(42, 7, "origin-a"));
        assert_eq!(back.merge_failed_keys, vec!["poisoned".to_string()]);
        assert_eq!(back.pruned_floor.unwrap(), hlc(10, 0, "origin-a"));
    }

    #[tokio::test]
    async fn pull_delta_from_unreachable_peer_returns_network_error() {
        let registry = shared_registry(vec![]);

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let client = SyncClient::with_client(registry, http_client);

        let result = client
            .pull_delta("127.0.0.1:1", "node-1", &hlc(0, 0, ""))
            .await;
        assert!(matches!(result, PullDeltaResult::NetworkError));
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

    // ---------------------------------------------------------------
    // PeerFrontierTracker tests
    // ---------------------------------------------------------------

    #[test]
    fn frontier_tracker_new_is_empty() {
        let tracker = PeerFrontierTracker::new();
        assert_eq!(tracker.peer_count(), 0);
        assert!(tracker.gc_frontier().is_none());
    }

    #[test]
    fn frontier_tracker_advance_and_get() {
        let mut tracker = PeerFrontierTracker::new();
        tracker.advance_frontier("peer-1:8000", hlc(100, 0, "node-1"));

        assert_eq!(tracker.peer_count(), 1);
        let f = tracker.frontier_for("peer-1:8000").unwrap();
        assert_eq!(f.physical, 100);
    }

    #[test]
    fn frontier_tracker_advance_only_moves_forward() {
        let mut tracker = PeerFrontierTracker::new();
        tracker.advance_frontier("peer-1:8000", hlc(200, 0, "node-1"));
        tracker.advance_frontier("peer-1:8000", hlc(100, 0, "node-1")); // older

        let f = tracker.frontier_for("peer-1:8000").unwrap();
        assert_eq!(f.physical, 200, "frontier should not regress");
    }

    #[test]
    fn frontier_tracker_remove_peer() {
        let mut tracker = PeerFrontierTracker::new();
        tracker.advance_frontier("peer-1:8000", hlc(100, 0, "node-1"));
        assert_eq!(tracker.peer_count(), 1);

        tracker.remove_peer("peer-1:8000");
        assert_eq!(tracker.peer_count(), 0);
        assert!(tracker.frontier_for("peer-1:8000").is_none());
    }

    #[test]
    fn frontier_tracker_gc_frontier_is_minimum() {
        let mut tracker = PeerFrontierTracker::new();
        tracker.advance_frontier("peer-1:8000", hlc(100, 0, "node-1"));
        tracker.advance_frontier("peer-2:8000", hlc(300, 0, "node-2"));
        tracker.advance_frontier("peer-3:8000", hlc(200, 0, "node-3"));

        let gc = tracker.advance_gc_frontier().unwrap();
        assert_eq!(gc.physical, 100, "GC frontier should be the minimum");
    }

    #[test]
    fn frontier_tracker_gc_frontier_none_when_empty() {
        let mut tracker = PeerFrontierTracker::new();
        assert!(tracker.advance_gc_frontier().is_none());
    }

    #[test]
    fn frontier_tracker_gc_frontier_advances_after_peer_catches_up() {
        let mut tracker = PeerFrontierTracker::new();
        tracker.advance_frontier("peer-1:8000", hlc(100, 0, "node-1"));
        tracker.advance_frontier("peer-2:8000", hlc(300, 0, "node-2"));

        let gc1 = tracker.advance_gc_frontier().unwrap().clone();
        assert_eq!(gc1.physical, 100);

        // Peer 1 catches up.
        tracker.advance_frontier("peer-1:8000", hlc(250, 0, "node-1"));
        let gc2 = tracker.advance_gc_frontier().unwrap().clone();
        assert_eq!(gc2.physical, 250, "GC frontier should advance");
    }

    #[test]
    fn frontier_tracker_default() {
        let tracker = PeerFrontierTracker::default();
        assert_eq!(tracker.peer_count(), 0);
    }

    // ---------------------------------------------------------------
    // Full sync fallback threshold tests
    // ---------------------------------------------------------------

    #[test]
    fn fallback_low_change_rate_returns_false() {
        // 10 changed out of 100 total = 10% < 50% threshold
        assert!(!super::should_fallback_to_full_sync(10, 100, 0.5));
    }

    #[test]
    fn fallback_at_threshold_returns_false() {
        // Exactly at 50% threshold should NOT trigger fallback (> not >=)
        assert!(!super::should_fallback_to_full_sync(50, 100, 0.5));
    }

    #[test]
    fn fallback_above_threshold_returns_true() {
        // 51 changed out of 100 total = 51% > 50% threshold
        assert!(super::should_fallback_to_full_sync(51, 100, 0.5));
    }

    #[test]
    fn fallback_all_keys_changed_returns_true() {
        // 100% change rate > any threshold < 1.0
        assert!(super::should_fallback_to_full_sync(100, 100, 0.5));
    }

    #[test]
    fn fallback_empty_store_returns_false() {
        // Empty store should never trigger fallback
        assert!(!super::should_fallback_to_full_sync(0, 0, 0.5));
    }

    #[test]
    fn fallback_zero_changes_returns_false() {
        // No changes should never trigger fallback
        assert!(!super::should_fallback_to_full_sync(0, 100, 0.5));
    }

    #[test]
    fn fallback_custom_threshold_low() {
        // With a 20% threshold, 25 out of 100 should trigger
        assert!(super::should_fallback_to_full_sync(25, 100, 0.2));
        assert!(!super::should_fallback_to_full_sync(15, 100, 0.2));
    }

    #[test]
    fn fallback_custom_threshold_high() {
        // With a 90% threshold, only very high rates should trigger
        assert!(!super::should_fallback_to_full_sync(89, 100, 0.9));
        assert!(super::should_fallback_to_full_sync(91, 100, 0.9));
    }

    #[test]
    fn fallback_default_threshold_constant() {
        assert!((super::DEFAULT_FULL_SYNC_THRESHOLD - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn max_delta_payload_bytes_is_512_kib() {
        assert_eq!(super::MAX_DELTA_PAYLOAD_BYTES, 512 * 1024);
    }

    // ---------------------------------------------------------------
    // Digest sync types serde
    // ---------------------------------------------------------------

    fn sample_digest_response() -> DigestSyncResponse {
        let mut counter = PnCounter::new();
        counter.increment(&nid("node-1"));
        let mut entries = HashMap::new();
        entries.insert("hits".to_string(), CrdtValue::Counter(counter));
        let mut timestamps = HashMap::new();
        timestamps.insert("hits".to_string(), hlc(500, 0, "node-1"));
        let mut applied_origins = HashMap::new();
        applied_origins.insert("node-1".to_string(), hlc(500, 0, "node-1"));

        DigestSyncResponse {
            scheme_ok: true,
            root_matched: false,
            mismatched_buckets: vec![3, 200],
            entries,
            timestamps,
            frontier: Some(hlc(500, 0, "node-1")),
            applied_origins,
            visible_origins: HashMap::new(),
            merge_failed_keys: vec!["bad-key".into()],
            total_keys: 42,
        }
    }

    #[test]
    fn digest_sync_request_bincode_roundtrip() {
        let req = DigestSyncRequest {
            sender: "node-1".to_string(),
            scheme_version: crate::store::digest::DIGEST_SCHEME_VERSION,
            root: vec![7u8; 32],
            buckets: vec![BucketDigestEntry {
                index: 12,
                digest: vec![9u8; 32],
            }],
            include_entries: true,
        };
        let bytes = bincode::serde::encode_to_vec(&req, bincode::config::standard()).unwrap();
        let (back, _): (DigestSyncRequest, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(back.sender, "node-1");
        assert_eq!(back.scheme_version, 1);
        assert_eq!(back.root, vec![7u8; 32]);
        assert_eq!(back.buckets.len(), 1);
        assert_eq!(back.buckets[0].index, 12);
        assert!(back.include_entries);
    }

    #[test]
    fn digest_sync_response_bincode_roundtrip() {
        let resp = sample_digest_response();
        let bytes = bincode::serde::encode_to_vec(&resp, bincode::config::standard()).unwrap();
        let (back, _): (DigestSyncResponse, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert!(back.scheme_ok);
        assert!(!back.root_matched);
        assert_eq!(back.mismatched_buckets, vec![3, 200]);
        assert!(back.entries.contains_key("hits"));
        assert_eq!(back.timestamps["hits"].physical, 500);
        assert_eq!(back.applied_origins["node-1"].physical, 500);
        assert_eq!(back.merge_failed_keys, vec!["bad-key".to_string()]);
        assert_eq!(back.total_keys, 42);
    }

    /// Fields missing from an older peer's JSON must default (serde
    /// forward compatibility for future trailing additions).
    #[test]
    fn digest_sync_response_legacy_json_deserialises_with_defaults() {
        let json = r#"{"scheme_ok":true,"root_matched":true}"#;
        let back: DigestSyncResponse = serde_json::from_str(json).unwrap();
        assert!(back.scheme_ok);
        assert!(back.root_matched);
        assert!(back.mismatched_buckets.is_empty());
        assert!(back.entries.is_empty());
        assert!(back.frontier.is_none());
        assert!(back.applied_origins.is_empty());
        assert_eq!(back.total_keys, 0);
    }

    #[test]
    fn digest_sync_request_from_digest_sends_only_non_empty_buckets() {
        use crate::store::digest::compute_store_digest;
        use std::collections::BTreeMap;

        let mut counter = PnCounter::new();
        counter.increment(&nid("node-1"));
        let mut data = BTreeMap::new();
        data.insert("hits".to_string(), CrdtValue::Counter(counter));
        let digest = compute_store_digest(&data);

        let req = DigestSyncRequest::from_digest("node-1", &digest, true);
        assert_eq!(
            req.scheme_version,
            crate::store::digest::DIGEST_SCHEME_VERSION
        );
        assert_eq!(req.root.len(), 32);
        assert_eq!(req.buckets.len(), 1, "single key → single non-empty bucket");
        assert_eq!(
            req.buckets[0].index as usize,
            crate::store::digest::bucket_of("hits")
        );
        assert_eq!(req.buckets[0].digest.len(), 32);
    }

    #[tokio::test]
    async fn digest_sync_to_unreachable_peer_returns_failed() {
        let registry = shared_registry(vec![]);
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let client = SyncClient::with_client(registry, http_client);

        let digest = crate::store::digest::compute_store_digest(&std::collections::BTreeMap::new());
        let req = DigestSyncRequest::from_digest("node-1", &digest, true);
        let result = client.digest_sync("127.0.0.1:1", &req).await;
        assert!(matches!(result, DigestSyncResult::Failed));
    }
}
