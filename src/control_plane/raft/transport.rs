//! Transport abstraction for Raft RPCs.
//!
//! The production implementation is `network::raft_transport::HttpRaftTransport`
//! (internal HTTP endpoints with Bearer token + bincode/JSON fallback). The
//! in-process [`ChannelTransport`] connects `RaftNode`s directly and supports
//! partition injection; it is compiled into the normal build so integration
//! tests under `tests/` can drive multi-node safety scenarios.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};

use crate::types::NodeId;

use super::node::RaftNode;
use super::types::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};

/// A peer could not be reached (or refused the request at the HTTP layer,
/// e.g. an old node returning 404 for the raft endpoints during a rolling
/// upgrade). The round is skipped and retried on the next tick — an
/// unreachable peer can only DELAY consensus, never corrupt it.
#[derive(Debug, Clone)]
pub struct TransportError(pub String);

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "raft transport error: {}", self.0)
    }
}

impl std::error::Error for TransportError {}

pub type TransportResult<T> = Result<T, TransportError>;

/// Boxed future so the trait stays object-safe (`Arc<dyn RaftTransport>`).
pub type TransportFuture<'a, T> = Pin<Box<dyn Future<Output = TransportResult<T>> + Send + 'a>>;

pub trait RaftTransport: Send + Sync {
    fn request_vote(
        &self,
        to: NodeId,
        req: RequestVoteRequest,
    ) -> TransportFuture<'_, RequestVoteResponse>;

    fn append_entries(
        &self,
        to: NodeId,
        req: AppendEntriesRequest,
    ) -> TransportFuture<'_, AppendEntriesResponse>;

    fn install_snapshot(
        &self,
        to: NodeId,
        req: InstallSnapshotRequest,
    ) -> TransportFuture<'_, InstallSnapshotResponse>;

    /// Best-effort synchronous address resolution for NotLeader hints.
    fn resolve_addr(&self, id: &NodeId) -> Option<String>;
}

/// A transport that reaches nobody. Used by single-voter test nodes (which
/// never need to send RPCs) and as a safe default.
pub struct NoopTransport;

impl RaftTransport for NoopTransport {
    fn request_vote(
        &self,
        to: NodeId,
        _req: RequestVoteRequest,
    ) -> TransportFuture<'_, RequestVoteResponse> {
        Box::pin(async move { Err(TransportError(format!("no transport to {}", to.0))) })
    }

    fn append_entries(
        &self,
        to: NodeId,
        _req: AppendEntriesRequest,
    ) -> TransportFuture<'_, AppendEntriesResponse> {
        Box::pin(async move { Err(TransportError(format!("no transport to {}", to.0))) })
    }

    fn install_snapshot(
        &self,
        to: NodeId,
        _req: InstallSnapshotRequest,
    ) -> TransportFuture<'_, InstallSnapshotResponse> {
        Box::pin(async move { Err(TransportError(format!("no transport to {}", to.0))) })
    }

    fn resolve_addr(&self, _id: &NodeId) -> Option<String> {
        None
    }
}

// ---------------------------------------------------------------
// In-process channel transport (multi-node tests)
// ---------------------------------------------------------------

/// Shared in-process "network" connecting registered `RaftNode`s, with
/// directional link blocking for partition injection.
#[derive(Default)]
pub struct ChannelNetwork {
    nodes: Mutex<HashMap<NodeId, Weak<RaftNode>>>,
    /// Blocked directed links `(from, to)`.
    blocked: Mutex<HashSet<(NodeId, NodeId)>>,
}

impl ChannelNetwork {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register a node under its ID (call after constructing the node).
    pub fn register(&self, id: NodeId, node: &Arc<RaftNode>) {
        self.nodes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, Arc::downgrade(node));
    }

    /// Build the transport handle for `self_id` (pass to `RaftNode::new`).
    pub fn transport_for(self: &Arc<Self>, self_id: NodeId) -> Arc<ChannelTransport> {
        Arc::new(ChannelTransport {
            network: Arc::clone(self),
            self_id,
        })
    }

    /// Cut both directions between `a` and `b`.
    pub fn block_pair(&self, a: &NodeId, b: &NodeId) {
        let mut blocked = self.blocked.lock().unwrap_or_else(|e| e.into_inner());
        blocked.insert((a.clone(), b.clone()));
        blocked.insert((b.clone(), a.clone()));
    }

    /// Partition the network into two sides: every cross-side link is cut.
    pub fn partition(&self, side_a: &[NodeId], side_b: &[NodeId]) {
        for a in side_a {
            for b in side_b {
                self.block_pair(a, b);
            }
        }
    }

    /// Isolate one node from everyone else.
    pub fn isolate(&self, id: &NodeId) {
        let others: Vec<NodeId> = self
            .nodes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .filter(|k| *k != id)
            .cloned()
            .collect();
        for other in &others {
            self.block_pair(id, other);
        }
    }

    /// Remove all link blocks (heal every partition).
    pub fn heal_all(&self) {
        self.blocked
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    fn allowed(&self, from: &NodeId, to: &NodeId) -> bool {
        !self
            .blocked
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&(from.clone(), to.clone()))
    }

    fn get(&self, id: &NodeId) -> Option<Arc<RaftNode>> {
        self.nodes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .and_then(Weak::upgrade)
    }

    fn deliver<T>(
        &self,
        from: &NodeId,
        to: &NodeId,
        call: impl FnOnce(&Arc<RaftNode>) -> Result<T, crate::error::CrdtError>,
    ) -> TransportResult<T> {
        if !self.allowed(from, to) {
            return Err(TransportError(format!(
                "link {} -> {} is partitioned",
                from.0, to.0
            )));
        }
        let node = self
            .get(to)
            .ok_or_else(|| TransportError(format!("node {} not registered", to.0)))?;
        let result = call(&node).map_err(|e| TransportError(e.to_string()))?;
        // The response leg travels the same (bidirectionally blocked) link.
        if !self.allowed(to, from) {
            return Err(TransportError(format!(
                "link {} -> {} is partitioned (response)",
                to.0, from.0
            )));
        }
        Ok(result)
    }
}

/// Per-node handle into a [`ChannelNetwork`].
pub struct ChannelTransport {
    network: Arc<ChannelNetwork>,
    self_id: NodeId,
}

impl RaftTransport for ChannelTransport {
    fn request_vote(
        &self,
        to: NodeId,
        req: RequestVoteRequest,
    ) -> TransportFuture<'_, RequestVoteResponse> {
        Box::pin(async move {
            self.network
                .deliver(&self.self_id, &to, |node| node.handle_request_vote(req))
        })
    }

    fn append_entries(
        &self,
        to: NodeId,
        req: AppendEntriesRequest,
    ) -> TransportFuture<'_, AppendEntriesResponse> {
        Box::pin(async move {
            self.network
                .deliver(&self.self_id, &to, |node| node.handle_append_entries(req))
        })
    }

    fn install_snapshot(
        &self,
        to: NodeId,
        req: InstallSnapshotRequest,
    ) -> TransportFuture<'_, InstallSnapshotResponse> {
        Box::pin(async move {
            self.network
                .deliver(&self.self_id, &to, |node| node.handle_install_snapshot(req))
        })
    }

    fn resolve_addr(&self, id: &NodeId) -> Option<String> {
        // In-process transport has no real addresses; return a synthetic one
        // so NotLeader hints stay exercised in tests.
        self.network.get(id).map(|_| format!("channel://{}", id.0))
    }
}
