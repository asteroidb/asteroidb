//! `RaftNode`: the shared handle that executes core transitions, persists
//! state, applies committed entries to the system namespace, and exchanges
//! RPCs with peers.
//!
//! # Locking & ordering discipline
//!
//! - All state transitions run under a single `std::sync::Mutex` and are
//!   fully synchronous, INCLUDING persistence (fsync via `write_atomic`).
//!   This makes the core safety invariant structural: a vote/ack/proposal
//!   response literally cannot be produced before its hard-state/log write
//!   has completed, and a persistence failure abandons the response
//!   (fail-stop, never best-effort). The cost — a disk fsync on the
//!   executor thread — is acceptable for the low-frequency control plane
//!   and is documented in the ops guide (a slow disk can delay heartbeats
//!   and provoke a spurious election; the conservative default election
//!   timeouts absorb this).
//! - The namespace `RwLock` is only ever taken while already holding the
//!   node lock (consistent `inner -> namespace` order) and never across an
//!   await point.
//! - Outbound RPCs are collected during the transition and dispatched via
//!   `tokio::spawn` AFTER the node lock is released — the lock is never
//!   held across network IO.

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::control_plane::system_namespace::SystemNamespace;
use crate::error::CrdtError;
use crate::ops::write_atomic;
use crate::types::NodeId;

use super::core::{Effect, HardState, OutboundRpc, RaftCore, SnapshotMeta};
use super::state_machine;
use super::storage::RaftStorage;
use super::transport::RaftTransport;
use super::types::{
    AppendEntriesRequest, AppendEntriesResponse, ApplyOutcome, AuthoritySpec, ControlPlaneCommand,
    ControlPlaneState, InstallSnapshotRequest, InstallSnapshotResponse, PolicySpec,
    RequestVoteRequest, RequestVoteResponse,
};

/// Tuning knobs. Defaults are conservative, sized for high-latency links
/// (the paper's 150-300ms assumes ~15ms broadcast time — never use those
/// values without measuring the deployment's RTT).
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub propose_timeout: Duration,
    /// Compact the log (fold applied entries into the snapshot) once the
    /// tail exceeds this many entries.
    pub log_max: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(5_000),
            election_timeout_max: Duration::from_millis(10_000),
            heartbeat_interval: Duration::from_millis(1_000),
            propose_timeout: Duration::from_millis(30_000),
            log_max: 4096,
        }
    }
}

/// Snapshot of the node's consensus status (for the status endpoint).
#[derive(Debug, Clone)]
pub struct RaftStatus {
    pub node_id: String,
    pub role: String,
    pub term: u64,
    pub leader_id: Option<String>,
    pub leader_addr: Option<String>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub last_log_index: u64,
    pub voters: Vec<String>,
}

type Waiter = (u64, oneshot::Sender<Result<ApplyOutcome, CrdtError>>);

/// Sidecar marker persisted next to `system_namespace.json` after every
/// successful namespace persist: the raft log index whose apply produced
/// that JSON, plus the namespace version it carried (to tie the marker to
/// the exact JSON incarnation). At startup it proves the JSON-restored
/// namespace is at-or-beyond the compacted snapshot, so the snapshot must
/// NOT be installed over it (that would roll back committed state until a
/// leader re-advances the commit index).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct NamespaceApplyMarker {
    applied_index: u64,
    ns_version: u64,
}

/// `<ns_path>.applied` (e.g. `system_namespace.json.applied`).
fn apply_marker_path(ns_path: &std::path::Path) -> PathBuf {
    let mut name = ns_path.file_name().unwrap_or_default().to_os_string();
    name.push(".applied");
    ns_path.with_file_name(name)
}

/// Best-effort marker load; any failure (missing, corrupt) simply means
/// "cannot prove freshness" and the caller falls back to installing the
/// snapshot (the previous, conservative behaviour).
fn load_apply_marker(ns_path: &std::path::Path) -> Option<NamespaceApplyMarker> {
    let data = std::fs::read_to_string(apply_marker_path(ns_path)).ok()?;
    serde_json::from_str(&data).ok()
}

struct Inner {
    core: RaftCore,
    /// Replicated state at `core.last_applied`.
    state: ControlPlaneState,
    /// Replicated state at `core.snapshot_meta` (what `log.json` and
    /// InstallSnapshot carry). Always in sync with `core.snapshot_meta`.
    snapshot_state: ControlPlaneState,
    /// Proposal waiters keyed by log index, holding the term the entry was
    /// proposed in: a waiter succeeds only if the entry that eventually
    /// commits at that index still carries that term.
    waiters: HashMap<u64, Waiter>,
    /// Set when a hard-state / log save failed AFTER the in-memory state
    /// was already mutated. While dirty, NO response may be produced from
    /// memory alone (a retransmitted RPC would otherwise be answered with
    /// unrecorded votes/entries); every transition first re-flushes.
    hard_dirty: bool,
    log_dirty: bool,
}

/// Fully-built outbound message (dispatched outside the lock).
enum Outbound {
    Vote {
        to: NodeId,
        req: RequestVoteRequest,
    },
    Append {
        to: NodeId,
        req: AppendEntriesRequest,
    },
    Snapshot {
        to: NodeId,
        req: InstallSnapshotRequest,
    },
}

pub struct RaftNode {
    inner: Mutex<Inner>,
    storage: Arc<dyn RaftStorage>,
    transport: Arc<dyn RaftTransport>,
    namespace: Arc<RwLock<SystemNamespace>>,
    namespace_persist_path: Option<PathBuf>,
    config: RaftConfig,
    self_id: NodeId,
    voters: BTreeSet<NodeId>,
    /// Notifies the driver to reset its randomized election timer.
    election_reset: tokio::sync::Notify,
}

impl RaftNode {
    /// Restore (or freshly initialize) a Raft node.
    ///
    /// Loads `HardState`, the snapshot, and the log tail from `storage`
    /// (an `Err` is a fail-stop condition for the caller: booting with
    /// damaged Raft state risks double voting). When a compacted snapshot
    /// exists AND the locally persisted namespace cannot be proven (via the
    /// apply marker) to be at-or-beyond it, the snapshot's replicated core
    /// is installed into the namespace before returning, so the
    /// `NodeRunner` initializes its version tracking against the restored
    /// state. Entries beyond the snapshot re-apply once they are
    /// (re-)learned committed — idempotent upsert/remove replay over the
    /// `system_namespace.json`-restored view.
    ///
    /// A single-voter cluster elects itself immediately (deterministic
    /// startup, preserves the standalone-node write availability).
    pub fn new(
        self_id: NodeId,
        voters: BTreeSet<NodeId>,
        config: RaftConfig,
        storage: Arc<dyn RaftStorage>,
        transport: Arc<dyn RaftTransport>,
        namespace: Arc<RwLock<SystemNamespace>>,
        namespace_persist_path: Option<PathBuf>,
    ) -> Result<Arc<Self>, String> {
        let restored = storage.load()?;
        let (hard, snapshot_meta, snapshot_state, entries) = match restored {
            Some(p) => (p.hard, p.snapshot_meta, p.snapshot_state, p.entries),
            None => (
                HardState::default(),
                SnapshotMeta::default(),
                ControlPlaneState::default(),
                Vec::new(),
            ),
        };

        let core = RaftCore::new(
            self_id.clone(),
            voters.clone(),
            config.election_timeout_min,
            hard,
            snapshot_meta,
            entries,
        );

        // Install the compacted snapshot into the namespace projection —
        // but ONLY when the locally persisted namespace view cannot be
        // proven to be at-or-beyond the snapshot. The JSON is persisted
        // after every apply, so it usually holds committed state NEWER than
        // the snapshot (entries N+1..M applied after the last compaction at
        // N); installing over it would roll those committed changes back
        // until a leader re-establishes the commit index (indefinitely
        // without quorum). Replaying the tail over the newer view is safe:
        // versions are assigned from the replicated counter, so re-applies
        // are idempotent upserts. When no compaction has happened yet
        // (index 0) the namespace always keeps its locally persisted view.
        if snapshot_meta.last_included_index > 0 {
            let ns_at_or_beyond = namespace_persist_path
                .as_deref()
                .and_then(load_apply_marker)
                .is_some_and(|marker| {
                    marker.applied_index >= snapshot_meta.last_included_index
                        && marker.ns_version
                            == namespace
                                .read()
                                .unwrap_or_else(|e| e.into_inner())
                                .version()
                                .0
                });
            if ns_at_or_beyond {
                tracing::info!(
                    snapshot_index = snapshot_meta.last_included_index,
                    "keeping the persisted namespace view (at or beyond the \
                     raft snapshot); committed entries replay over it"
                );
            } else {
                let mut ns = namespace.write().unwrap_or_else(|e| e.into_inner());
                state_machine::install(&snapshot_state, &mut ns);
            }
        }

        let node = Arc::new(Self {
            inner: Mutex::new(Inner {
                core,
                state: snapshot_state.clone(),
                snapshot_state,
                waiters: HashMap::new(),
                hard_dirty: false,
                log_dirty: false,
            }),
            storage,
            transport,
            namespace,
            namespace_persist_path,
            config,
            self_id: self_id.clone(),
            voters: voters.clone(),
            election_reset: tokio::sync::Notify::new(),
        });

        if voters.len() == 1 && voters.contains(&self_id) {
            // No peers to contact: the whole election (persist term/vote,
            // append + commit + apply the Noop, propose Bootstrap) runs
            // synchronously here.
            node.on_election_timeout();
        }

        Ok(node)
    }

    pub fn config(&self) -> &RaftConfig {
        &self.config
    }

    pub fn is_voter(&self) -> bool {
        self.voters.contains(&self.self_id)
    }

    pub fn voters(&self) -> &BTreeSet<NodeId> {
        &self.voters
    }

    /// Await-able election timer reset signal (driver).
    pub fn election_reset_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.election_reset.notified()
    }

    pub fn is_leader(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .core
            .is_leader()
    }

    /// Best-known leader `(id, resolved address)` for NotLeader hints.
    pub fn leader_hint(&self) -> Option<(NodeId, Option<String>)> {
        let hint = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .core
            .leader_hint
            .clone()?;
        let addr = self.transport.resolve_addr(&hint);
        Some((hint, addr))
    }

    pub fn status(&self) -> RaftStatus {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let leader_id = inner.core.leader_hint.clone();
        let leader_addr = leader_id.as_ref().and_then(|id| {
            if *id == self.self_id {
                None
            } else {
                self.transport.resolve_addr(id)
            }
        });
        RaftStatus {
            node_id: self.self_id.0.clone(),
            role: inner.core.role_name().to_string(),
            term: inner.core.hard.current_term,
            leader_id: leader_id.map(|id| id.0),
            leader_addr,
            commit_index: inner.core.commit_index,
            last_applied: inner.core.last_applied,
            last_log_index: inner.core.last_log_index(),
            voters: self.voters.iter().map(|v| v.0.clone()).collect(),
        }
    }

    // -----------------------------------------------------------
    // RPC receivers (HTTP handlers / ChannelTransport call these)
    // -----------------------------------------------------------

    /// Handle a RequestVote RPC. A granted vote is durably recorded
    /// (fsync) before this returns; a persistence failure returns `Err`
    /// and NO response is produced (the candidate simply retries).
    pub fn handle_request_vote(
        self: &Arc<Self>,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, CrdtError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let (resp, effects) = inner.core.handle_request_vote(&req, Instant::now());
        let outbound = self.run_effects(&mut inner, effects)?;
        drop(inner);
        self.dispatch(outbound);
        Ok(resp)
    }

    /// Handle an AppendEntries RPC. Appended entries are durably recorded
    /// before the (success) ack is produced.
    pub fn handle_append_entries(
        self: &Arc<Self>,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, CrdtError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let (resp, effects) = inner.core.handle_append_entries(&req, Instant::now());
        let outbound = self.run_effects(&mut inner, effects)?;
        drop(inner);
        self.dispatch(outbound);
        Ok(resp)
    }

    /// Handle an InstallSnapshot RPC (single-message lite variant).
    pub fn handle_install_snapshot(
        self: &Arc<Self>,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, CrdtError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let last_included = req.last_included_index;
        let (resp, install, effects) = inner.core.handle_install_snapshot_meta(
            req.term,
            &req.leader_id,
            req.last_included_index,
            req.last_included_term,
            Instant::now(),
        );
        if install {
            inner.snapshot_state = req.state.clone();
            inner.state = req.state;
            inner.core.last_applied = last_included;
            {
                let mut ns = self.namespace.write().unwrap_or_else(|e| e.into_inner());
                state_machine::install(&inner.state, &mut ns);
            }
        }
        let outbound = self.run_effects(&mut inner, effects)?;
        drop(inner);
        if install {
            self.persist_namespace_best_effort(last_included);
        }
        self.dispatch(outbound);
        Ok(resp)
    }

    // -----------------------------------------------------------
    // Driver entry points
    // -----------------------------------------------------------

    /// Election timer fired (or forced). Safe to call at any time.
    pub fn on_election_timeout(self: &Arc<Self>) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let effects = inner.core.on_election_timeout(Instant::now());
        let outbound = match self.run_effects(&mut inner, effects) {
            Ok(o) => o,
            Err(e) => {
                tracing::error!(error = %e, "raft election aborted: persistence failure");
                return;
            }
        };
        drop(inner);
        self.dispatch(outbound);
    }

    /// Heartbeat tick: when leader, (re-)send AppendEntries/InstallSnapshot
    /// to every peer according to its progress.
    pub fn on_heartbeat_tick(self: &Arc<Self>) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if !inner.core.is_leader() {
            return;
        }
        let effects = inner.core.broadcast_append();
        let outbound = match self.run_effects(&mut inner, effects) {
            Ok(o) => o,
            Err(e) => {
                tracing::error!(error = %e, "raft heartbeat aborted: persistence failure");
                return;
            }
        };
        drop(inner);
        self.dispatch(outbound);
    }

    // -----------------------------------------------------------
    // Proposals
    // -----------------------------------------------------------

    /// Propose a command and wait until it is committed AND applied (or
    /// fails). Non-leaders get an immediate `NotLeader` with a hint.
    pub async fn propose_and_wait(
        self: &Arc<Self>,
        command: ControlPlaneCommand,
    ) -> Result<ApplyOutcome, CrdtError> {
        let (index, term, rx, outbound) = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let Some(((index, term), effects)) = inner.core.propose(command) else {
                return Err(self.not_leader_error(&inner));
            };
            let (tx, rx) = oneshot::channel();
            inner.waiters.insert(index, (term, tx));
            let outbound = match self.run_effects(&mut inner, effects) {
                Ok(o) => o,
                Err(e) => {
                    inner.waiters.remove(&index);
                    return Err(e);
                }
            };
            (index, term, rx, outbound)
        };
        self.dispatch(outbound);

        match tokio::time::timeout(self.config.propose_timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(CrdtError::Internal(
                "control-plane proposal waiter dropped".into(),
            )),
            Err(_) => {
                // Remove OUR waiter only: after a step-down (which drained
                // this waiter) and a later re-election, a NEWER proposal may
                // be registered at the same log index — removing by index
                // alone would drop that unrelated request's sender and fail
                // it spuriously. The stored term identifies the proposal.
                let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                if inner
                    .waiters
                    .get(&index)
                    .is_some_and(|(waiter_term, _)| *waiter_term == term)
                {
                    inner.waiters.remove(&index);
                }
                // Commit could not be reached in time — typically the
                // majority is unreachable (minority side of a partition).
                Err(CrdtError::Timeout)
            }
        }
    }

    fn not_leader_error(&self, inner: &Inner) -> CrdtError {
        let leader_id = inner
            .core
            .leader_hint
            .clone()
            .filter(|id| *id != self.self_id);
        let leader_addr = leader_id
            .as_ref()
            .and_then(|id| self.transport.resolve_addr(id));
        CrdtError::NotLeader {
            leader_id: leader_id.map(|id| id.0),
            leader_addr,
        }
    }

    // -----------------------------------------------------------
    // Effect execution (the safety-ordering heart)
    // -----------------------------------------------------------

    /// Execute effects in order. Persistence effects run synchronously and
    /// a failure aborts everything that follows (including the caller's
    /// response). Send effects are only collected — dispatch happens after
    /// the lock is released.
    fn run_effects(
        self: &Arc<Self>,
        inner: &mut Inner,
        effects: Vec<Effect>,
    ) -> Result<Vec<Outbound>, CrdtError> {
        // A previous transition may have mutated memory and then failed to
        // persist (its response was suppressed). Nothing may be answered —
        // not even an "unchanged" duplicate — until that state is durable.
        self.flush_dirty(inner)?;
        let mut outbound = Vec::new();
        for effect in effects {
            match effect {
                Effect::PersistHardState => {
                    inner.hard_dirty = true;
                    self.storage
                        .save_hard_state(&inner.core.hard)
                        .map_err(|e| CrdtError::Storage(format!("raft hard state: {e}")))?;
                    inner.hard_dirty = false;
                }
                Effect::PersistLog => {
                    inner.log_dirty = true;
                    self.storage
                        .save_log(
                            &inner.core.snapshot_meta,
                            &inner.snapshot_state,
                            &inner.core.log,
                        )
                        .map_err(|e| CrdtError::Storage(format!("raft log: {e}")))?;
                    inner.log_dirty = false;
                }
                Effect::Send(to, rpc) => {
                    outbound.push(self.build_outbound(inner, to, rpc));
                }
                Effect::ApplyCommitted => {
                    self.apply_committed(inner)?;
                }
                Effect::ResetElectionTimer => {
                    self.election_reset.notify_waiters();
                }
                Effect::SteppedDown => {
                    let err = self.not_leader_error(inner);
                    for (_, (_, tx)) in inner.waiters.drain() {
                        let _ = tx.send(Err(err.clone()));
                    }
                }
                Effect::BecameLeader => {
                    tracing::info!(
                        term = inner.core.hard.current_term,
                        node = %self.self_id.0,
                        "won control-plane raft election"
                    );
                    // One-shot Bootstrap: import this (first) leader's local
                    // replicated core so all nodes converge on it.
                    if !inner.state.bootstrapped {
                        let command = self.build_bootstrap_command();
                        if let Some((_, effects)) = inner.core.propose(command) {
                            outbound.extend(self.run_effects(inner, effects)?);
                        }
                    }
                }
            }
        }
        Ok(outbound)
    }

    /// Re-flush state whose save previously failed. Called before every
    /// transition; an `Err` keeps suppressing responses (fail-stop posture).
    fn flush_dirty(&self, inner: &mut Inner) -> Result<(), CrdtError> {
        if inner.hard_dirty {
            self.storage
                .save_hard_state(&inner.core.hard)
                .map_err(|e| CrdtError::Storage(format!("raft hard state: {e}")))?;
            inner.hard_dirty = false;
        }
        if inner.log_dirty {
            self.storage
                .save_log(
                    &inner.core.snapshot_meta,
                    &inner.snapshot_state,
                    &inner.core.log,
                )
                .map_err(|e| CrdtError::Storage(format!("raft log: {e}")))?;
            inner.log_dirty = false;
        }
        Ok(())
    }

    fn build_outbound(&self, inner: &Inner, to: NodeId, rpc: OutboundRpc) -> Outbound {
        match rpc {
            OutboundRpc::Vote(req) => Outbound::Vote { to, req },
            OutboundRpc::Append(req) => Outbound::Append { to, req },
            // Built under the lock so meta and state cannot diverge.
            OutboundRpc::Snapshot => Outbound::Snapshot {
                to,
                req: InstallSnapshotRequest {
                    term: inner.core.hard.current_term,
                    leader_id: self.self_id.clone(),
                    last_included_index: inner.core.snapshot_meta.last_included_index,
                    last_included_term: inner.core.snapshot_meta.last_included_term,
                    state: inner.snapshot_state.clone(),
                },
            },
        }
    }

    /// Build the reset-and-import `Bootstrap` command from the local
    /// namespace (policies + manual authority definitions + version floor).
    fn build_bootstrap_command(&self) -> ControlPlaneCommand {
        let ns = self.namespace.read().unwrap_or_else(|e| e.into_inner());
        let mut policies: Vec<PolicySpec> = ns
            .all_placement_policies()
            .into_iter()
            .map(PolicySpec::from_policy)
            .collect();
        policies.sort_by(|a, b| a.prefix.cmp(&b.prefix));
        let mut authorities: Vec<AuthoritySpec> = ns
            .all_authority_definitions()
            .into_iter()
            .filter(|def| !def.auto_generated)
            .map(AuthoritySpec::from_definition)
            .collect();
        authorities.sort_by(|a, b| a.prefix.cmp(&b.prefix));
        ControlPlaneCommand::Bootstrap {
            version_floor: ns.version().0,
            policies,
            authorities,
        }
    }

    /// Apply entries `last_applied+1 ..= commit_index` to the replicated
    /// state and the namespace projection, resolve waiters, compact when
    /// due, and best-effort persist the namespace.
    fn apply_committed(&self, inner: &mut Inner) -> Result<(), CrdtError> {
        let commit = inner.core.commit_index;
        let mut applied_any = false;
        while inner.core.last_applied < commit {
            let idx = inner.core.last_applied + 1;
            let entry = inner
                .core
                .entry_at(idx)
                .expect("committed entry must be present in the log tail")
                .clone();
            let outcome = {
                let mut ns = self.namespace.write().unwrap_or_else(|e| e.into_inner());
                state_machine::apply(&entry, &mut inner.state, &mut ns)
            };
            inner.core.last_applied = idx;
            applied_any = true;
            if let Some((term, tx)) = inner.waiters.remove(&idx) {
                let result = if term == entry.term {
                    Ok(outcome)
                } else {
                    // The proposed entry was overwritten by another leader.
                    Err(self.not_leader_error(inner))
                };
                let _ = tx.send(result);
            }
        }

        // Compaction: fold everything applied into the snapshot when the
        // tail grows beyond log_max. Never touches unapplied (and therefore
        // uncommitted) entries.
        if inner.core.log.len() > self.config.log_max {
            self.compact(inner);
        }

        if applied_any {
            self.persist_namespace_best_effort(inner.core.last_applied);
        }
        Ok(())
    }

    fn compact(&self, inner: &mut Inner) {
        let target = inner.core.last_applied;
        if target <= inner.core.snapshot_meta.last_included_index {
            return;
        }
        let term = inner
            .core
            .term_at(target)
            .expect("applied index is within the log");
        inner.core.drop_log_through(target);
        inner.core.snapshot_meta = SnapshotMeta {
            last_included_index: target,
            last_included_term: term,
        };
        inner.snapshot_state = inner.state.clone();
        // Single-file co-persist: snapshot and remaining tail land
        // atomically. On failure the in-memory compaction stands while the
        // durable file keeps the longer (still complete) log — safe either
        // way; the next successful save catches up.
        if let Err(e) = self.storage.save_log(
            &inner.core.snapshot_meta,
            &inner.snapshot_state,
            &inner.core.log,
        ) {
            tracing::error!(error = %e, "raft log compaction persist failed");
        } else {
            tracing::info!(
                last_included_index = target,
                tail_len = inner.core.log.len(),
                "compacted control-plane raft log"
            );
        }
    }

    /// Best-effort `system_namespace.json` write. The namespace is a
    /// projection reconstructible from the raft snapshot + log, so failures
    /// are logged, not fatal (matches `persist_namespace`'s posture).
    ///
    /// `applied_index` is the raft log index whose apply produced this
    /// namespace state; it is recorded in the sidecar marker (written only
    /// after the namespace write succeeded) so the next startup can prove
    /// the JSON view is at-or-beyond the compacted snapshot.
    fn persist_namespace_best_effort(&self, applied_index: u64) {
        let Some(path) = &self.namespace_persist_path else {
            return;
        };
        let (json, ns_version) = {
            let ns = self.namespace.read().unwrap_or_else(|e| e.into_inner());
            match serde_json::to_string_pretty(&*ns) {
                Ok(j) => (j, ns.version().0),
                Err(e) => {
                    tracing::warn!(error = %e, "failed to serialise system namespace");
                    return;
                }
            }
        };
        if let Err(e) = write_atomic(path, json.as_bytes()) {
            tracing::warn!(error = %e, "failed to persist system namespace after raft apply");
            return;
        }
        // Marker strictly AFTER the namespace write: a stale (lower) marker
        // merely causes a conservative snapshot install at the next boot,
        // never a skipped one over stale JSON.
        let marker = NamespaceApplyMarker {
            applied_index,
            ns_version,
        };
        match serde_json::to_vec(&marker) {
            Ok(bytes) => {
                if let Err(e) = write_atomic(&apply_marker_path(path), &bytes) {
                    tracing::warn!(error = %e, "failed to persist namespace apply marker");
                }
            }
            Err(e) => tracing::warn!(error = %e, "failed to serialise namespace apply marker"),
        }
    }

    // -----------------------------------------------------------
    // Outbound dispatch & response feedback
    // -----------------------------------------------------------

    fn dispatch(self: &Arc<Self>, outbound: Vec<Outbound>) {
        for message in outbound {
            let node = Arc::clone(self);
            tokio::spawn(async move {
                match message {
                    Outbound::Vote { to, req } => {
                        let term = req.term;
                        match node.transport.request_vote(to.clone(), req).await {
                            Ok(resp) => node.on_vote_response(term, to, resp),
                            Err(e) => {
                                tracing::debug!(peer = %to.0, error = %e, "raft vote rpc failed")
                            }
                        }
                    }
                    Outbound::Append { to, req } => {
                        let term = req.term;
                        match node.transport.append_entries(to.clone(), req).await {
                            Ok(resp) => node.on_append_response(term, to, resp),
                            Err(e) => {
                                tracing::debug!(peer = %to.0, error = %e, "raft append rpc failed")
                            }
                        }
                    }
                    Outbound::Snapshot { to, req } => {
                        let term = req.term;
                        let last_included = req.last_included_index;
                        match node.transport.install_snapshot(to.clone(), req).await {
                            Ok(resp) => node.on_snapshot_response(term, to, resp, last_included),
                            Err(e) => {
                                tracing::debug!(peer = %to.0, error = %e, "raft snapshot rpc failed")
                            }
                        }
                    }
                }
            });
        }
    }

    fn on_vote_response(self: &Arc<Self>, term_sent: u64, from: NodeId, resp: RequestVoteResponse) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let effects = inner.core.handle_vote_response(term_sent, from, &resp);
        let outbound = match self.run_effects(&mut inner, effects) {
            Ok(o) => o,
            Err(e) => {
                tracing::error!(error = %e, "raft vote response handling aborted");
                return;
            }
        };
        drop(inner);
        self.dispatch(outbound);
    }

    fn on_append_response(
        self: &Arc<Self>,
        term_sent: u64,
        from: NodeId,
        resp: AppendEntriesResponse,
    ) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let effects = inner.core.handle_append_response(term_sent, from, &resp);
        let outbound = match self.run_effects(&mut inner, effects) {
            Ok(o) => o,
            Err(e) => {
                tracing::error!(error = %e, "raft append response handling aborted");
                return;
            }
        };
        drop(inner);
        self.dispatch(outbound);
    }

    fn on_snapshot_response(
        self: &Arc<Self>,
        term_sent: u64,
        to: NodeId,
        resp: InstallSnapshotResponse,
        last_included: u64,
    ) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let effects = inner
            .core
            .handle_snapshot_ack(term_sent, to, resp.term, last_included);
        let outbound = match self.run_effects(&mut inner, effects) {
            Ok(o) => o,
            Err(e) => {
                tracing::error!(error = %e, "raft snapshot response handling aborted");
                return;
            }
        };
        drop(inner);
        self.dispatch(outbound);
    }
}
