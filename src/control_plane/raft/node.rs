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
use std::sync::atomic::{AtomicU64, Ordering};
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
    ControlPlaneState, InstallSnapshotRequest, InstallSnapshotResponse, NamespaceSnapshotRequest,
    NamespaceSnapshotResponse, PolicySpec, RequestVoteRequest, RequestVoteResponse,
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
    /// Non-voter (observer) namespace pull interval (M-17): how often an
    /// observer fetches a voter's committed control-plane state so its
    /// namespace projection — and therefore the policy version its
    /// authority signatures carry — keeps following policy bumps.
    /// `Duration::ZERO` disables the pull loop (test/repro use only:
    /// a disabled pull re-freezes the observer namespace).
    pub observer_pull_interval: Duration,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(5_000),
            election_timeout_max: Duration::from_millis(10_000),
            heartbeat_interval: Duration::from_millis(1_000),
            propose_timeout: Duration::from_millis(30_000),
            log_max: 4096,
            observer_pull_interval: Duration::from_millis(5_000),
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
    /// Cumulative successful observer namespace pulls (M-17; 0 on voters).
    pub observer_ns_pull_success_total: u64,
    /// Cumulative failed observer namespace pulls (M-17; 0 on voters).
    pub observer_ns_pull_failure_total: u64,
    /// Wall-clock ms of the last successful pull (0 = never pulled).
    pub observer_ns_last_pull_unix_ms: u64,
    /// The replicated version counter of the local control-plane state —
    /// on an observer, compare against a voter's to gauge namespace lag.
    pub observer_ns_version_counter: u64,
}

type Waiter = (u64, oneshot::Sender<Result<ApplyOutcome, CrdtError>>);

/// Outcome of an adoption attempt on a pulled committed snapshot (M-17).
///
/// The distinction matters for the pull metrics: `Adopted` and `NotNewer`
/// are HEALTHY rounds (they prove a voter was reached and answered with
/// its committed state — counted as success, refreshing the pull-age
/// freshness signal), while `RejectedResponder` is a misconfiguration
/// that leaves the namespace frozen and must therefore count as a FAILED
/// pull — otherwise `observer_ns_last_pull_unix_ms` would stay green while
/// the observer silently re-freezes (the exact posture M-17 removes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdoptOutcome {
    /// Strictly newer committed state was installed (and persisted).
    Adopted,
    /// Healthy no-op: the pulled state is not newer than the local view
    /// (the steady state between policy changes).
    NotNewer,
    /// Defensive no-op: this node is a voter — voters only ingest state
    /// via regular Raft replication (the pull loop never runs on voters,
    /// so this is unreachable in practice).
    VoterRefusal,
    /// The responder is not in the local voter set: address
    /// mis-resolution or a zombie removed from the env. Nothing was
    /// adopted and nothing ever will be until the operator fixes the
    /// address mapping.
    RejectedResponder,
}

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
    /// Replicated `ControlPlaneState::version_counter` at the apply that
    /// produced this JSON. Together with `applied_index` it gives the
    /// observer pull guard a floor covering the KEPT persisted namespace
    /// view, which can be ahead of the compaction snapshot the in-memory
    /// state is restored from (see `adopt_pulled_snapshot`). `None` in
    /// markers written before this field existed — those fall back to the
    /// snapshot pair (the previous, conservative behaviour).
    #[serde(default)]
    version_counter: Option<u64>,
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
    /// Observer pull-adoption floor (M-17): the `(version_counter,
    /// applied_index)` pair the persisted namespace view was proven to be
    /// at when startup KEPT it instead of installing the compaction
    /// snapshot. `state`/`last_applied` are restored from the snapshot,
    /// which can trail the kept view by up to `log_max` applies (a voter
    /// persists the namespace on every apply but compacts rarely); without
    /// this floor a demoted ex-voter observer would adopt a pulled
    /// snapshot that is newer than its compaction snapshot yet OLDER than
    /// its persisted namespace — durably rolling the namespace (and the
    /// policy version its authority signatures carry) back.
    adopt_floor: (u64, u64),
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
    /// Observer namespace pull counters (M-17). Only the observer pull
    /// loop writes them; they stay 0 on voters.
    observer_pull_success: AtomicU64,
    observer_pull_failure: AtomicU64,
    observer_last_pull_ms: AtomicU64,
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
        let marker = namespace_persist_path
            .as_deref()
            .and_then(load_apply_marker);
        // A marker only speaks for the CURRENT JSON incarnation.
        let marker_matches_ns = marker.as_ref().is_some_and(|m| {
            m.ns_version
                == namespace
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .version()
                    .0
        });
        // Whether the locally persisted namespace view survives startup
        // (as opposed to being overwritten by the compaction snapshot).
        let mut kept_persisted_view = true;
        if snapshot_meta.last_included_index > 0 {
            let ns_at_or_beyond = marker_matches_ns
                && marker
                    .as_ref()
                    .is_some_and(|m| m.applied_index >= snapshot_meta.last_included_index);
            if ns_at_or_beyond {
                tracing::info!(
                    snapshot_index = snapshot_meta.last_included_index,
                    "keeping the persisted namespace view (at or beyond the \
                     raft snapshot); committed entries replay over it"
                );
            } else {
                kept_persisted_view = false;
                let mut ns = namespace.write().unwrap_or_else(|e| e.into_inner());
                state_machine::install(&snapshot_state, &mut ns);
            }
        }

        // Observer pull-adoption floor: when the (possibly newer) persisted
        // namespace view was kept, the marker's pair is the freshest state
        // this node has durably exposed — a pulled snapshot must beat it,
        // not merely the compaction snapshot, or the pull would durably
        // roll the kept view back (see the `Inner::adopt_floor` docs).
        // Markers without the counter (pre-existing format) fall back to
        // the snapshot pair. A disaster-recovery Bootstrap re-floors the
        // counter and increments it per imported policy, so its (higher
        // counter, low index) states still clear this lexicographic floor.
        let mut adopt_floor = (
            snapshot_state.version_counter,
            snapshot_meta.last_included_index,
        );
        if kept_persisted_view
            && marker_matches_ns
            && let Some(m) = &marker
            && let Some(counter) = m.version_counter
        {
            adopt_floor = adopt_floor.max((counter, m.applied_index));
        }

        let node = Arc::new(Self {
            inner: Mutex::new(Inner {
                core,
                state: snapshot_state.clone(),
                snapshot_state,
                adopt_floor,
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
            observer_pull_success: AtomicU64::new(0),
            observer_pull_failure: AtomicU64::new(0),
            observer_last_pull_ms: AtomicU64::new(0),
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

    pub fn self_id(&self) -> &NodeId {
        &self.self_id
    }

    pub fn voters(&self) -> &BTreeSet<NodeId> {
        &self.voters
    }

    /// Wall-clock ms of the last successful observer namespace pull
    /// (0 = never). Used by the driver's pull-age staleness warning.
    pub fn observer_last_pull_unix_ms(&self) -> u64 {
        self.observer_last_pull_ms.load(Ordering::Relaxed)
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
            observer_ns_pull_success_total: self.observer_pull_success.load(Ordering::Relaxed),
            observer_ns_pull_failure_total: self.observer_pull_failure.load(Ordering::Relaxed),
            observer_ns_last_pull_unix_ms: self.observer_last_pull_ms.load(Ordering::Relaxed),
            observer_ns_version_counter: inner.state.version_counter,
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
        let mut installed_counter = 0;
        if install {
            self.install_state(&mut inner, req.state, last_included);
            installed_counter = inner.state.version_counter;
        }
        let outbound = self.run_effects(&mut inner, effects)?;
        drop(inner);
        if install {
            self.persist_namespace_best_effort(last_included, installed_counter);
        }
        self.dispatch(outbound);
        Ok(resp)
    }

    /// Install a full replicated state at `last_included_index` into the
    /// in-memory state and the namespace projection. Shared by the push
    /// path (`handle_install_snapshot`) and the observer pull path
    /// (`adopt_pulled_snapshot`) so both leave identical in-memory state;
    /// the caller is responsible for snapshot-meta/log bookkeeping,
    /// persistence, and `persist_namespace_best_effort`.
    fn install_state(&self, inner: &mut Inner, state: ControlPlaneState, last_included_index: u64) {
        inner.snapshot_state = state.clone();
        inner.state = state;
        inner.core.last_applied = last_included_index;
        let mut ns = self.namespace.write().unwrap_or_else(|e| e.into_inner());
        state_machine::install(&inner.state, &mut ns);
    }

    // -----------------------------------------------------------
    // Observer namespace pull (M-17)
    // -----------------------------------------------------------

    /// Serve the committed control-plane state (any node; leader not
    /// required). The state machine only applies committed entries, so
    /// `inner.state` at `last_applied` is always a committed prefix.
    pub fn committed_snapshot(&self) -> NamespaceSnapshotResponse {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let last_applied = inner.core.last_applied;
        // `last_applied` is at-or-beyond the snapshot boundary and within
        // the retained log, so `term_at` only misses when the log was
        // damaged; fall back to the snapshot term (conservative).
        let last_applied_term = inner
            .core
            .term_at(last_applied)
            .unwrap_or(inner.core.snapshot_meta.last_included_term);
        NamespaceSnapshotResponse {
            node_id: self.self_id.clone(),
            term: inner.core.hard.current_term,
            last_applied_index: last_applied,
            last_applied_term,
            state: inner.state.clone(),
        }
    }

    /// One observer pull round: fetch `target`'s committed state and adopt
    /// it when it is strictly newer. Returns whether a snapshot was
    /// adopted.
    ///
    /// Metric contract (what the ops guide's alerts are built on): a round
    /// counts as SUCCESS — refreshing `observer_ns_last_pull_unix_ms` —
    /// only when a voter's committed state was actually obtained
    /// (`Adopted` / `NotNewer` / the defensive voter self-refusal).
    /// Everything else counts in `observer_ns_pull_failure_total` and
    /// leaves the freshness timestamp alone: transport errors, responses
    /// from outside the voter set (address misconfiguration — the fetch
    /// "succeeds" every round but the namespace stays frozen), and local
    /// adoption/persistence failures (full or read-only disk).
    pub async fn pull_namespace_once(self: &Arc<Self>, target: &NodeId) -> Result<bool, CrdtError> {
        let req = NamespaceSnapshotRequest {
            requester: self.self_id.clone(),
        };
        let resp = match self
            .transport
            .fetch_namespace_snapshot(target.clone(), req)
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                self.observer_pull_failure.fetch_add(1, Ordering::Relaxed);
                return Err(CrdtError::Internal(format!(
                    "observer namespace pull from {} failed: {e}",
                    target.0
                )));
            }
        };
        let responder = resp.node_id.clone();
        match self.adopt_pulled_snapshot(resp) {
            Ok(
                outcome @ (AdoptOutcome::Adopted
                | AdoptOutcome::NotNewer
                | AdoptOutcome::VoterRefusal),
            ) => {
                self.observer_pull_success.fetch_add(1, Ordering::Relaxed);
                self.observer_last_pull_ms
                    .store(crate::hlc::wall_clock_ms(), Ordering::Relaxed);
                Ok(outcome == AdoptOutcome::Adopted)
            }
            Ok(AdoptOutcome::RejectedResponder) => {
                self.observer_pull_failure.fetch_add(1, Ordering::Relaxed);
                Err(CrdtError::Internal(format!(
                    "observer namespace pull from {} rejected: responder {} is \
                     not in the local voter set — the pulled state was NOT \
                     adopted (check the ASTEROIDB_RAFT_PEERS / \
                     ASTEROIDB_CONTROL_PLANE_NODES address mapping)",
                    target.0, responder.0
                )))
            }
            Err(e) => {
                // Local adoption/persistence failure: the fetch worked but
                // nothing durable came of it — a failed round.
                self.observer_pull_failure.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Adopt a pulled committed snapshot after guarding (M-17).
    ///
    /// Guards, in order:
    /// 1. Voters never adopt (their only ingestion path is regular Raft
    ///    replication).
    /// 2. The responder must be in the local voter set (defence against
    ///    address mis-resolution and zombies removed from the env, same
    ///    posture as the receiver-side config fencing in `core`).
    /// 3. Monotonicity: adopt only when
    ///    `(version_counter, last_applied_index)` is LEXICOGRAPHICALLY
    ///    greater than the local pair. `Bootstrap` floors the counter and
    ///    then increments it once per imported policy, and every policy
    ///    upsert increments it, so within any legal history the pair is
    ///    monotone; authority-only updates advance the index at an equal
    ///    counter. An OR-combination over the two components would let a
    ///    zombie voter (high index, low counter) roll the observer back.
    ///    The local pair is the max of the LIVE pair and the startup
    ///    `adopt_floor`: after a restart the live pair is restored from
    ///    the compaction snapshot, which can trail the kept persisted
    ///    namespace view by up to `log_max` applies (typical for a voter
    ///    demoted to observer) — without the floor, a pull from a lagging
    ///    voter landing in that gap would durably roll the namespace back.
    ///
    /// The responder's term is deliberately ignored and hard state
    /// (`currentTerm` / `votedFor`) is never touched: pulls are unrelated
    /// to elections, which is what makes a later voter promotion (env
    /// change + restart) behave exactly like a follower that had received
    /// an InstallSnapshot.
    pub fn adopt_pulled_snapshot(
        &self,
        resp: NamespaceSnapshotResponse,
    ) -> Result<AdoptOutcome, CrdtError> {
        if self.is_voter() {
            tracing::warn!(
                from = %resp.node_id.0,
                "refusing to adopt a pulled namespace snapshot on a voter \
                 (voters follow the leader's log replication only)"
            );
            return Ok(AdoptOutcome::VoterRefusal);
        }
        if !self.voters.contains(&resp.node_id) {
            tracing::warn!(
                from = %resp.node_id.0,
                voters = ?self.voters.iter().map(|v| v.0.as_str()).collect::<Vec<_>>(),
                "refusing pulled namespace snapshot from a node outside the \
                 local voter set; check ASTEROIDB_CONTROL_PLANE_NODES / \
                 ASTEROIDB_RAFT_PEERS address mapping"
            );
            return Ok(AdoptOutcome::RejectedResponder);
        }

        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let local = (inner.state.version_counter, inner.core.last_applied).max(inner.adopt_floor);
        let remote = (resp.state.version_counter, resp.last_applied_index);
        if remote <= local {
            tracing::debug!(
                from = %resp.node_id.0,
                remote_counter = remote.0,
                remote_index = remote.1,
                local_counter = local.0,
                local_index = local.1,
                "pulled namespace snapshot is not newer; keeping local state"
            );
            return Ok(AdoptOutcome::NotNewer);
        }

        // Flush any previously failed persistence first (same fail-stop
        // discipline as run_effects: never layer new state over an
        // unpersisted mutation).
        self.flush_dirty(&mut inner)?;

        // Snapshot-meta / log bookkeeping, mirroring
        // `handle_install_snapshot_meta` minus the term/leader handling:
        // keep a log suffix only when it is consistent with the boundary.
        let suffix_consistent = matches!(
            inner.core.term_at(resp.last_applied_index),
            Some(t) if t == resp.last_applied_term
        );
        if suffix_consistent {
            inner.core.drop_log_through(resp.last_applied_index);
        } else {
            inner.core.log.clear();
        }
        inner.core.snapshot_meta = SnapshotMeta {
            last_included_index: resp.last_applied_index,
            last_included_term: resp.last_applied_term,
        };
        inner.core.commit_index = inner.core.commit_index.max(resp.last_applied_index);
        self.install_state(&mut inner, resp.state, resp.last_applied_index);

        // Term floor: the durable-state invariant requires
        // `current_term >= max log term` (the storage loader fail-stops
        // on a violation), so adopting a snapshot boundary at a newer
        // term must raise our term to at least that boundary — exactly
        // what a follower does when it receives an InstallSnapshot.
        // `voted_for` is cleared only on a raise (entering a strictly
        // newer term with no vote cast — never a double-vote risk). The
        // RESPONDER's current term (`resp.term`) is still deliberately
        // unused: only the adopted boundary's own term matters.
        if resp.last_applied_term > inner.core.hard.current_term {
            inner.core.hard.current_term = resp.last_applied_term;
            inner.core.hard.voted_for = None;
        }

        // Persist the hard state BEFORE the log. An observer never votes,
        // so `hard_state.json` may not exist yet — and the storage layer
        // deliberately fail-stops on "log without hard state" at load
        // (double-vote protection). Without this write, the FIRST restart
        // after an adoption would refuse to boot. (Design-review finding
        // during implementation; the push install path never hits this
        // because InstallSnapshot's term handling persists hard state.)
        inner.hard_dirty = true;
        self.storage
            .save_hard_state(&inner.core.hard)
            .map_err(|e| CrdtError::Storage(format!("raft hard state: {e}")))?;
        inner.hard_dirty = false;

        // Persist snapshot + log exactly like the install path (the
        // Effect::PersistLog arm), so a restart — possibly followed by a
        // voter promotion — sees the same durable state as a follower
        // that received this content via InstallSnapshot.
        inner.log_dirty = true;
        self.storage
            .save_log(
                &inner.core.snapshot_meta,
                &inner.snapshot_state,
                &inner.core.log,
            )
            .map_err(|e| CrdtError::Storage(format!("raft log: {e}")))?;
        inner.log_dirty = false;

        let last_included = resp.last_applied_index;
        let version_counter = inner.state.version_counter;
        drop(inner);
        self.persist_namespace_best_effort(last_included, version_counter);
        tracing::info!(
            from = %resp.node_id.0,
            last_applied_index = last_included,
            version_counter,
            "adopted pulled control-plane namespace snapshot (observer sync)"
        );
        Ok(AdoptOutcome::Adopted)
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
            self.persist_namespace_best_effort(
                inner.core.last_applied,
                inner.state.version_counter,
            );
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
    /// namespace state and `version_counter` the replicated counter at
    /// that point; both are recorded in the sidecar marker (written only
    /// after the namespace write succeeded) so the next startup can prove
    /// the JSON view is at-or-beyond the compacted snapshot and floor the
    /// observer pull-adoption guard accordingly.
    fn persist_namespace_best_effort(&self, applied_index: u64, version_counter: u64) {
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
            version_counter: Some(version_counter),
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
