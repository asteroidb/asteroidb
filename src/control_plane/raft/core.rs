//! Pure Raft core: deterministic state transitions with no IO.
//!
//! Every transition returns the RPC response (when there is one) plus a list
//! of [`Effect`]s that the driver ([`super::node::RaftNode`]) must execute
//! **in order**. The safety-critical execution rule is:
//!
//! > `PersistHardState` / `PersistLog` effects MUST complete (fsync) before
//! > the response is sent to the caller and before any `Send` effect is
//! > dispatched. If persistence fails, the response and all subsequent
//! > effects are abandoned (no vote granted, no append acked, no proposal
//! > accepted).
//!
//! The three safety rules from the Raft paper are implemented here:
//!
//! 1. **Election restriction**: a voter only grants its vote to candidates
//!    whose log is at least as up-to-date, compared by
//!    `(last_log_term, last_log_index)` ([`RaftCore::handle_request_vote`]).
//! 2. **Commit rule (Figure 8)**: an entry only commits when the CURRENT
//!    term's leader has replicated it on a majority; entries from previous
//!    terms commit only indirectly ([`RaftCore::advance_commit`]).
//! 3. **Term fencing**: every RPC carries the term; a smaller term is
//!    rejected, a larger term forces an immediate step-down. Stale leaders
//!    can never gather majority acks.
//!
//! Additionally, a **prevote-lite guard** (the dissertation §4.2.3
//! "removed server" rule) makes voters ignore RequestVote RPCs — without
//! updating their term — while they have heard from a live leader within the
//! minimum election timeout, so nodes returning from a partition cannot
//! disrupt a healthy leader with inflated terms.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

use super::types::{
    AppendEntriesRequest, AppendEntriesResponse, ControlPlaneCommand, InstallSnapshotResponse,
    LogEntry, RequestVoteRequest, RequestVoteResponse,
};

/// Durable per-term voting state. MUST be fsynced before any RPC response
/// that depends on it — losing it allows double voting in the same term
/// (Election Safety violation → split brain).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HardState {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
}

/// Metadata of the compacted log prefix.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub last_included_index: u64,
    pub last_included_term: u64,
}

/// Current role of this node.
#[derive(Debug, Clone)]
pub enum Role {
    Follower,
    Candidate {
        votes: BTreeSet<NodeId>,
    },
    Leader {
        /// Next log index to send to each peer.
        next_index: BTreeMap<NodeId, u64>,
        /// Highest log index known replicated on each peer.
        match_index: BTreeMap<NodeId, u64>,
    },
}

/// An outbound RPC produced by a transition. `Snapshot` carries no payload:
/// the driver builds the InstallSnapshot request from the current snapshot
/// state under the same lock, so meta and state can never diverge.
#[derive(Debug, Clone)]
pub enum OutboundRpc {
    Vote(RequestVoteRequest),
    Append(AppendEntriesRequest),
    Snapshot,
}

/// Side effects the driver must execute, in order.
#[derive(Debug, Clone)]
pub enum Effect {
    /// fsync `HardState` BEFORE responding / sending anything.
    PersistHardState,
    /// fsync the log (snapshot + tail, single atomic file) BEFORE
    /// responding / sending anything.
    PersistLog,
    /// Send an RPC to a peer (after all persistence effects).
    Send(NodeId, OutboundRpc),
    /// Apply entries `last_applied+1..=commit_index` to the state machine.
    ApplyCommitted,
    /// Reset the randomized election timer.
    ResetElectionTimer,
    /// This node just won an election: the driver appends/broadcasts are
    /// already queued; it must additionally check the one-shot `Bootstrap`
    /// proposal.
    BecameLeader,
    /// Lost leadership/candidacy: fail all pending proposal waiters with
    /// `NotLeader`.
    SteppedDown,
}

/// Pure Raft state machine (control-plane consensus core).
#[derive(Debug)]
pub struct RaftCore {
    pub id: NodeId,
    /// Static voter set. If `id` is not a member the core is inert: it never
    /// starts elections and never accepts proposals (observer mode).
    pub voters: BTreeSet<NodeId>,
    pub hard: HardState,
    /// Log tail; the first entry has index `snapshot_meta.last_included_index + 1`.
    pub log: Vec<LogEntry>,
    pub snapshot_meta: SnapshotMeta,
    pub commit_index: u64,
    pub last_applied: u64,
    pub role: Role,
    /// Best-known current leader (for NotLeader hints).
    pub leader_hint: Option<NodeId>,
    /// Last time a valid AppendEntries/InstallSnapshot arrived from a live
    /// leader (prevote-lite guard input). Injected clock: all transitions
    /// take `now` as an argument.
    pub last_leader_contact: Option<Instant>,
    /// Minimum election timeout, used by the prevote-lite guard.
    pub election_timeout_min: Duration,
}

impl RaftCore {
    pub fn new(
        id: NodeId,
        voters: BTreeSet<NodeId>,
        election_timeout_min: Duration,
        hard: HardState,
        snapshot_meta: SnapshotMeta,
        log: Vec<LogEntry>,
    ) -> Self {
        let commit_index = snapshot_meta.last_included_index;
        Self {
            id,
            voters,
            hard,
            log,
            snapshot_meta,
            commit_index,
            last_applied: commit_index,
            role: Role::Follower,
            leader_hint: None,
            last_leader_contact: None,
            election_timeout_min,
        }
    }

    // -----------------------------------------------------------
    // Log accessors
    // -----------------------------------------------------------

    pub fn last_log_index(&self) -> u64 {
        self.snapshot_meta.last_included_index + self.log.len() as u64
    }

    pub fn last_log_term(&self) -> u64 {
        self.log
            .last()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_meta.last_included_term)
    }

    /// Term of the entry at `index`, if known. The snapshot boundary counts;
    /// indexes compacted away return `None`.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        let snap = self.snapshot_meta.last_included_index;
        if index == snap {
            return Some(self.snapshot_meta.last_included_term);
        }
        if index < snap {
            return None;
        }
        let pos = (index - snap - 1) as usize;
        self.log.get(pos).map(|e| e.term)
    }

    /// The entry at `index`, if present in the tail.
    pub fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        let snap = self.snapshot_meta.last_included_index;
        if index <= snap {
            return None;
        }
        self.log.get((index - snap - 1) as usize)
    }

    pub fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    fn peers(&self) -> impl Iterator<Item = &NodeId> {
        self.voters.iter().filter(move |v| **v != self.id)
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader { .. })
    }

    pub fn role_name(&self) -> &'static str {
        match self.role {
            Role::Follower => "follower",
            Role::Candidate { .. } => "candidate",
            Role::Leader { .. } => "leader",
        }
    }

    /// Drop all log entries with `index <= through` (compaction). The caller
    /// must only compact applied entries and must update `snapshot_meta`
    /// itself (with the state snapshot) in the same critical section.
    pub fn drop_log_through(&mut self, through: u64) {
        let snap = self.snapshot_meta.last_included_index;
        if through <= snap {
            return;
        }
        let drop_n = ((through - snap) as usize).min(self.log.len());
        self.log.drain(..drop_n);
    }

    // -----------------------------------------------------------
    // Term / role transitions
    // -----------------------------------------------------------

    /// Observe `term`: bump our term (and clear the vote) when it is newer,
    /// and fall back to follower when we were leading or campaigning.
    fn step_down_to_term(&mut self, term: u64, effects: &mut Vec<Effect>) {
        if term > self.hard.current_term {
            self.hard.current_term = term;
            self.hard.voted_for = None;
            effects.push(Effect::PersistHardState);
        }
        if !matches!(self.role, Role::Follower) {
            self.role = Role::Follower;
            effects.push(Effect::SteppedDown);
        }
    }

    // -----------------------------------------------------------
    // RequestVote (receiver)
    // -----------------------------------------------------------

    pub fn handle_request_vote(
        &mut self,
        req: &RequestVoteRequest,
        now: Instant,
    ) -> (RequestVoteResponse, Vec<Effect>) {
        let mut effects = Vec::new();

        // Configuration fencing: a candidate outside our static voter set is
        // a misconfigured node (e.g. mismatched ASTEROIDB_CONTROL_PLANE_NODES).
        // Ignore it WITHOUT adopting its term — its votes/terms must never
        // influence this cluster.
        if !self.voters.contains(&req.candidate_id) {
            tracing::warn!(
                candidate = %req.candidate_id.0,
                voters = ?self.voters.iter().map(|v| v.0.as_str()).collect::<Vec<_>>(),
                "rejecting RequestVote from a node outside the local voter set; \
                 check ASTEROIDB_CONTROL_PLANE_NODES is identical on every node"
            );
            return (
                RequestVoteResponse {
                    term: self.hard.current_term,
                    vote_granted: false,
                },
                effects,
            );
        }

        // Term fencing: stale candidates are rejected outright.
        if req.term < self.hard.current_term {
            return (
                RequestVoteResponse {
                    term: self.hard.current_term,
                    vote_granted: false,
                },
                effects,
            );
        }

        // Prevote-lite / leader-contact guard: while we have heard from a
        // live leader within the minimum election timeout, ignore the
        // request WITHOUT adopting its term. A node returning from a
        // partition with an inflated term therefore cannot depose a healthy
        // leader through us.
        if !self.is_leader()
            && let Some(contact) = self.last_leader_contact
            && now.duration_since(contact) < self.election_timeout_min
        {
            return (
                RequestVoteResponse {
                    term: self.hard.current_term,
                    vote_granted: false,
                },
                effects,
            );
        }

        if req.term > self.hard.current_term {
            self.step_down_to_term(req.term, &mut effects);
        }

        // Election restriction: grant only if the candidate's log is at
        // least as up-to-date as ours, by (last_log_term, last_log_index).
        let up_to_date = (req.last_log_term, req.last_log_index)
            >= (self.last_log_term(), self.last_log_index());
        let can_vote = match &self.hard.voted_for {
            None => true,
            Some(v) => *v == req.candidate_id,
        };
        let granted = up_to_date && can_vote;

        if granted {
            self.hard.voted_for = Some(req.candidate_id.clone());
            // ALWAYS persist on grant (even for an idempotent re-grant):
            // a previous attempt may have updated the in-memory vote and
            // then failed to persist it — answering from memory alone
            // would grant an unrecorded vote (double-vote enabler).
            effects.push(Effect::PersistHardState);
            effects.push(Effect::ResetElectionTimer);
        }

        (
            RequestVoteResponse {
                term: self.hard.current_term,
                vote_granted: granted,
            },
            effects,
        )
    }

    // -----------------------------------------------------------
    // AppendEntries (receiver)
    // -----------------------------------------------------------

    pub fn handle_append_entries(
        &mut self,
        req: &AppendEntriesRequest,
        now: Instant,
    ) -> (AppendEntriesResponse, Vec<Effect>) {
        let mut effects = Vec::new();

        // Configuration fencing: a "leader" outside our static voter set can
        // only be a misconfigured node (a node that defaulted its voter set
        // to itself self-elects with majority=1 and replicates a divergent
        // log). Accepting its entries could truncate/replace state committed
        // by the real cluster — reject without adopting its term.
        if !self.voters.contains(&req.leader_id) {
            tracing::warn!(
                leader = %req.leader_id.0,
                voters = ?self.voters.iter().map(|v| v.0.as_str()).collect::<Vec<_>>(),
                "rejecting AppendEntries from a node outside the local voter set; \
                 check ASTEROIDB_CONTROL_PLANE_NODES is identical on every node"
            );
            return (
                AppendEntriesResponse {
                    term: self.hard.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                },
                effects,
            );
        }

        // Term fencing: a stale leader's AppendEntries is rejected; it will
        // observe our newer term in the response and step down.
        if req.term < self.hard.current_term {
            return (
                AppendEntriesResponse {
                    term: self.hard.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                },
                effects,
            );
        }

        self.step_down_to_term(req.term, &mut effects);
        self.leader_hint = Some(req.leader_id.clone());
        self.last_leader_contact = Some(now);
        effects.push(Effect::ResetElectionTimer);

        let snap = self.snapshot_meta.last_included_index;

        // Entries at or below the snapshot boundary are committed by
        // definition; clamp the consistency check to the boundary.
        let eff_prev = req.prev_log_index.max(snap);
        let prev_ok = if req.prev_log_index < snap {
            true
        } else {
            matches!(self.term_at(eff_prev), Some(t) if t == req.prev_log_term)
        };
        if !prev_ok {
            return (
                AppendEntriesResponse {
                    term: self.hard.current_term,
                    success: false,
                    // Back-off hint: our last log index bounds where the
                    // leader needs to look.
                    match_index: self.last_log_index(),
                },
                effects,
            );
        }

        // Append / reconcile entries. Duplicates (same index & term) are
        // skipped for idempotency; a conflict (same index, different term)
        // truncates our suffix from that point (Log Matching).
        let mut changed = false;
        for entry in req.entries.iter().filter(|e| e.index > snap) {
            let idx = entry.index;
            if idx <= self.last_log_index() {
                match self.term_at(idx) {
                    Some(t) if t == entry.term => continue,
                    _ => {
                        // A conflict at or below our commit index is
                        // impossible in a correctly configured cluster
                        // (Leader Completeness: an elected leader's log
                        // contains every committed entry). Reaching this
                        // branch means diverged voter sets / damaged state;
                        // truncating would silently lose committed, applied
                        // entries — refuse instead (fail-stop posture).
                        if idx <= self.commit_index {
                            tracing::error!(
                                index = idx,
                                commit_index = self.commit_index,
                                leader = %req.leader_id.0,
                                "refusing AppendEntries that conflicts with a \
                                 committed entry; the cluster configuration has \
                                 diverged (see docs/ops-guide.md)"
                            );
                            if changed {
                                // Unreachable in practice (entries ascend, so
                                // nothing can change before a committed-range
                                // conflict) — kept so a mutated log is never
                                // left unpersisted.
                                effects.push(Effect::PersistLog);
                            }
                            return (
                                AppendEntriesResponse {
                                    term: self.hard.current_term,
                                    success: false,
                                    match_index: self.last_log_index(),
                                },
                                effects,
                            );
                        }
                        let pos = (idx - snap - 1) as usize;
                        self.log.truncate(pos);
                        self.log.push(entry.clone());
                        changed = true;
                    }
                }
            } else {
                debug_assert_eq!(idx, self.last_log_index() + 1, "log gap in AppendEntries");
                self.log.push(entry.clone());
                changed = true;
            }
        }
        if changed {
            effects.push(Effect::PersistLog);
        }

        // Commit advance (bounded by our own log length).
        let new_commit = req.leader_commit.min(self.last_log_index());
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            effects.push(Effect::ApplyCommitted);
        }

        // All entries in the request are now known present.
        let match_index = req.prev_log_index + req.entries.len() as u64;
        (
            AppendEntriesResponse {
                term: self.hard.current_term,
                success: true,
                match_index,
            },
            effects,
        )
    }

    // -----------------------------------------------------------
    // InstallSnapshot (receiver) — log/meta part only; the driver swaps the
    // state machine content when `install == true`.
    // -----------------------------------------------------------

    pub fn handle_install_snapshot_meta(
        &mut self,
        term: u64,
        leader_id: &NodeId,
        last_included_index: u64,
        last_included_term: u64,
        now: Instant,
    ) -> (InstallSnapshotResponse, bool, Vec<Effect>) {
        let mut effects = Vec::new();

        // Configuration fencing: see `handle_append_entries`.
        if !self.voters.contains(leader_id) {
            tracing::warn!(
                leader = %leader_id.0,
                voters = ?self.voters.iter().map(|v| v.0.as_str()).collect::<Vec<_>>(),
                "rejecting InstallSnapshot from a node outside the local voter set; \
                 check ASTEROIDB_CONTROL_PLANE_NODES is identical on every node"
            );
            return (
                InstallSnapshotResponse {
                    term: self.hard.current_term,
                },
                false,
                effects,
            );
        }

        if term < self.hard.current_term {
            return (
                InstallSnapshotResponse {
                    term: self.hard.current_term,
                },
                false,
                effects,
            );
        }

        self.step_down_to_term(term, &mut effects);
        self.leader_hint = Some(leader_id.clone());
        self.last_leader_contact = Some(now);
        effects.push(Effect::ResetElectionTimer);

        // Stale snapshot: everything it contains is already committed here.
        if last_included_index <= self.commit_index {
            return (
                InstallSnapshotResponse {
                    term: self.hard.current_term,
                },
                false,
                effects,
            );
        }

        // Keep a log suffix beyond the snapshot only when it is consistent
        // with the snapshot boundary; otherwise discard the whole log.
        let suffix_consistent =
            matches!(self.term_at(last_included_index), Some(t) if t == last_included_term);
        if suffix_consistent {
            self.drop_log_through(last_included_index);
        } else {
            self.log.clear();
        }
        self.snapshot_meta = SnapshotMeta {
            last_included_index,
            last_included_term,
        };
        self.commit_index = last_included_index;
        // last_applied is set by the driver when it swaps in the state.
        effects.push(Effect::PersistLog);

        (
            InstallSnapshotResponse {
                term: self.hard.current_term,
            },
            true,
            effects,
        )
    }

    // -----------------------------------------------------------
    // Elections
    // -----------------------------------------------------------

    /// Election timer fired: start a new election (unless we are the leader,
    /// not a voter, or have fresh leader contact from a raced reset).
    pub fn on_election_timeout(&mut self, now: Instant) -> Vec<Effect> {
        if !self.voters.contains(&self.id) || self.is_leader() {
            return Vec::new();
        }
        if let Some(contact) = self.last_leader_contact
            && now.duration_since(contact) < self.election_timeout_min
        {
            return vec![Effect::ResetElectionTimer];
        }

        let mut effects = Vec::new();
        self.hard.current_term += 1;
        self.hard.voted_for = Some(self.id.clone());
        effects.push(Effect::PersistHardState);

        let mut votes = BTreeSet::new();
        votes.insert(self.id.clone());
        self.role = Role::Candidate { votes };
        effects.push(Effect::ResetElectionTimer);

        // Single-voter cluster: win immediately.
        if 1 >= self.majority() {
            effects.extend(self.become_leader());
            return effects;
        }

        let req = RequestVoteRequest {
            term: self.hard.current_term,
            candidate_id: self.id.clone(),
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };
        let sends: Vec<Effect> = self
            .peers()
            .map(|peer| Effect::Send(peer.clone(), OutboundRpc::Vote(req.clone())))
            .collect();
        effects.extend(sends);
        effects
    }

    pub fn handle_vote_response(
        &mut self,
        term_sent: u64,
        from: NodeId,
        resp: &RequestVoteResponse,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        if resp.term > self.hard.current_term {
            self.step_down_to_term(resp.term, &mut effects);
            return effects;
        }
        if term_sent != self.hard.current_term {
            return effects; // stale response from an earlier campaign
        }
        let majority = self.majority();
        let won = match &mut self.role {
            Role::Candidate { votes } if resp.vote_granted && self.voters.contains(&from) => {
                votes.insert(from);
                votes.len() >= majority
            }
            _ => false,
        };
        if won {
            effects.extend(self.become_leader());
        }
        effects
    }

    fn become_leader(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        let next = self.last_log_index() + 1;
        let next_index: BTreeMap<NodeId, u64> = self.peers().map(|p| (p.clone(), next)).collect();
        let match_index: BTreeMap<NodeId, u64> = self.peers().map(|p| (p.clone(), 0)).collect();
        self.role = Role::Leader {
            next_index,
            match_index,
        };
        self.leader_hint = Some(self.id.clone());

        // Append a current-term no-op immediately (Figure 8): committing it
        // is what commits any surviving entries from previous terms.
        let idx = self.last_log_index() + 1;
        self.log.push(LogEntry {
            index: idx,
            term: self.hard.current_term,
            command: ControlPlaneCommand::Noop,
        });
        effects.push(Effect::PersistLog);
        effects.push(Effect::BecameLeader);
        effects.extend(self.advance_commit());
        effects.extend(self.broadcast_append());
        effects
    }

    // -----------------------------------------------------------
    // Replication (leader)
    // -----------------------------------------------------------

    /// Build the replication RPC for a peer whose next index is `next`.
    fn build_rpc_for(&self, next: u64) -> OutboundRpc {
        let snap = self.snapshot_meta.last_included_index;
        if next <= snap {
            return OutboundRpc::Snapshot;
        }
        let prev = next - 1;
        let prev_term = self
            .term_at(prev)
            .expect("prev index is at or above the snapshot boundary");
        let entries: Vec<LogEntry> = self.log[(next - snap - 1) as usize..].to_vec();
        OutboundRpc::Append(AppendEntriesRequest {
            term: self.hard.current_term,
            leader_id: self.id.clone(),
            prev_log_index: prev,
            prev_log_term: prev_term,
            entries,
            leader_commit: self.commit_index,
        })
    }

    /// Send an AppendEntries (or InstallSnapshot) to every peer, according
    /// to each peer's `next_index`. No-op when not leader.
    pub fn broadcast_append(&self) -> Vec<Effect> {
        let Role::Leader { next_index, .. } = &self.role else {
            return Vec::new();
        };
        next_index
            .iter()
            .map(|(peer, &next)| Effect::Send(peer.clone(), self.build_rpc_for(next)))
            .collect()
    }

    pub fn handle_append_response(
        &mut self,
        term_sent: u64,
        from: NodeId,
        resp: &AppendEntriesResponse,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        if resp.term > self.hard.current_term {
            self.step_down_to_term(resp.term, &mut effects);
            return effects;
        }
        if term_sent != self.hard.current_term || !self.is_leader() {
            return effects;
        }

        let last = self.last_log_index();
        let retry_next = {
            let Role::Leader {
                next_index,
                match_index,
            } = &mut self.role
            else {
                unreachable!("checked is_leader above");
            };
            if resp.success {
                let m = match_index.entry(from.clone()).or_insert(0);
                *m = (*m).max(resp.match_index);
                let new_next = (*m + 1).max(1);
                next_index.insert(from.clone(), new_next);
                // Follow up immediately when the peer is still behind.
                if new_next <= last {
                    Some(new_next)
                } else {
                    None
                }
            } else {
                // Log-mismatch back-off with the follower's hint.
                let cur = *next_index.get(&from).unwrap_or(&1);
                let new_next = cur.saturating_sub(1).min(resp.match_index + 1).max(1);
                next_index.insert(from.clone(), new_next);
                Some(new_next)
            }
        };

        if resp.success {
            effects.extend(self.advance_commit());
        }
        if let Some(next) = retry_next {
            effects.push(Effect::Send(from, self.build_rpc_for(next)));
        }
        effects
    }

    /// Record that `to` has installed our snapshot up to `last_included`.
    pub fn handle_snapshot_ack(
        &mut self,
        term_sent: u64,
        to: NodeId,
        resp_term: u64,
        last_included: u64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        if resp_term > self.hard.current_term {
            self.step_down_to_term(resp_term, &mut effects);
            return effects;
        }
        if term_sent != self.hard.current_term {
            return effects;
        }
        let last = self.last_log_index();
        let retry_next = {
            let Role::Leader {
                next_index,
                match_index,
            } = &mut self.role
            else {
                return effects;
            };
            let m = match_index.entry(to.clone()).or_insert(0);
            *m = (*m).max(last_included);
            let new_next = *m + 1;
            next_index.insert(to.clone(), new_next);
            if new_next <= last {
                Some(new_next)
            } else {
                None
            }
        };
        effects.extend(self.advance_commit());
        if let Some(next) = retry_next {
            effects.push(Effect::Send(to, self.build_rpc_for(next)));
        }
        effects
    }

    /// Advance `commit_index` (leader only), respecting the Figure 8 rule:
    /// only an entry of the CURRENT term may be committed by counting
    /// replicas; older entries commit transitively.
    fn advance_commit(&mut self) -> Vec<Effect> {
        let Role::Leader { match_index, .. } = &self.role else {
            return Vec::new();
        };
        let majority = self.majority();
        let last = self.last_log_index();
        let mut new_commit = self.commit_index;
        for n in ((self.commit_index + 1)..=last).rev() {
            let mut count = usize::from(self.voters.contains(&self.id));
            count += match_index
                .iter()
                .filter(|(peer, m)| self.voters.contains(*peer) && **m >= n)
                .count();
            if count >= majority {
                // Log terms are non-decreasing: if the highest
                // majority-replicated index is from an older term, every
                // lower index is too — nothing commits yet (Figure 8).
                if self.term_at(n) == Some(self.hard.current_term) {
                    new_commit = n;
                }
                break;
            }
        }
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            vec![Effect::ApplyCommitted]
        } else {
            Vec::new()
        }
    }

    // -----------------------------------------------------------
    // Proposals (leader)
    // -----------------------------------------------------------

    /// Append a new command to the leader's log. Returns the entry's
    /// `(index, term)` and the effects (persist + replicate + possibly an
    /// immediate single-voter commit), or `None` when this node is not the
    /// leader. NO state-machine work happens here: version numbering etc.
    /// is assigned at apply time, in commit order.
    pub fn propose(&mut self, command: ControlPlaneCommand) -> Option<((u64, u64), Vec<Effect>)> {
        if !self.is_leader() {
            return None;
        }
        let idx = self.last_log_index() + 1;
        let term = self.hard.current_term;
        self.log.push(LogEntry {
            index: idx,
            term,
            command,
        });
        let mut effects = vec![Effect::PersistLog];
        effects.extend(self.advance_commit());
        effects.extend(self.broadcast_append());
        Some(((idx, term), effects))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::raft::types::PolicySpec;

    fn nid(s: &str) -> NodeId {
        NodeId(s.into())
    }

    fn voters(ids: &[&str]) -> BTreeSet<NodeId> {
        ids.iter().map(|s| nid(s)).collect()
    }

    fn fresh_core(id: &str, voter_ids: &[&str]) -> RaftCore {
        RaftCore::new(
            nid(id),
            voters(voter_ids),
            Duration::from_millis(150),
            HardState::default(),
            SnapshotMeta::default(),
            Vec::new(),
        )
    }

    fn now() -> Instant {
        Instant::now()
    }

    fn sends_of(effects: &[Effect]) -> Vec<(&NodeId, &OutboundRpc)> {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::Send(to, rpc) => Some((to, rpc)),
                _ => None,
            })
            .collect()
    }

    fn has_effect_persist_hard(effects: &[Effect]) -> bool {
        effects
            .iter()
            .any(|e| matches!(e, Effect::PersistHardState))
    }

    fn grant_all_votes(core: &mut RaftCore, from: &[&str]) {
        let term = core.hard.current_term;
        for f in from {
            core.handle_vote_response(
                term,
                nid(f),
                &RequestVoteResponse {
                    term,
                    vote_granted: true,
                },
            );
        }
    }

    fn spec(prefix: &str) -> PolicySpec {
        PolicySpec {
            prefix: prefix.into(),
            replica_count: 3,
            required_tags: BTreeSet::new(),
            forbidden_tags: BTreeSet::new(),
            allow_local_write_on_partition: false,
            certified: false,
            max_read_latency_ms: None,
            preferred_cost_tier: None,
        }
    }

    // --- Elections ---

    #[test]
    fn election_timeout_starts_campaign_and_requests_votes() {
        let mut core = fresh_core("n1", &["n1", "n2", "n3"]);
        let effects = core.on_election_timeout(now());
        assert_eq!(core.hard.current_term, 1);
        assert_eq!(core.hard.voted_for, Some(nid("n1")));
        assert!(matches!(core.role, Role::Candidate { .. }));
        assert!(has_effect_persist_hard(&effects));
        let sends = sends_of(&effects);
        assert_eq!(sends.len(), 2, "vote requests go to both peers");
    }

    #[test]
    fn majority_votes_win_election_and_append_noop() {
        let mut core = fresh_core("n1", &["n1", "n2", "n3"]);
        core.on_election_timeout(now());
        let effects = core.handle_vote_response(
            1,
            nid("n2"),
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert!(core.is_leader());
        assert!(
            effects.iter().any(|e| matches!(e, Effect::BecameLeader)),
            "BecameLeader effect expected"
        );
        // A current-term Noop must be appended immediately.
        assert_eq!(core.last_log_index(), 1);
        assert!(matches!(
            core.entry_at(1).unwrap().command,
            ControlPlaneCommand::Noop
        ));
    }

    #[test]
    fn single_voter_wins_immediately() {
        let mut core = fresh_core("solo", &["solo"]);
        let effects = core.on_election_timeout(now());
        assert!(core.is_leader());
        // Noop committed & applied immediately (majority of 1).
        assert_eq!(core.commit_index, 1);
        assert!(
            effects.iter().any(|e| matches!(e, Effect::ApplyCommitted)),
            "single-voter commit applies immediately"
        );
    }

    #[test]
    fn non_voter_never_campaigns() {
        let mut core = fresh_core("observer", &["n1", "n2", "n3"]);
        let effects = core.on_election_timeout(now());
        assert!(effects.is_empty());
        assert_eq!(core.hard.current_term, 0);
        assert!(matches!(core.role, Role::Follower));
    }

    #[test]
    fn split_vote_retries_with_higher_term() {
        let mut core = fresh_core("n1", &["n1", "n2", "n3"]);
        core.on_election_timeout(now());
        assert_eq!(core.hard.current_term, 1);
        // No votes arrive; the timer fires again.
        core.on_election_timeout(now());
        assert_eq!(core.hard.current_term, 2);
        assert!(matches!(core.role, Role::Candidate { .. }));
    }

    #[test]
    fn higher_term_response_demotes_candidate() {
        let mut core = fresh_core("n1", &["n1", "n2", "n3"]);
        core.on_election_timeout(now());
        let effects = core.handle_vote_response(
            1,
            nid("n2"),
            &RequestVoteResponse {
                term: 5,
                vote_granted: false,
            },
        );
        assert_eq!(core.hard.current_term, 5);
        assert!(matches!(core.role, Role::Follower));
        assert!(has_effect_persist_hard(&effects));
    }

    #[test]
    fn stale_vote_response_ignored() {
        let mut core = fresh_core("n1", &["n1", "n2", "n3"]);
        core.on_election_timeout(now()); // term 1
        core.on_election_timeout(now()); // term 2 (new campaign)
        // A straggler grant from term 1 must not count for term 2.
        core.handle_vote_response(
            1,
            nid("n2"),
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert!(!core.is_leader());
    }

    #[test]
    fn non_voter_grant_does_not_count() {
        let mut core = fresh_core("n1", &["n1", "n2", "n3", "n4", "n5"]);
        core.on_election_timeout(now());
        core.handle_vote_response(
            1,
            nid("stranger"),
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        core.handle_vote_response(
            1,
            nid("n2"),
            &RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        );
        assert!(
            !core.is_leader(),
            "2 real votes of 5 voters is not a majority even with a stranger's grant"
        );
    }

    // --- Voting rules ---

    #[test]
    fn vote_granted_once_per_term() {
        let mut core = fresh_core("n3", &["n1", "n2", "n3"]);
        let req1 = RequestVoteRequest {
            term: 1,
            candidate_id: nid("n1"),
            last_log_index: 0,
            last_log_term: 0,
        };
        let (resp, effects) = core.handle_request_vote(&req1, now());
        assert!(resp.vote_granted);
        assert!(
            has_effect_persist_hard(&effects),
            "granting a vote must persist votedFor first"
        );

        // Different candidate, same term: refused.
        let req2 = RequestVoteRequest {
            term: 1,
            candidate_id: nid("n2"),
            last_log_index: 0,
            last_log_term: 0,
        };
        let (resp, _) = core.handle_request_vote(&req2, now());
        assert!(!resp.vote_granted, "second vote in the same term refused");

        // Same candidate again (retransmission): re-granted idempotently.
        let (resp, _) = core.handle_request_vote(&req1, now());
        assert!(resp.vote_granted);
    }

    #[test]
    fn restart_with_persisted_vote_refuses_second_vote() {
        // Simulates a crash-restart: HardState is reloaded, so the node
        // still refuses to vote for a different candidate in the same term.
        let mut core = fresh_core("n3", &["n1", "n2", "n3"]);
        let req = RequestVoteRequest {
            term: 4,
            candidate_id: nid("n1"),
            last_log_index: 0,
            last_log_term: 0,
        };
        core.handle_request_vote(&req, now());
        let persisted = core.hard.clone();

        // "Restart": rebuild from the persisted hard state.
        let mut restarted = RaftCore::new(
            nid("n3"),
            voters(&["n1", "n2", "n3"]),
            Duration::from_millis(150),
            persisted,
            SnapshotMeta::default(),
            Vec::new(),
        );
        let req2 = RequestVoteRequest {
            term: 4,
            candidate_id: nid("n2"),
            last_log_index: 0,
            last_log_term: 0,
        };
        let (resp, _) = restarted.handle_request_vote(&req2, now());
        assert!(
            !resp.vote_granted,
            "restart must not enable double voting in the same term"
        );
    }

    #[test]
    fn election_restriction_rejects_stale_log() {
        let mut core = fresh_core("n3", &["n1", "n2", "n3"]);
        core.log.push(LogEntry {
            index: 1,
            term: 2,
            command: ControlPlaneCommand::Noop,
        });
        core.hard.current_term = 2;

        // Candidate with an older last term: rejected.
        let (resp, _) = core.handle_request_vote(
            &RequestVoteRequest {
                term: 3,
                candidate_id: nid("n1"),
                last_log_index: 5,
                last_log_term: 1,
            },
            now(),
        );
        assert!(!resp.vote_granted, "older lastLogTerm must be rejected");

        // Candidate with the same last term but a shorter log: rejected.
        core.log.push(LogEntry {
            index: 2,
            term: 2,
            command: ControlPlaneCommand::Noop,
        });
        let (resp, _) = core.handle_request_vote(
            &RequestVoteRequest {
                term: 4,
                candidate_id: nid("n2"),
                last_log_index: 1,
                last_log_term: 2,
            },
            now(),
        );
        assert!(
            !resp.vote_granted,
            "shorter log at same term must be rejected"
        );

        // Candidate at least as up-to-date: granted.
        let (resp, _) = core.handle_request_vote(
            &RequestVoteRequest {
                term: 5,
                candidate_id: nid("n1"),
                last_log_index: 2,
                last_log_term: 2,
            },
            now(),
        );
        assert!(resp.vote_granted);
    }

    #[test]
    fn prevote_lite_guard_ignores_disruptive_candidate() {
        let mut core = fresh_core("n2", &["n1", "n2", "n3"]);
        // Receive a heartbeat from the current leader at term 3.
        let hb = AppendEntriesRequest {
            term: 3,
            leader_id: nid("n1"),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        core.handle_append_entries(&hb, now());
        assert_eq!(core.hard.current_term, 3);

        // A partitioned node returns with an inflated term and campaigns.
        let (resp, effects) = core.handle_request_vote(
            &RequestVoteRequest {
                term: 99,
                candidate_id: nid("n3"),
                last_log_index: 0,
                last_log_term: 0,
            },
            now(),
        );
        assert!(!resp.vote_granted);
        assert_eq!(
            core.hard.current_term, 3,
            "the guard must not adopt the disruptive term"
        );
        assert!(effects.is_empty());
    }

    // --- Configuration fencing (diverged voter sets) ---

    /// RPCs from nodes outside the local static voter set are rejected
    /// without adopting their term: a node whose voter set silently
    /// defaulted to itself (missing ASTEROIDB_CONTROL_PLANE_NODES)
    /// self-elects with majority=1 — its RPCs must never influence, and
    /// never truncate, the correctly configured cluster.
    #[test]
    fn rpcs_from_outside_voter_set_are_rejected_without_term_adoption() {
        let mut core = fresh_core("n2", &["n1", "n2", "n3"]);
        core.hard.current_term = 3;

        // RequestVote from a non-voter candidate: refused, term untouched.
        let (resp, effects) = core.handle_request_vote(
            &RequestVoteRequest {
                term: 99,
                candidate_id: nid("rogue"),
                last_log_index: 10,
                last_log_term: 99,
            },
            now(),
        );
        assert!(!resp.vote_granted);
        assert_eq!(core.hard.current_term, 3, "rogue term must not be adopted");
        assert!(effects.is_empty());

        // AppendEntries from a non-voter "leader": refused, nothing appended.
        let (resp, effects) = core.handle_append_entries(
            &AppendEntriesRequest {
                term: 99,
                leader_id: nid("rogue"),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    index: 1,
                    term: 99,
                    command: ControlPlaneCommand::Noop,
                }],
                leader_commit: 1,
            },
            now(),
        );
        assert!(!resp.success);
        assert_eq!(core.hard.current_term, 3);
        assert_eq!(core.last_log_index(), 0, "rogue entries must not append");
        assert_eq!(core.commit_index, 0);
        assert!(effects.is_empty());
        assert!(core.leader_hint.is_none(), "rogue must not become the hint");

        // InstallSnapshot from a non-voter "leader": refused, not installed.
        let (resp, install, effects) =
            core.handle_install_snapshot_meta(99, &nid("rogue"), 5, 2, now());
        assert_eq!(resp.term, 3);
        assert!(!install);
        assert!(effects.is_empty());
        assert_eq!(core.snapshot_meta, SnapshotMeta::default());
    }

    /// A conflicting AppendEntries at or below the commit index (only
    /// possible with diverged configurations / damaged state) must be
    /// refused, never truncate committed entries — in release builds too.
    #[test]
    fn conflicting_append_below_commit_is_refused_not_truncated() {
        let mut core = fresh_core("n2", &["n1", "n2", "n3"]);
        let mut req = leader_with_log(&[(1, 1), (2, 1)]);
        req.leader_commit = 2;
        let (resp, _) = core.handle_append_entries(&req, now());
        assert!(resp.success);
        assert_eq!(core.commit_index, 2);

        // A conflicting entry (different term) at committed index 1.
        let conflict = AppendEntriesRequest {
            term: 2,
            leader_id: nid("n1"),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                index: 1,
                term: 2,
                command: ControlPlaneCommand::Noop,
            }],
            leader_commit: 2,
        };
        let (resp, _) = core.handle_append_entries(&conflict, now());
        assert!(!resp.success, "committed-range conflict must be refused");
        assert_eq!(core.term_at(1), Some(1), "committed entry must survive");
        assert_eq!(core.last_log_index(), 2, "no truncation");
        assert_eq!(core.commit_index, 2, "commit index untouched");
    }

    // --- AppendEntries / log matching ---

    fn leader_with_log(entries: &[(u64, u64)]) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term: 1,
            leader_id: nid("n1"),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: entries
                .iter()
                .map(|(i, t)| LogEntry {
                    index: *i,
                    term: *t,
                    command: ControlPlaneCommand::Noop,
                })
                .collect(),
            leader_commit: 0,
        }
    }

    #[test]
    fn append_entries_rejects_stale_term() {
        let mut core = fresh_core("n2", &["n1", "n2", "n3"]);
        core.hard.current_term = 5;
        let req = leader_with_log(&[(1, 1)]);
        let (resp, _) = core.handle_append_entries(&req, now());
        assert!(!resp.success, "stale leader must be fenced");
        assert_eq!(resp.term, 5);
    }

    #[test]
    fn append_entries_rejects_prev_mismatch_with_hint() {
        let mut core = fresh_core("n2", &["n1", "n2", "n3"]);
        // Local log: one entry at term 1.
        core.log.push(LogEntry {
            index: 1,
            term: 1,
            command: ControlPlaneCommand::Noop,
        });
        core.hard.current_term = 2;
        let req = AppendEntriesRequest {
            term: 2,
            leader_id: nid("n1"),
            prev_log_index: 5,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 0,
        };
        let (resp, _) = core.handle_append_entries(&req, now());
        assert!(!resp.success);
        assert_eq!(resp.match_index, 1, "hint = follower's last log index");
    }

    #[test]
    fn append_entries_truncates_conflicting_suffix() {
        let mut core = fresh_core("n2", &["n1", "n2", "n3"]);
        for i in 1..=3 {
            core.log.push(LogEntry {
                index: i,
                term: 1,
                command: ControlPlaneCommand::Noop,
            });
        }
        // New leader at term 2 overwrites indexes 2..3 with term-2 entries.
        let req = AppendEntriesRequest {
            term: 2,
            leader_id: nid("n1"),
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![LogEntry {
                index: 2,
                term: 2,
                command: ControlPlaneCommand::Noop,
            }],
            leader_commit: 0,
        };
        let (resp, effects) = core.handle_append_entries(&req, now());
        assert!(resp.success);
        assert_eq!(core.last_log_index(), 2, "conflicting suffix truncated");
        assert_eq!(core.term_at(2), Some(2));
        assert!(
            effects.iter().any(|e| matches!(e, Effect::PersistLog)),
            "log change must persist before ack"
        );
    }

    #[test]
    fn append_entries_duplicate_delivery_is_idempotent() {
        let mut core = fresh_core("n2", &["n1", "n2", "n3"]);
        let req = leader_with_log(&[(1, 1), (2, 1)]);
        let (resp1, _) = core.handle_append_entries(&req, now());
        assert!(resp1.success);
        let log_before = core.log.clone();
        let (resp2, effects) = core.handle_append_entries(&req, now());
        assert!(resp2.success);
        assert_eq!(core.log, log_before);
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::PersistLog)),
            "no re-persist for a pure duplicate"
        );
        assert_eq!(resp2.match_index, 2);
    }

    #[test]
    fn append_entries_prev_check_across_snapshot_boundary() {
        // Snapshot covers 1..=5 (term 2); log tail holds 6..=7.
        let mut core = RaftCore::new(
            nid("n2"),
            voters(&["n1", "n2", "n3"]),
            Duration::from_millis(150),
            HardState {
                current_term: 3,
                voted_for: None,
            },
            SnapshotMeta {
                last_included_index: 5,
                last_included_term: 2,
            },
            vec![
                LogEntry {
                    index: 6,
                    term: 3,
                    command: ControlPlaneCommand::Noop,
                },
                LogEntry {
                    index: 7,
                    term: 3,
                    command: ControlPlaneCommand::Noop,
                },
            ],
        );
        // prev below the snapshot: entries overlapping the snapshot are
        // dropped and the check passes at the boundary.
        let req = AppendEntriesRequest {
            term: 3,
            leader_id: nid("n1"),
            prev_log_index: 3,
            prev_log_term: 1,
            entries: (4..=8)
                .map(|i| LogEntry {
                    index: i,
                    term: if i <= 5 { 2 } else { 3 },
                    command: ControlPlaneCommand::Noop,
                })
                .collect(),
            leader_commit: 5,
        };
        let (resp, _) = core.handle_append_entries(&req, now());
        assert!(resp.success);
        assert_eq!(core.last_log_index(), 8);
        assert_eq!(resp.match_index, 8);
        // prev exactly at the boundary uses the snapshot term.
        let req = AppendEntriesRequest {
            term: 3,
            leader_id: nid("n1"),
            prev_log_index: 5,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 5,
        };
        let (resp, _) = core.handle_append_entries(&req, now());
        assert!(resp.success);
        // ... and a wrong snapshot term is rejected.
        let req = AppendEntriesRequest {
            term: 3,
            leader_id: nid("n1"),
            prev_log_index: 5,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 5,
        };
        let (resp, _) = core.handle_append_entries(&req, now());
        assert!(!resp.success);
    }

    #[test]
    fn append_entries_advances_commit_and_applies() {
        let mut core = fresh_core("n2", &["n1", "n2", "n3"]);
        let mut req = leader_with_log(&[(1, 1), (2, 1)]);
        req.leader_commit = 2;
        let (resp, effects) = core.handle_append_entries(&req, now());
        assert!(resp.success);
        assert_eq!(core.commit_index, 2);
        assert!(effects.iter().any(|e| matches!(e, Effect::ApplyCommitted)));
    }

    #[test]
    fn commit_bounded_by_local_log() {
        let mut core = fresh_core("n2", &["n1", "n2", "n3"]);
        let mut req = leader_with_log(&[(1, 1)]);
        req.leader_commit = 10;
        core.handle_append_entries(&req, now());
        assert_eq!(core.commit_index, 1, "commit must not exceed the local log");
    }

    // --- Leader replication & commit rule ---

    fn make_leader(core: &mut RaftCore) {
        core.on_election_timeout(now());
        grant_all_votes(core, &["n2"]);
        assert!(core.is_leader());
    }

    #[test]
    fn leader_backtracks_next_index_on_mismatch() {
        let mut core = fresh_core("n1", &["n1", "n2", "n3"]);
        make_leader(&mut core);
        for _ in 0..3 {
            core.propose(ControlPlaneCommand::PutPolicy(spec("user/")));
        }
        // Simulate the just-elected default: next = leader_last + 1.
        let last = core.last_log_index();
        if let Role::Leader { next_index, .. } = &mut core.role {
            next_index.insert(nid("n2"), last + 1);
        }
        // n2 rejects with hint: its log ends at index 1.
        let effects = core.handle_append_response(
            core.hard.current_term,
            nid("n2"),
            &AppendEntriesResponse {
                term: core.hard.current_term,
                success: false,
                match_index: 1,
            },
        );
        let Role::Leader { next_index, .. } = &core.role else {
            panic!()
        };
        assert_eq!(next_index[&nid("n2")], 2, "hint-guided back-off");
        // A retry append is sent immediately, starting from index 2.
        let sends = sends_of(&effects);
        assert_eq!(sends.len(), 1);
        match sends[0].1 {
            OutboundRpc::Append(req) => {
                assert_eq!(req.prev_log_index, 1);
            }
            other => panic!("expected Append, got {other:?}"),
        }
    }

    #[test]
    fn current_term_majority_commits() {
        let mut core = fresh_core("n1", &["n1", "n2", "n3"]);
        make_leader(&mut core);
        let ((idx, term), _) = core
            .propose(ControlPlaneCommand::PutPolicy(spec("user/")))
            .unwrap();
        assert_eq!(core.commit_index, 0, "not committed before majority ack");
        let effects = core.handle_append_response(
            term,
            nid("n2"),
            &AppendEntriesResponse {
                term,
                success: true,
                match_index: idx,
            },
        );
        assert_eq!(core.commit_index, idx, "leader + n2 = majority of 3");
        assert!(effects.iter().any(|e| matches!(e, Effect::ApplyCommitted)));
    }

    /// Figure 8: an entry from a PREVIOUS term must not commit by counting
    /// replicas alone; it commits only once a CURRENT-term entry commits.
    #[test]
    fn figure_8_old_term_entry_never_commits_directly() {
        // 5 voters. n1 is leader in term 2 with an uncommitted entry at
        // index 1 (term 2). n1 crashes and comes back as leader in term 4
        // (its Noop for term 4 sits at index 2).
        let mut core = RaftCore::new(
            nid("n1"),
            voters(&["n1", "n2", "n3", "n4", "n5"]),
            Duration::from_millis(150),
            HardState {
                current_term: 3,
                voted_for: None,
            },
            SnapshotMeta::default(),
            vec![LogEntry {
                index: 1,
                term: 2,
                command: ControlPlaneCommand::PutPolicy(spec("old/")),
            }],
        );
        core.on_election_timeout(now()); // term 4 campaign
        grant_all_votes(&mut core, &["n2", "n3"]);
        assert!(core.is_leader());
        assert_eq!(core.hard.current_term, 4);
        let noop_idx = core.last_log_index(); // term-4 Noop at index 2

        // The OLD entry (index 1, term 2) reaches a majority: n2 and n3
        // ack up to index 1 only.
        for peer in ["n2", "n3"] {
            core.handle_append_response(
                4,
                nid(peer),
                &AppendEntriesResponse {
                    term: 4,
                    success: true,
                    match_index: 1,
                },
            );
        }
        assert_eq!(
            core.commit_index, 0,
            "a previous-term entry with majority replication must NOT commit (Figure 8)"
        );

        // Once the CURRENT-term Noop reaches a majority, both commit.
        for peer in ["n2", "n3"] {
            core.handle_append_response(
                4,
                nid(peer),
                &AppendEntriesResponse {
                    term: 4,
                    success: true,
                    match_index: noop_idx,
                },
            );
        }
        assert_eq!(
            core.commit_index, noop_idx,
            "current-term commit carries the old entry with it"
        );
    }

    /// Real networks delay, reorder, and duplicate responses: a success ack
    /// from an EARLIER replication round arriving after a newer one must
    /// not regress `match_index` / `next_index` (the `.max()` guard) — a
    /// regression there would stall or redo replication below the commit
    /// point under reordering.
    #[test]
    fn stale_and_duplicate_append_responses_do_not_regress_progress() {
        let mut core = fresh_core("n1", &["n1", "n2", "n3"]);
        make_leader(&mut core);
        for _ in 0..3 {
            core.propose(ControlPlaneCommand::PutPolicy(spec("user/")));
        }
        let last = core.last_log_index(); // noop + 3 proposals = 4
        let term = core.hard.current_term;

        // Fresh response: n2 has everything.
        core.handle_append_response(
            term,
            nid("n2"),
            &AppendEntriesResponse {
                term,
                success: true,
                match_index: last,
            },
        );
        assert_eq!(core.commit_index, last, "leader + n2 commit everything");

        let progress = |core: &RaftCore| {
            let Role::Leader {
                next_index,
                match_index,
            } = &core.role
            else {
                panic!("must still be leader");
            };
            (match_index[&nid("n2")], next_index[&nid("n2")])
        };
        assert_eq!(progress(&core), (last, last + 1));

        // A DELAYED success from an earlier round (match_index = 1) arrives
        // late: progress must not move backwards, commit must stand, and no
        // redundant re-send below the acked point may be triggered.
        let effects = core.handle_append_response(
            term,
            nid("n2"),
            &AppendEntriesResponse {
                term,
                success: true,
                match_index: 1,
            },
        );
        assert_eq!(
            progress(&core),
            (last, last + 1),
            "stale response must not regress match/next index"
        );
        assert_eq!(core.commit_index, last);
        assert!(
            sends_of(&effects).is_empty(),
            "no redundant re-send for a stale ack"
        );

        // A DUPLICATED copy of the fresh response is idempotent.
        let effects = core.handle_append_response(
            term,
            nid("n2"),
            &AppendEntriesResponse {
                term,
                success: true,
                match_index: last,
            },
        );
        assert_eq!(progress(&core), (last, last + 1));
        assert_eq!(core.commit_index, last);
        assert!(sends_of(&effects).is_empty());
    }

    #[test]
    fn stale_leader_steps_down_on_higher_term_ack() {
        let mut core = fresh_core("n1", &["n1", "n2", "n3"]);
        make_leader(&mut core);
        let effects = core.handle_append_response(
            core.hard.current_term,
            nid("n2"),
            &AppendEntriesResponse {
                term: 9,
                success: false,
                match_index: 0,
            },
        );
        assert!(!core.is_leader());
        assert_eq!(core.hard.current_term, 9);
        assert!(effects.iter().any(|e| matches!(e, Effect::SteppedDown)));
    }

    #[test]
    fn even_cluster_requires_strict_majority() {
        let mut core = fresh_core("n1", &["n1", "n2", "n3", "n4"]);
        assert_eq!(core.majority(), 3);
        core.on_election_timeout(now());
        grant_all_votes(&mut core, &["n2"]);
        assert!(!core.is_leader(), "2 of 4 is not a majority");
        grant_all_votes(&mut core, &["n3"]);
        assert!(core.is_leader(), "3 of 4 is a majority");
    }

    #[test]
    fn propose_rejected_when_not_leader() {
        let mut core = fresh_core("n1", &["n1", "n2", "n3"]);
        assert!(core.propose(ControlPlaneCommand::Noop).is_none());
    }

    #[test]
    fn snapshot_ack_advances_peer_and_switches_to_append() {
        let mut core = fresh_core("n1", &["n1", "n2", "n3"]);
        make_leader(&mut core);
        for _ in 0..4 {
            core.propose(ControlPlaneCommand::PutPolicy(spec("p/")));
        }
        // Compact through index 3 (pretend applied).
        core.commit_index = 5;
        core.last_applied = 5;
        let term3 = core.term_at(3).unwrap();
        core.drop_log_through(3);
        core.snapshot_meta = SnapshotMeta {
            last_included_index: 3,
            last_included_term: term3,
        };
        // Force n2's next below the snapshot boundary.
        if let Role::Leader { next_index, .. } = &mut core.role {
            next_index.insert(nid("n2"), 1);
        }
        let effects = core.broadcast_append();
        let snapshot_send = sends_of(&effects)
            .into_iter()
            .find(|(to, _)| **to == nid("n2"))
            .unwrap();
        assert!(matches!(snapshot_send.1, OutboundRpc::Snapshot));

        // The ack promotes the peer past the snapshot and resumes appends.
        let effects =
            core.handle_snapshot_ack(core.hard.current_term, nid("n2"), core.hard.current_term, 3);
        let Role::Leader { next_index, .. } = &core.role else {
            panic!()
        };
        assert_eq!(next_index[&nid("n2")], 4);
        let sends = sends_of(&effects);
        assert_eq!(sends.len(), 1);
        assert!(matches!(sends[0].1, OutboundRpc::Append(_)));
    }

    #[test]
    fn install_snapshot_meta_replaces_log_and_commit() {
        let mut core = fresh_core("n2", &["n1", "n2", "n3"]);
        core.log.push(LogEntry {
            index: 1,
            term: 1,
            command: ControlPlaneCommand::Noop,
        });
        let (resp, install, effects) =
            core.handle_install_snapshot_meta(2, &nid("n1"), 5, 2, now());
        assert_eq!(resp.term, 2);
        assert!(install);
        assert!(core.log.is_empty(), "inconsistent log discarded");
        assert_eq!(core.commit_index, 5);
        assert_eq!(core.snapshot_meta.last_included_index, 5);
        assert!(effects.iter().any(|e| matches!(e, Effect::PersistLog)));

        // A stale snapshot (at or below commit) is ignored.
        let (_, install, _) = core.handle_install_snapshot_meta(2, &nid("n1"), 4, 2, now());
        assert!(!install);
    }
}
