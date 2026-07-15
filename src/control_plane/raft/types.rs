//! Wire, log, and state-machine types for the control-plane Raft consensus.
//!
//! # bincode compatibility rules
//!
//! Every type in this module travels over the internal bincode wire format
//! (`application/octet-stream`) or is persisted to the Raft log file. bincode
//! is field-order dependent and incompatible with `skip_serializing_if`
//! (see the precedent note in `network/frontier_sync.rs`), so:
//!
//! - NO `#[serde(skip_serializing_if = ...)]` anywhere in this module.
//! - `Option` fields use plain serde encoding.
//! - New fields may only ever be appended at the END of a struct, and doing
//!   so still breaks bincode wire compatibility with older nodes — adding
//!   fields requires a new endpoint or a JSON-only migration.
//!
//! `PlacementPolicy` / `AuthorityDefinition` carry `skip_serializing_if` /
//! `#[serde(default)]` attributes, so they are never placed on the wire or
//! in the log directly; the mirror types [`PolicySpec`] / [`AuthoritySpec`]
//! are used instead.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::control_plane::system_namespace::AuthorityDefinition;
use crate::placement::PlacementPolicy;
use crate::types::{KeyRange, NodeId, PolicyVersion, Tag};

/// Mirror of [`PlacementPolicy`] without a version and without
/// serde attributes that break bincode. Tag sets are `BTreeSet` so the
/// encoding (and therefore log entry bytes) is deterministic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicySpec {
    pub prefix: String,
    pub replica_count: usize,
    pub required_tags: BTreeSet<String>,
    pub forbidden_tags: BTreeSet<String>,
    pub allow_local_write_on_partition: bool,
    pub certified: bool,
    pub max_read_latency_ms: Option<f64>,
    pub preferred_cost_tier: Option<String>,
}

impl PolicySpec {
    /// Build a spec from an existing policy (used by `Bootstrap` import).
    pub fn from_policy(policy: &PlacementPolicy) -> Self {
        Self {
            prefix: policy.key_range.prefix.clone(),
            replica_count: policy.replica_count,
            required_tags: policy.required_tags.iter().map(|t| t.0.clone()).collect(),
            forbidden_tags: policy.forbidden_tags.iter().map(|t| t.0.clone()).collect(),
            allow_local_write_on_partition: policy.allow_local_write_on_partition,
            certified: policy.certified,
            max_read_latency_ms: policy.max_read_latency_ms,
            preferred_cost_tier: policy.preferred_cost_tier.clone(),
        }
    }

    /// Materialize a [`PlacementPolicy`] at the given (commit-order assigned)
    /// version.
    pub fn to_policy(&self, version: PolicyVersion) -> PlacementPolicy {
        let mut policy = PlacementPolicy::new(
            version,
            KeyRange {
                prefix: self.prefix.clone(),
            },
            self.replica_count,
        );
        if !self.required_tags.is_empty() {
            policy = policy
                .with_required_tags(self.required_tags.iter().map(|t| Tag(t.clone())).collect());
        }
        if !self.forbidden_tags.is_empty() {
            policy = policy
                .with_forbidden_tags(self.forbidden_tags.iter().map(|t| Tag(t.clone())).collect());
        }
        policy = policy.with_local_write_on_partition(self.allow_local_write_on_partition);
        policy = policy.with_certified(self.certified);
        if let Some(ms) = self.max_read_latency_ms {
            policy = policy.with_max_read_latency_ms(ms);
        }
        if let Some(tier) = &self.preferred_cost_tier {
            policy = policy.with_preferred_cost_tier(tier.clone());
        }
        policy
    }
}

/// Mirror of a MANUAL [`AuthorityDefinition`] (`auto_generated` is always
/// `false` — auto-generated definitions are node-local derivations and are
/// never replicated).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthoritySpec {
    pub prefix: String,
    pub authority_nodes: Vec<NodeId>,
}

impl AuthoritySpec {
    /// Build a spec from an existing manual authority definition.
    pub fn from_definition(def: &AuthorityDefinition) -> Self {
        Self {
            prefix: def.key_range.prefix.clone(),
            authority_nodes: def.authority_nodes.clone(),
        }
    }

    /// Materialize a manual [`AuthorityDefinition`].
    pub fn to_definition(&self) -> AuthorityDefinition {
        AuthorityDefinition {
            key_range: KeyRange {
                prefix: self.prefix.clone(),
            },
            authority_nodes: self.authority_nodes.clone(),
            auto_generated: false,
        }
    }
}

/// A command in the replicated control-plane log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ControlPlaneCommand {
    /// Appended by every newly elected leader. Committing a current-term
    /// no-op is what allows entries from previous terms to commit indirectly
    /// (the Figure 8 rule).
    Noop,
    /// Reset-and-import of the leader's local replicated core, proposed once
    /// by the first leader of a fresh cluster. Idempotent: applies only when
    /// the state machine is not yet bootstrapped.
    Bootstrap {
        /// The proposing leader's local `ns.version().0` — floors the
        /// replicated version counter so policy version fencing stays
        /// monotone across the migration from pre-Raft state.
        version_floor: u64,
        /// The leader's local placement policies (prefix ascending).
        policies: Vec<PolicySpec>,
        /// The leader's local manual authority definitions (catch-all
        /// included).
        authorities: Vec<AuthoritySpec>,
    },
    /// Upsert a placement policy. The policy version is assigned at APPLY
    /// time from the replicated version counter (commit order), never at
    /// propose time.
    PutPolicy(PolicySpec),
    /// Remove the placement policy for a prefix (no-op if absent).
    RemovePolicy { prefix: String },
    /// Upsert a manual authority definition.
    PutAuthority(AuthoritySpec),
}

/// One entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: ControlPlaneCommand,
}

/// A placement policy spec together with its commit-order assigned version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionedPolicy {
    pub version: u64,
    pub spec: PolicySpec,
}

/// The replicated control-plane core — the exact contents of a snapshot.
///
/// Contains ONLY replicated state: placement policies (with commit-assigned
/// versions), manual authority definitions, and the deterministic version
/// counter. Auto-generated authorities, `version_history`, and the namespace
/// version are node-local derivations and deliberately excluded.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ControlPlaneState {
    /// Whether a `Bootstrap` entry has been applied.
    pub bootstrapped: bool,
    /// Deterministic policy version counter; incremented once per applied
    /// policy upsert. Replays identically on every node.
    pub version_counter: u64,
    /// Placement policies keyed by prefix.
    pub policies: BTreeMap<String, VersionedPolicy>,
    /// Manual authority definitions keyed by prefix.
    pub authorities: BTreeMap<String, AuthoritySpec>,
}

// ---------------------------------------------------------------
// RPC wire types (bincode + JSON fallback)
// ---------------------------------------------------------------

/// `POST /api/internal/raft/vote` request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// `POST /api/internal/raft/vote` response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

/// `POST /api/internal/raft/append` request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

/// `POST /api/internal/raft/append` response.
///
/// On success, `match_index` is the highest log index known to be replicated
/// on the follower. On failure it is the follower's last log index, used by
/// the leader as a back-off hint for `next_index`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

/// `POST /api/internal/raft/snapshot` request (single-message "lite"
/// InstallSnapshot: the control-plane state is small by construction).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub state: ControlPlaneState,
}

/// `POST /api/internal/raft/snapshot` response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub term: u64,
}

/// Result of applying one committed log entry to the state machine.
#[derive(Debug, Clone)]
pub enum ApplyOutcome {
    /// The applied policy, carrying its commit-order assigned version.
    PolicyApplied(PlacementPolicy),
    /// The removed policy, or `None` when the prefix did not exist
    /// (handlers map `None` to 404).
    PolicyRemoved(Option<PlacementPolicy>),
    /// The applied manual authority definition.
    AuthorityApplied(AuthorityDefinition),
    /// A `Bootstrap` entry was applied (or skipped as already bootstrapped).
    Bootstrapped,
    /// A no-op entry (leader election marker, or a defensively skipped
    /// invalid command).
    Noop,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_policy_spec() -> PolicySpec {
        PolicySpec {
            prefix: "user/".into(),
            replica_count: 3,
            required_tags: ["dc:tokyo".to_string()].into_iter().collect(),
            forbidden_tags: BTreeSet::new(),
            allow_local_write_on_partition: true,
            certified: true,
            max_read_latency_ms: Some(120.5),
            preferred_cost_tier: Some("low".into()),
        }
    }

    fn bincode_roundtrip<T>(value: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let bytes = bincode::serde::encode_to_vec(value, bincode::config::standard()).unwrap();
        let (decoded, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        decoded
    }

    #[test]
    fn policy_spec_roundtrips_through_placement_policy() {
        let spec = sample_policy_spec();
        let policy = spec.to_policy(PolicyVersion(7));
        assert_eq!(policy.version, PolicyVersion(7));
        assert_eq!(policy.replica_count, 3);
        assert!(policy.certified);
        assert_eq!(policy.max_read_latency_ms, Some(120.5));
        let back = PolicySpec::from_policy(&policy);
        assert_eq!(back, spec);
    }

    #[test]
    fn authority_spec_roundtrips_through_definition() {
        let spec = AuthoritySpec {
            prefix: "order/".into(),
            authority_nodes: vec![NodeId("a1".into()), NodeId("a2".into())],
        };
        let def = spec.to_definition();
        assert!(!def.auto_generated);
        assert_eq!(AuthoritySpec::from_definition(&def), spec);
    }

    /// Regression guard: every wire/log type must survive a bincode
    /// round-trip, including `Option` fields in both `Some` and `None`
    /// states (a `skip_serializing_if` regression would break this).
    #[test]
    fn bincode_roundtrip_all_wire_types() {
        let mut spec = sample_policy_spec();
        assert_eq!(bincode_roundtrip(&spec), spec);
        spec.max_read_latency_ms = None;
        spec.preferred_cost_tier = None;
        assert_eq!(bincode_roundtrip(&spec), spec);

        let entry = LogEntry {
            index: 4,
            term: 2,
            command: ControlPlaneCommand::PutPolicy(sample_policy_spec()),
        };
        assert_eq!(bincode_roundtrip(&entry), entry);

        let bootstrap = LogEntry {
            index: 1,
            term: 1,
            command: ControlPlaneCommand::Bootstrap {
                version_floor: 9,
                policies: vec![sample_policy_spec()],
                authorities: vec![AuthoritySpec {
                    prefix: String::new(),
                    authority_nodes: vec![NodeId("auth-1".into())],
                }],
            },
        };
        assert_eq!(bincode_roundtrip(&bootstrap), bootstrap);

        let mut state = ControlPlaneState {
            bootstrapped: true,
            version_counter: 12,
            ..Default::default()
        };
        state.policies.insert(
            "user/".into(),
            VersionedPolicy {
                version: 12,
                spec: sample_policy_spec(),
            },
        );
        state.authorities.insert(
            String::new(),
            AuthoritySpec {
                prefix: String::new(),
                authority_nodes: vec![NodeId("auth-1".into())],
            },
        );
        assert_eq!(bincode_roundtrip(&state), state);

        let vote_req = RequestVoteRequest {
            term: 3,
            candidate_id: NodeId("n1".into()),
            last_log_index: 10,
            last_log_term: 2,
        };
        assert_eq!(bincode_roundtrip(&vote_req), vote_req);

        let vote_resp = RequestVoteResponse {
            term: 3,
            vote_granted: true,
        };
        assert_eq!(bincode_roundtrip(&vote_resp), vote_resp);

        let append_req = AppendEntriesRequest {
            term: 3,
            leader_id: NodeId("n1".into()),
            prev_log_index: 4,
            prev_log_term: 2,
            entries: vec![entry.clone()],
            leader_commit: 4,
        };
        assert_eq!(bincode_roundtrip(&append_req), append_req);

        let append_resp = AppendEntriesResponse {
            term: 3,
            success: false,
            match_index: 2,
        };
        assert_eq!(bincode_roundtrip(&append_resp), append_resp);

        let snap_req = InstallSnapshotRequest {
            term: 3,
            leader_id: NodeId("n1".into()),
            last_included_index: 8,
            last_included_term: 2,
            state: state.clone(),
        };
        assert_eq!(bincode_roundtrip(&snap_req), snap_req);

        let snap_resp = InstallSnapshotResponse { term: 3 };
        assert_eq!(bincode_roundtrip(&snap_resp), snap_resp);
    }

    /// JSON fallback must also round-trip (rolling upgrade path).
    #[test]
    fn json_roundtrip_wire_types() {
        let entry = LogEntry {
            index: 4,
            term: 2,
            command: ControlPlaneCommand::RemovePolicy {
                prefix: "user/".into(),
            },
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: LogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back, entry);

        let spec = sample_policy_spec();
        let json = serde_json::to_string(&spec).unwrap();
        let back: PolicySpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, spec);
    }
}
