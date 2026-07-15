//! Control-plane consensus facade (FR-009).
//!
//! Historically this was a stateless majority check over caller-supplied
//! approval lists. It is now a thin, `Clone`-able facade over the real Raft
//! consensus in [`crate::control_plane::raft`]: proposals are appended to
//! the replicated log by the leader, committed on a majority of the static
//! voter set, and applied — in commit order — to the system namespace on
//! every voter.
//!
//! Two modes:
//! - **Raft**: production and single-node test wiring; proposals go through
//!   `RaftNode::propose_and_wait`.
//! - **Detached**: no consensus configured. Every proposal is rejected with
//!   `PolicyDenied` (HTTP 403), matching the old "insufficient approvals"
//!   behavior that test scaffolding constructed via
//!   `ControlPlaneConsensus::new(vec![])`.
//!
//! NOTE: this type deliberately does NOT share anything with the data-plane
//! majority machinery (`MajorityCertificate::has_majority` and friends) —
//! those certify data-plane writes and are a different concept entirely.

#[cfg(feature = "native-runtime")]
use crate::error::CrdtError;
use crate::types::NodeId;

#[cfg(feature = "native-runtime")]
use std::sync::Arc;

#[cfg(feature = "native-runtime")]
use crate::control_plane::system_namespace::AuthorityDefinition;
#[cfg(feature = "native-runtime")]
use crate::placement::PlacementPolicy;

#[cfg(feature = "native-runtime")]
use super::raft::node::{RaftConfig, RaftNode, RaftStatus};
#[cfg(feature = "native-runtime")]
use super::raft::types::{ApplyOutcome, AuthoritySpec, ControlPlaneCommand, PolicySpec};

/// Clone-able handle to the control-plane consensus. Handlers clone this
/// out of the `AppState` mutex and drop the lock BEFORE awaiting a
/// proposal, so a slow commit never blocks other control-plane requests or
/// the Raft internals.
#[derive(Clone)]
pub struct ControlPlaneConsensus {
    mode: Mode,
}

#[derive(Clone)]
enum Mode {
    /// No consensus configured: all proposals are denied.
    Detached,
    #[cfg(feature = "native-runtime")]
    Raft(Arc<RaftNode>),
}

impl ControlPlaneConsensus {
    /// Compatibility constructor (signature preserved from the MVP): the
    /// argument is ignored and the instance is DETACHED — every proposal
    /// fails with `PolicyDenied`, the same 403 the old implementation
    /// returned for missing approvals. Production wiring uses
    /// [`ControlPlaneConsensus::with_raft`].
    pub fn new(_authority_nodes: Vec<NodeId>) -> Self {
        Self {
            mode: Mode::Detached,
        }
    }

    /// Wrap a running Raft node.
    #[cfg(feature = "native-runtime")]
    pub fn with_raft(node: Arc<RaftNode>) -> Self {
        Self {
            mode: Mode::Raft(node),
        }
    }

    /// Test helper: a single-voter Raft node over in-memory storage that is
    /// already leader (single-voter clusters elect themselves in
    /// `RaftNode::new`), sharing `namespace` with the caller. Proposals
    /// commit and apply within a single `propose_and_wait` call, so no
    /// driver task is needed.
    #[cfg(feature = "native-runtime")]
    pub fn single_node_for_test(
        self_id: NodeId,
        namespace: Arc<std::sync::RwLock<crate::control_plane::system_namespace::SystemNamespace>>,
    ) -> Self {
        use super::raft::storage::MemRaftStorage;
        use super::raft::transport::NoopTransport;
        let voters = [self_id.clone()].into_iter().collect();
        let node = RaftNode::new(
            self_id,
            voters,
            RaftConfig::default(),
            Arc::new(MemRaftStorage::new()),
            Arc::new(NoopTransport),
            namespace,
            None,
        )
        .expect("in-memory raft storage cannot fail to load");
        Self::with_raft(node)
    }

    /// The underlying Raft node, when attached.
    #[cfg(feature = "native-runtime")]
    pub fn raft_handle(&self) -> Option<Arc<RaftNode>> {
        match &self.mode {
            Mode::Detached => None,
            Mode::Raft(node) => Some(Arc::clone(node)),
        }
    }

    #[cfg(feature = "native-runtime")]
    fn detached_error() -> CrdtError {
        CrdtError::PolicyDenied("control-plane consensus is not configured (detached mode)".into())
    }

    /// Propose a placement policy upsert and wait for commit + apply.
    /// Returns the applied policy carrying its commit-order version.
    #[cfg(feature = "native-runtime")]
    pub async fn propose_policy_update(
        &self,
        spec: PolicySpec,
    ) -> Result<PlacementPolicy, CrdtError> {
        match &self.mode {
            Mode::Detached => Err(Self::detached_error()),
            Mode::Raft(node) => {
                match node
                    .propose_and_wait(ControlPlaneCommand::PutPolicy(spec))
                    .await?
                {
                    ApplyOutcome::PolicyApplied(policy) => Ok(policy),
                    ApplyOutcome::Noop => Err(CrdtError::InvalidArgument(
                        "replica_count must be at least 1".into(),
                    )),
                    other => Err(CrdtError::Internal(format!(
                        "unexpected apply outcome for policy update: {other:?}"
                    ))),
                }
            }
        }
    }

    /// Propose a placement policy removal and wait for commit + apply.
    /// `Ok(None)` means the prefix did not exist (callers map to 404).
    #[cfg(feature = "native-runtime")]
    pub async fn propose_policy_removal(
        &self,
        prefix: &str,
    ) -> Result<Option<PlacementPolicy>, CrdtError> {
        match &self.mode {
            Mode::Detached => Err(Self::detached_error()),
            Mode::Raft(node) => {
                match node
                    .propose_and_wait(ControlPlaneCommand::RemovePolicy {
                        prefix: prefix.to_string(),
                    })
                    .await?
                {
                    ApplyOutcome::PolicyRemoved(removed) => Ok(removed),
                    other => Err(CrdtError::Internal(format!(
                        "unexpected apply outcome for policy removal: {other:?}"
                    ))),
                }
            }
        }
    }

    /// Propose a manual authority definition upsert and wait for commit +
    /// apply.
    #[cfg(feature = "native-runtime")]
    pub async fn propose_authority_update(
        &self,
        spec: AuthoritySpec,
    ) -> Result<AuthorityDefinition, CrdtError> {
        match &self.mode {
            Mode::Detached => Err(Self::detached_error()),
            Mode::Raft(node) => {
                match node
                    .propose_and_wait(ControlPlaneCommand::PutAuthority(spec))
                    .await?
                {
                    ApplyOutcome::AuthorityApplied(def) => Ok(def),
                    other => Err(CrdtError::Internal(format!(
                        "unexpected apply outcome for authority update: {other:?}"
                    ))),
                }
            }
        }
    }

    /// Whether this node is currently the control-plane Raft leader.
    pub fn is_leader(&self) -> bool {
        match &self.mode {
            Mode::Detached => false,
            #[cfg(feature = "native-runtime")]
            Mode::Raft(node) => node.is_leader(),
        }
    }

    /// Best-known leader `(id, resolved address)`.
    pub fn leader_hint(&self) -> Option<(NodeId, Option<String>)> {
        match &self.mode {
            Mode::Detached => None,
            #[cfg(feature = "native-runtime")]
            Mode::Raft(node) => node.leader_hint(),
        }
    }

    /// Consensus status for the observability endpoint. `None` = detached.
    #[cfg(feature = "native-runtime")]
    pub fn status(&self) -> Option<RaftStatus> {
        match &self.mode {
            Mode::Detached => None,
            Mode::Raft(node) => Some(node.status()),
        }
    }
}

#[cfg(all(test, feature = "native-runtime"))]
mod tests {
    use super::*;
    use crate::control_plane::system_namespace::SystemNamespace;
    use std::collections::BTreeSet;
    use std::sync::{Arc, RwLock};

    fn node_id(s: &str) -> NodeId {
        NodeId(s.into())
    }

    fn policy_spec(prefix: &str, replica_count: usize) -> PolicySpec {
        PolicySpec {
            prefix: prefix.into(),
            replica_count,
            required_tags: BTreeSet::new(),
            forbidden_tags: BTreeSet::new(),
            allow_local_write_on_partition: false,
            certified: false,
            max_read_latency_ms: None,
            preferred_cost_tier: None,
        }
    }

    fn fresh_namespace() -> Arc<RwLock<SystemNamespace>> {
        Arc::new(RwLock::new(SystemNamespace::new()))
    }

    // --- Detached mode (compat with the old `new(vec![])` scaffolding) ---

    #[tokio::test]
    async fn detached_rejects_all_proposals_with_policy_denied() {
        let consensus = ControlPlaneConsensus::new(vec![node_id("n1")]);
        assert!(!consensus.is_leader());
        assert!(consensus.leader_hint().is_none());
        assert!(consensus.raft_handle().is_none());

        let err = consensus
            .propose_policy_update(policy_spec("user/", 3))
            .await
            .unwrap_err();
        assert!(matches!(err, CrdtError::PolicyDenied(_)), "{err:?}");
        let err = consensus.propose_policy_removal("user/").await.unwrap_err();
        assert!(matches!(err, CrdtError::PolicyDenied(_)));
        let err = consensus
            .propose_authority_update(AuthoritySpec {
                prefix: "user/".into(),
                authority_nodes: vec![node_id("a1")],
            })
            .await
            .unwrap_err();
        assert!(matches!(err, CrdtError::PolicyDenied(_)));
    }

    // --- Single-node mode ---

    #[tokio::test]
    async fn single_node_commits_immediately_and_updates_namespace() {
        let namespace = fresh_namespace();
        let consensus =
            ControlPlaneConsensus::single_node_for_test(node_id("solo"), Arc::clone(&namespace));
        assert!(consensus.is_leader());

        let policy = consensus
            .propose_policy_update(policy_spec("user/", 3))
            .await
            .unwrap();
        assert_eq!(policy.key_range.prefix, "user/");
        {
            let ns = namespace.read().unwrap();
            let stored = ns.get_placement_policy("user/").unwrap();
            assert_eq!(stored.version, policy.version);
            assert_eq!(stored.replica_count, 3);
        }

        // Versions increase with commit order.
        let policy2 = consensus
            .propose_policy_update(policy_spec("order/", 2))
            .await
            .unwrap();
        assert!(policy2.version.0 > policy.version.0);

        // Removal round-trip.
        let removed = consensus.propose_policy_removal("user/").await.unwrap();
        assert_eq!(removed.unwrap().key_range.prefix, "user/");
        assert!(
            namespace
                .read()
                .unwrap()
                .get_placement_policy("user/")
                .is_none()
        );
        // Missing prefix maps to Ok(None) (handler turns it into 404).
        assert!(
            consensus
                .propose_policy_removal("user/")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn single_node_authority_update_applies() {
        let namespace = fresh_namespace();
        let consensus =
            ControlPlaneConsensus::single_node_for_test(node_id("solo"), Arc::clone(&namespace));
        let def = consensus
            .propose_authority_update(AuthoritySpec {
                prefix: "user/".into(),
                authority_nodes: vec![node_id("a1"), node_id("a2")],
            })
            .await
            .unwrap();
        assert!(!def.auto_generated);
        let ns = namespace.read().unwrap();
        let stored = ns.get_authority_definition("user/").unwrap();
        assert_eq!(stored.authority_nodes.len(), 2);
    }

    #[tokio::test]
    async fn failed_proposal_does_not_change_namespace() {
        let namespace = fresh_namespace();
        let consensus =
            ControlPlaneConsensus::single_node_for_test(node_id("solo"), Arc::clone(&namespace));
        let version_before = namespace.read().unwrap().version().0;
        let err = consensus
            .propose_policy_update(policy_spec("bad/", 0))
            .await
            .unwrap_err();
        assert!(matches!(err, CrdtError::InvalidArgument(_)), "{err:?}");
        let ns = namespace.read().unwrap();
        assert!(ns.get_placement_policy("bad/").is_none());
        assert_eq!(ns.version().0, version_before);
    }

    #[tokio::test]
    async fn single_node_leader_hint_points_to_self() {
        let namespace = fresh_namespace();
        let consensus =
            ControlPlaneConsensus::single_node_for_test(node_id("solo"), Arc::clone(&namespace));
        let (leader, _addr) = consensus.leader_hint().unwrap();
        assert_eq!(leader, node_id("solo"));
        let status = consensus.status().unwrap();
        assert_eq!(status.role, "leader");
        assert_eq!(status.voters, vec!["solo".to_string()]);
    }
}
