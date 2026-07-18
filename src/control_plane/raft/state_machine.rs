//! Deterministic application of committed control-plane log entries.
//!
//! [`ControlPlaneState`] is the replicated source of truth; the
//! [`SystemNamespace`] is a projection kept for the rest of the system
//! (placement, fencing, HTTP reads). Namespace mutation goes through the
//! existing `SystemNamespace` API only, so downstream consumers —
//! `NodeRunner::detect_version_changes` driving `fence_version`,
//! `recalculate_authorities`, rebalancing — follow automatically via their
//! version polling. `fence_version` / `recalculate_authorities` are NEVER
//! called from here (one-way dependency: control-plane commit → namespace
//! version bump → runner polling → fence).
//!
//! Determinism rules:
//! - Policy versions are assigned here, at apply time, from the replicated
//!   `version_counter` — commit order decides versions identically on every
//!   node. Propose-time numbering is forbidden.
//! - Invalid commands (e.g. `replica_count == 0`, which propose-side
//!   validation should have rejected) apply as deterministic no-ops so that
//!   every replica takes the same branch.

use crate::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use crate::placement::PlacementPolicy;
use crate::types::PolicyVersion;

use super::types::{
    ApplyOutcome, ControlPlaneCommand, ControlPlaneState, LogEntry, VersionedPolicy,
};

/// Apply one committed entry to the replicated state and its namespace
/// projection. Must be called exactly once per index, in log order.
pub fn apply(
    entry: &LogEntry,
    state: &mut ControlPlaneState,
    ns: &mut SystemNamespace,
) -> ApplyOutcome {
    match &entry.command {
        ControlPlaneCommand::Noop => ApplyOutcome::Noop,
        ControlPlaneCommand::PutPolicy(spec) => {
            if spec.replica_count == 0 {
                // Deterministic defensive no-op; propose-side validation
                // rejects this before it can reach the log.
                return ApplyOutcome::Noop;
            }
            state.version_counter += 1;
            let policy = spec.to_policy(PolicyVersion(state.version_counter));
            ns.set_placement_policy(policy.clone())
                .expect("replica_count >= 1 was checked above");
            state.policies.insert(
                spec.prefix.clone(),
                VersionedPolicy {
                    version: state.version_counter,
                    spec: spec.clone(),
                },
            );
            ApplyOutcome::PolicyApplied(policy)
        }
        ControlPlaneCommand::RemovePolicy { prefix } => {
            match state.policies.remove(prefix) {
                Some(versioned) => {
                    let removed = ns.remove_placement_policy(prefix).unwrap_or_else(|| {
                        // The namespace projection should always track the
                        // replicated state; reconstruct defensively if not.
                        versioned.spec.to_policy(PolicyVersion(versioned.version))
                    });
                    ApplyOutcome::PolicyRemoved(Some(removed))
                }
                None => ApplyOutcome::PolicyRemoved(None),
            }
        }
        ControlPlaneCommand::PutAuthority(spec) => {
            let def = spec.to_definition();
            ns.set_authority_definition(def.clone());
            state.authorities.insert(spec.prefix.clone(), spec.clone());
            ApplyOutcome::AuthorityApplied(def)
        }
        ControlPlaneCommand::Bootstrap {
            version_floor,
            policies,
            authorities,
        } => {
            if state.bootstrapped {
                // Idempotent: a leader change can re-propose Bootstrap.
                return ApplyOutcome::Bootstrapped;
            }
            state.bootstrapped = true;
            state.version_counter = state.version_counter.max(*version_floor);
            if *version_floor < ns.version().0 {
                tracing::warn!(
                    version_floor,
                    local_ns_version = ns.version().0,
                    "Bootstrap version floor is below the local namespace version: \
                     replicated policy versions will restart below versions this \
                     node already used and fenced. Re-assigned versions are \
                     unfenced by the runner when they become current again \
                     (certification would otherwise stall); still, prefer electing \
                     the node with the freshest namespace as the first leader \
                     (see ops-guide §14.2)"
                );
            }

            // Deterministic import order: prefix ascending. The proposer
            // builds the vector sorted already; sort again defensively.
            let mut sorted = policies.clone();
            sorted.sort_by(|a, b| a.prefix.cmp(&b.prefix));

            state.policies.clear();
            let mut ns_policies = Vec::new();
            for spec in sorted {
                if spec.replica_count == 0 {
                    continue; // deterministic skip
                }
                state.version_counter += 1;
                ns_policies.push(spec.to_policy(PolicyVersion(state.version_counter)));
                state.policies.insert(
                    spec.prefix.clone(),
                    VersionedPolicy {
                        version: state.version_counter,
                        spec,
                    },
                );
            }

            state.authorities.clear();
            let mut ns_defs = Vec::new();
            for spec in authorities {
                ns_defs.push(spec.to_definition());
                state.authorities.insert(spec.prefix.clone(), spec.clone());
            }

            ns.replace_control_plane_core(ns_policies, ns_defs);
            ApplyOutcome::Bootstrapped
        }
    }
}

/// Install a full replicated state into the namespace projection
/// (InstallSnapshot and startup recovery). Reset-and-import: replaces all
/// placement policies and manual authority definitions in one namespace
/// version bump; auto-generated authorities and history stay node-local.
pub fn install(state: &ControlPlaneState, ns: &mut SystemNamespace) {
    let policies: Vec<PlacementPolicy> = state
        .policies
        .values()
        .map(|vp| vp.spec.to_policy(PolicyVersion(vp.version)))
        .collect();
    let defs: Vec<AuthorityDefinition> = state
        .authorities
        .values()
        .map(|spec| spec.to_definition())
        .collect();
    ns.replace_control_plane_core(policies, defs);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::raft::types::{AuthoritySpec, PolicySpec};
    use std::collections::BTreeSet;

    fn spec(prefix: &str, replica_count: usize) -> PolicySpec {
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

    fn entry(index: u64, command: ControlPlaneCommand) -> LogEntry {
        LogEntry {
            index,
            term: 1,
            command,
        }
    }

    fn auth_spec(prefix: &str, nodes: &[&str]) -> AuthoritySpec {
        AuthoritySpec {
            prefix: prefix.into(),
            authority_nodes: nodes
                .iter()
                .map(|s| crate::types::NodeId((*s).into()))
                .collect(),
        }
    }

    fn sample_log() -> Vec<LogEntry> {
        vec![
            entry(1, ControlPlaneCommand::Noop),
            entry(
                2,
                ControlPlaneCommand::Bootstrap {
                    version_floor: 3,
                    policies: vec![spec("seed/", 2)],
                    authorities: vec![auth_spec("", &["auth-1"])],
                },
            ),
            entry(3, ControlPlaneCommand::PutPolicy(spec("user/", 3))),
            entry(4, ControlPlaneCommand::PutPolicy(spec("order/", 2))),
            entry(
                5,
                ControlPlaneCommand::RemovePolicy {
                    prefix: "user/".into(),
                },
            ),
            entry(6, ControlPlaneCommand::PutPolicy(spec("user/", 5))),
            entry(
                7,
                ControlPlaneCommand::PutAuthority(auth_spec("a/", &["x"])),
            ),
        ]
    }

    fn replay(log: &[LogEntry]) -> (ControlPlaneState, SystemNamespace) {
        let mut state = ControlPlaneState::default();
        let mut ns = SystemNamespace::new();
        for e in log {
            apply(e, &mut state, &mut ns);
        }
        (state, ns)
    }

    #[test]
    fn same_log_produces_same_state_deterministically() {
        let log = sample_log();
        let (s1, ns1) = replay(&log);
        let (s2, ns2) = replay(&log);
        assert_eq!(s1, s2, "two replays must produce identical state");
        // Namespace projections agree on the replicated core.
        for (prefix, vp) in &s1.policies {
            let p1 = ns1.get_placement_policy(prefix).unwrap();
            let p2 = ns2.get_placement_policy(prefix).unwrap();
            assert_eq!(p1.version.0, vp.version);
            assert_eq!(p1.version, p2.version);
            assert_eq!(p1.replica_count, p2.replica_count);
        }
    }

    #[test]
    fn versions_assigned_in_commit_order() {
        let (state, ns) = replay(&sample_log());
        // floor 3, seed/ -> 4, user/ -> 5, order/ -> 6, user/(re-put) -> 7.
        assert_eq!(state.policies["seed/"].version, 4);
        assert_eq!(state.policies["order/"].version, 6);
        assert_eq!(state.policies["user/"].version, 7);
        assert_eq!(state.version_counter, 7);
        assert_eq!(ns.get_placement_policy("user/").unwrap().version.0, 7);
        assert!(ns.get_placement_policy("seed/").is_some());
    }

    #[test]
    fn bootstrap_is_reset_and_import() {
        let mut state = ControlPlaneState::default();
        let mut ns = SystemNamespace::new();
        // Pre-existing local-only policy that the leader does not have.
        ns.set_placement_policy(spec("stale/", 1).to_policy(PolicyVersion(1)))
            .unwrap();
        // Local auto-generated definition must survive; manual must be replaced.
        ns.set_authority_definition(AuthorityDefinition {
            key_range: crate::types::KeyRange {
                prefix: "manual/".into(),
            },
            authority_nodes: vec![crate::types::NodeId("m1".into())],
            auto_generated: false,
        });

        let boot = entry(
            1,
            ControlPlaneCommand::Bootstrap {
                version_floor: 10,
                policies: vec![spec("seed/", 2)],
                authorities: vec![auth_spec("", &["auth-1"])],
            },
        );
        apply(&boot, &mut state, &mut ns);

        assert!(state.bootstrapped);
        assert_eq!(state.version_counter, 11);
        assert!(
            ns.get_placement_policy("stale/").is_none(),
            "surplus local policies are replaced by the leader's core"
        );
        assert!(ns.get_placement_policy("seed/").is_some());
        assert!(
            ns.get_authority_definition("manual/").is_none(),
            "manual authorities not in the bootstrap set are replaced"
        );
        assert!(ns.get_authority_definition("").is_some());

        // Second Bootstrap (leader re-proposal) is a no-op.
        let before = state.clone();
        let ns_version_before = ns.version().0;
        let boot2 = entry(
            2,
            ControlPlaneCommand::Bootstrap {
                version_floor: 99,
                policies: vec![spec("other/", 1)],
                authorities: vec![],
            },
        );
        apply(&boot2, &mut state, &mut ns);
        assert_eq!(state, before, "duplicate Bootstrap must be idempotent");
        assert_eq!(ns.version().0, ns_version_before);
    }

    #[test]
    fn remove_missing_policy_is_noop() {
        let mut state = ControlPlaneState::default();
        let mut ns = SystemNamespace::new();
        let version_before = ns.version().0;
        let outcome = apply(
            &entry(
                1,
                ControlPlaneCommand::RemovePolicy {
                    prefix: "missing/".into(),
                },
            ),
            &mut state,
            &mut ns,
        );
        assert!(matches!(outcome, ApplyOutcome::PolicyRemoved(None)));
        assert_eq!(ns.version().0, version_before, "namespace untouched");
    }

    #[test]
    fn zero_replica_policy_is_deterministic_noop() {
        let mut state = ControlPlaneState::default();
        let mut ns = SystemNamespace::new();
        let outcome = apply(
            &entry(1, ControlPlaneCommand::PutPolicy(spec("bad/", 0))),
            &mut state,
            &mut ns,
        );
        assert!(matches!(outcome, ApplyOutcome::Noop));
        assert_eq!(state.version_counter, 0);
        assert!(ns.get_placement_policy("bad/").is_none());
    }

    #[test]
    fn install_round_trips_replicated_core() {
        let (state, ns_src) = replay(&sample_log());
        let mut ns = SystemNamespace::new();
        install(&state, &mut ns);
        for (prefix, vp) in &state.policies {
            let p = ns.get_placement_policy(prefix).unwrap();
            assert_eq!(p.version.0, vp.version);
            assert_eq!(
                p.replica_count,
                ns_src.get_placement_policy(prefix).unwrap().replica_count
            );
        }
        for prefix in state.authorities.keys() {
            assert!(ns.get_authority_definition(prefix).is_some());
        }
    }
}
