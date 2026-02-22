use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::placement::PlacementPolicy;
use crate::types::{KeyRange, NodeId, PolicyVersion};

/// Defines which nodes are authorities for a key range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorityDefinition {
    pub key_range: KeyRange,
    pub authority_nodes: Vec<NodeId>,
}

/// The system namespace stores all control-plane configuration.
/// Updates require control-plane Authority consensus (FR-009).
#[derive(Debug, Clone)]
pub struct SystemNamespace {
    version: PolicyVersion,
    placement_policies: HashMap<String, PlacementPolicy>,
    authority_definitions: HashMap<String, AuthorityDefinition>,
    /// History of policy versions for observability (NFR-004).
    version_history: Vec<PolicyVersion>,
}

impl SystemNamespace {
    /// Creates a new empty system namespace at version 1.
    pub fn new() -> Self {
        let initial_version = PolicyVersion(1);
        Self {
            version: initial_version,
            placement_policies: HashMap::new(),
            authority_definitions: HashMap::new(),
            version_history: vec![initial_version],
        }
    }

    /// Returns the current policy version.
    pub fn version(&self) -> &PolicyVersion {
        &self.version
    }

    /// Adds or updates a placement policy, keyed by its key range prefix.
    /// Increments the namespace version.
    pub fn set_placement_policy(&mut self, policy: PlacementPolicy) {
        let prefix = policy.key_range.prefix.clone();
        self.placement_policies.insert(prefix, policy);
        self.bump_version();
    }

    /// Returns the placement policy for the given prefix, if any.
    pub fn get_placement_policy(&self, prefix: &str) -> Option<&PlacementPolicy> {
        self.placement_policies.get(prefix)
    }

    /// Removes and returns the placement policy for the given prefix.
    /// Increments the namespace version if a policy was removed.
    pub fn remove_placement_policy(&mut self, prefix: &str) -> Option<PlacementPolicy> {
        let removed = self.placement_policies.remove(prefix);
        if removed.is_some() {
            self.bump_version();
        }
        removed
    }

    /// Returns all placement policies.
    pub fn all_placement_policies(&self) -> Vec<&PlacementPolicy> {
        self.placement_policies.values().collect()
    }

    /// Defines the authority set for a key range, keyed by its prefix.
    /// Increments the namespace version.
    pub fn set_authority_definition(&mut self, def: AuthorityDefinition) {
        let prefix = def.key_range.prefix.clone();
        self.authority_definitions.insert(prefix, def);
        self.bump_version();
    }

    /// Returns the authority definition for the given prefix, if any.
    pub fn get_authority_definition(&self, prefix: &str) -> Option<&AuthorityDefinition> {
        self.authority_definitions.get(prefix)
    }

    /// Returns all authority definitions.
    pub fn all_authority_definitions(&self) -> Vec<&AuthorityDefinition> {
        self.authority_definitions.values().collect()
    }

    /// Returns the version history for observability (NFR-004).
    pub fn version_history(&self) -> &[PolicyVersion] {
        &self.version_history
    }

    /// Finds the authority definition whose key range prefix matches the given key.
    /// Uses longest-prefix match.
    pub fn get_authorities_for_key(&self, key: &str) -> Option<&AuthorityDefinition> {
        self.authority_definitions
            .iter()
            .filter(|(prefix, _)| key.starts_with(prefix.as_str()))
            .max_by_key(|(prefix, _)| prefix.len())
            .map(|(_, def)| def)
    }

    fn bump_version(&mut self) {
        self.version = PolicyVersion(self.version.0 + 1);
        self.version_history.push(self.version);
    }
}

impl Default for SystemNamespace {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_range(prefix: &str) -> KeyRange {
        KeyRange {
            prefix: prefix.into(),
        }
    }

    fn make_policy(prefix: &str) -> PlacementPolicy {
        PlacementPolicy::new(PolicyVersion(1), key_range(prefix), 3)
    }

    fn make_authority_def(prefix: &str, nodes: &[&str]) -> AuthorityDefinition {
        AuthorityDefinition {
            key_range: key_range(prefix),
            authority_nodes: nodes.iter().map(|s| NodeId((*s).into())).collect(),
        }
    }

    // --- SystemNamespace creation ---

    #[test]
    fn new_starts_at_version_1() {
        let ns = SystemNamespace::new();
        assert_eq!(*ns.version(), PolicyVersion(1));
    }

    #[test]
    fn new_has_initial_version_history() {
        let ns = SystemNamespace::new();
        assert_eq!(ns.version_history(), &[PolicyVersion(1)]);
    }

    #[test]
    fn new_has_no_policies() {
        let ns = SystemNamespace::new();
        assert!(ns.all_placement_policies().is_empty());
    }

    #[test]
    fn new_has_no_authority_definitions() {
        let ns = SystemNamespace::new();
        assert!(ns.all_authority_definitions().is_empty());
    }

    // --- Placement policy CRUD ---

    #[test]
    fn set_and_get_placement_policy() {
        let mut ns = SystemNamespace::new();
        let policy = make_policy("user/");
        ns.set_placement_policy(policy);

        let got = ns.get_placement_policy("user/").unwrap();
        assert_eq!(got.key_range.prefix, "user/");
        assert_eq!(got.replica_count, 3);
    }

    #[test]
    fn set_placement_policy_overwrites() {
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_policy("user/"));
        let updated = PlacementPolicy::new(PolicyVersion(2), key_range("user/"), 5);
        ns.set_placement_policy(updated);

        let got = ns.get_placement_policy("user/").unwrap();
        assert_eq!(got.replica_count, 5);
    }

    #[test]
    fn get_nonexistent_policy_returns_none() {
        let ns = SystemNamespace::new();
        assert!(ns.get_placement_policy("missing/").is_none());
    }

    #[test]
    fn remove_placement_policy() {
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_policy("user/"));

        let removed = ns.remove_placement_policy("user/");
        assert!(removed.is_some());
        assert!(ns.get_placement_policy("user/").is_none());
    }

    #[test]
    fn remove_nonexistent_policy_returns_none() {
        let mut ns = SystemNamespace::new();
        assert!(ns.remove_placement_policy("missing/").is_none());
    }

    #[test]
    fn all_placement_policies_lists_all() {
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_policy("user/"));
        ns.set_placement_policy(make_policy("order/"));

        let all = ns.all_placement_policies();
        assert_eq!(all.len(), 2);
    }

    // --- Version tracking ---

    #[test]
    fn version_increments_on_set_policy() {
        let mut ns = SystemNamespace::new();
        assert_eq!(*ns.version(), PolicyVersion(1));

        ns.set_placement_policy(make_policy("user/"));
        assert_eq!(*ns.version(), PolicyVersion(2));

        ns.set_placement_policy(make_policy("order/"));
        assert_eq!(*ns.version(), PolicyVersion(3));
    }

    #[test]
    fn version_increments_on_remove_policy() {
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_policy("user/"));
        let v_before = ns.version().0;
        ns.remove_placement_policy("user/");
        assert_eq!(ns.version().0, v_before + 1);
    }

    #[test]
    fn version_does_not_increment_on_noop_remove() {
        let mut ns = SystemNamespace::new();
        let v_before = ns.version().0;
        ns.remove_placement_policy("missing/");
        assert_eq!(ns.version().0, v_before);
    }

    #[test]
    fn version_increments_on_set_authority() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(make_authority_def("user/", &["n1", "n2", "n3"]));
        assert_eq!(*ns.version(), PolicyVersion(2));
    }

    #[test]
    fn version_history_tracks_all_changes() {
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_policy("user/"));
        ns.set_authority_definition(make_authority_def("user/", &["n1"]));
        ns.remove_placement_policy("user/");

        assert_eq!(
            ns.version_history(),
            &[
                PolicyVersion(1),
                PolicyVersion(2),
                PolicyVersion(3),
                PolicyVersion(4),
            ]
        );
    }

    // --- Authority definition CRUD ---

    #[test]
    fn set_and_get_authority_definition() {
        let mut ns = SystemNamespace::new();
        let def = make_authority_def("user/", &["n1", "n2", "n3"]);
        ns.set_authority_definition(def);

        let got = ns.get_authority_definition("user/").unwrap();
        assert_eq!(got.key_range.prefix, "user/");
        assert_eq!(got.authority_nodes.len(), 3);
    }

    #[test]
    fn get_nonexistent_authority_returns_none() {
        let ns = SystemNamespace::new();
        assert!(ns.get_authority_definition("missing/").is_none());
    }

    #[test]
    fn all_authority_definitions_lists_all() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(make_authority_def("user/", &["n1"]));
        ns.set_authority_definition(make_authority_def("order/", &["n2"]));

        let all = ns.all_authority_definitions();
        assert_eq!(all.len(), 2);
    }

    // --- get_authorities_for_key ---

    #[test]
    fn get_authorities_for_key_exact_prefix() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(make_authority_def("user/", &["n1", "n2", "n3"]));

        let result = ns.get_authorities_for_key("user/alice");
        assert!(result.is_some());
        assert_eq!(result.unwrap().key_range.prefix, "user/");
    }

    #[test]
    fn get_authorities_for_key_no_match() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(make_authority_def("user/", &["n1"]));

        assert!(ns.get_authorities_for_key("order/123").is_none());
    }

    #[test]
    fn get_authorities_for_key_longest_prefix_match() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(make_authority_def("user/", &["n1"]));
        ns.set_authority_definition(make_authority_def("user/vip/", &["n2", "n3"]));

        let result = ns.get_authorities_for_key("user/vip/alice");
        assert!(result.is_some());
        assert_eq!(result.unwrap().key_range.prefix, "user/vip/");
        assert_eq!(result.unwrap().authority_nodes.len(), 2);
    }

    #[test]
    fn get_authorities_for_key_falls_back_to_shorter_prefix() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(make_authority_def("user/", &["n1"]));
        ns.set_authority_definition(make_authority_def("user/vip/", &["n2"]));

        let result = ns.get_authorities_for_key("user/regular/bob");
        assert!(result.is_some());
        assert_eq!(result.unwrap().key_range.prefix, "user/");
    }

    // --- Default trait ---

    #[test]
    fn default_is_same_as_new() {
        let ns = SystemNamespace::default();
        assert_eq!(*ns.version(), PolicyVersion(1));
        assert!(ns.all_placement_policies().is_empty());
        assert!(ns.all_authority_definitions().is_empty());
    }

    // --- Serde for AuthorityDefinition ---

    #[test]
    fn serde_authority_definition_round_trip() {
        let def = make_authority_def("user/", &["n1", "n2", "n3"]);
        let json = serde_json::to_string(&def).unwrap();
        let back: AuthorityDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(back.key_range.prefix, "user/");
        assert_eq!(back.authority_nodes.len(), 3);
    }
}
