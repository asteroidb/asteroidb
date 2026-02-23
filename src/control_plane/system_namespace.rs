use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::node::Node;
use crate::placement::PlacementPolicy;
use crate::types::{KeyRange, NodeId, PolicyVersion};

/// Error type for system namespace persistence operations.
#[derive(Debug, Error)]
pub enum PersistError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Defines which nodes are authorities for a key range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorityDefinition {
    pub key_range: KeyRange,
    pub authority_nodes: Vec<NodeId>,
}

/// The system namespace stores all control-plane configuration.
/// Updates require control-plane Authority consensus (FR-009).
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Recalculate authority definitions from placement policies and available nodes.
    ///
    /// For each placement policy with `certified == true`, selects candidate
    /// nodes via the policy's tag-based criteria and updates the authority
    /// definition for that key range. Authority definitions for non-certified
    /// policies are left unchanged.
    ///
    /// Only bumps the namespace version if at least one authority definition
    /// was actually created or modified.
    ///
    /// Returns the number of authority definitions that changed.
    pub fn recalculate_authorities(&mut self, nodes: &[Node]) -> usize {
        let policies: Vec<PlacementPolicy> = self.placement_policies.values().cloned().collect();
        let mut changed = 0;

        for policy in &policies {
            if !policy.certified {
                continue;
            }

            let mut candidates: Vec<NodeId> = policy
                .select_nodes(nodes)
                .iter()
                .map(|n| n.id.clone())
                .collect();
            candidates.sort_by(|a, b| a.0.cmp(&b.0));

            let prefix = &policy.key_range.prefix;

            let needs_update = match self.authority_definitions.get(prefix) {
                Some(existing) => {
                    let mut existing_sorted = existing.authority_nodes.clone();
                    existing_sorted.sort_by(|a, b| a.0.cmp(&b.0));
                    existing_sorted != candidates
                }
                None => !candidates.is_empty(),
            };

            if needs_update {
                self.authority_definitions.insert(
                    prefix.clone(),
                    AuthorityDefinition {
                        key_range: policy.key_range.clone(),
                        authority_nodes: candidates,
                    },
                );
                changed += 1;
            }
        }

        if changed > 0 {
            self.bump_version();
        }

        changed
    }

    /// Saves the system namespace to a JSON file at the given path.
    ///
    /// Writes to a temporary file first, then atomically renames to ensure
    /// crash safety. If the parent directory does not exist, it is created.
    pub fn save(&self, path: &Path) -> Result<(), PersistError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("tmp");
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&tmp, json)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Loads a system namespace from a JSON file.
    ///
    /// Returns `Ok(None)` if the file does not exist, allowing callers to
    /// fall back to a fresh namespace via [`SystemNamespace::new`].
    pub fn load(path: &Path) -> Result<Option<Self>, PersistError> {
        if !path.exists() {
            return Ok(None);
        }
        let data = std::fs::read_to_string(path)?;
        let ns: Self = serde_json::from_str(&data)?;
        Ok(Some(ns))
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

    // --- Serde for SystemNamespace ---

    #[test]
    fn serde_system_namespace_round_trip() {
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_policy("user/"));
        ns.set_authority_definition(make_authority_def("user/", &["n1", "n2", "n3"]));

        let json = serde_json::to_string(&ns).unwrap();
        let back: SystemNamespace = serde_json::from_str(&json).unwrap();

        assert_eq!(*back.version(), *ns.version());
        assert!(back.get_placement_policy("user/").is_some());
        assert!(back.get_authority_definition("user/").is_some());
        assert_eq!(back.version_history(), ns.version_history());
    }

    // --- Persistence (save / load) ---

    #[test]
    fn save_and_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ns.json");

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_policy("user/"));
        ns.set_placement_policy(make_policy("order/"));
        ns.set_authority_definition(make_authority_def("user/", &["n1", "n2", "n3"]));

        ns.save(&path).unwrap();
        let loaded = SystemNamespace::load(&path).unwrap().unwrap();

        assert_eq!(*loaded.version(), *ns.version());
        assert_eq!(loaded.version_history(), ns.version_history());
        assert_eq!(loaded.all_placement_policies().len(), 2);
        assert!(loaded.get_placement_policy("user/").is_some());
        assert!(loaded.get_placement_policy("order/").is_some());
        assert!(loaded.get_authority_definition("user/").is_some());
        assert_eq!(
            loaded
                .get_authority_definition("user/")
                .unwrap()
                .authority_nodes
                .len(),
            3
        );
    }

    #[test]
    fn load_nonexistent_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does_not_exist.json");
        let result = SystemNamespace::load(&path).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn save_creates_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub").join("dir").join("ns.json");

        let ns = SystemNamespace::new();
        ns.save(&path).unwrap();

        assert!(path.exists());
    }

    #[test]
    fn version_continues_after_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ns.json");

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_policy("user/"));
        // version is now 2
        ns.save(&path).unwrap();

        let mut loaded = SystemNamespace::load(&path).unwrap().unwrap();
        assert_eq!(*loaded.version(), PolicyVersion(2));

        loaded.set_placement_policy(make_policy("order/"));
        assert_eq!(*loaded.version(), PolicyVersion(3));
        assert_eq!(
            loaded.version_history(),
            &[PolicyVersion(1), PolicyVersion(2), PolicyVersion(3)]
        );
    }

    #[test]
    fn save_overwrites_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ns.json");

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_policy("user/"));
        ns.save(&path).unwrap();

        ns.set_placement_policy(make_policy("order/"));
        ns.save(&path).unwrap();

        let loaded = SystemNamespace::load(&path).unwrap().unwrap();
        assert_eq!(loaded.all_placement_policies().len(), 2);
        assert_eq!(*loaded.version(), *ns.version());
    }

    #[test]
    fn load_corrupt_json_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        std::fs::write(&path, "not valid json {{{").unwrap();

        let result = SystemNamespace::load(&path);
        assert!(result.is_err());
    }

    // --- recalculate_authorities ---

    fn make_certified_policy(prefix: &str, tags: &[&str]) -> PlacementPolicy {
        use crate::types::Tag;
        use std::collections::HashSet;
        let tag_set: HashSet<Tag> = tags.iter().map(|t| Tag((*t).into())).collect();
        PlacementPolicy::new(PolicyVersion(1), key_range(prefix), 3)
            .with_certified(true)
            .with_required_tags(tag_set)
    }

    fn make_node(id: &str, mode: crate::types::NodeMode, tags: &[&str]) -> crate::node::Node {
        use crate::types::Tag;
        let mut n = crate::node::Node::new(NodeId(id.into()), mode);
        for t in tags {
            n.add_tag(Tag((*t).into()));
        }
        n
    }

    #[test]
    fn recalculate_authorities_creates_from_certified_policy() {
        use crate::types::NodeMode;

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_certified_policy("user/", &["dc:tokyo"]));

        let nodes = vec![
            make_node("n1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:tokyo"]),
            make_node("n3", NodeMode::Store, &["dc:osaka"]),
        ];

        let changed = ns.recalculate_authorities(&nodes);
        assert_eq!(changed, 1);

        let def = ns.get_authority_definition("user/").unwrap();
        assert_eq!(def.authority_nodes.len(), 2);
        assert!(def.authority_nodes.contains(&NodeId("n1".into())));
        assert!(def.authority_nodes.contains(&NodeId("n2".into())));
    }

    #[test]
    fn recalculate_authorities_ignores_non_certified_policy() {
        use crate::types::NodeMode;

        let mut ns = SystemNamespace::new();
        let policy = PlacementPolicy::new(PolicyVersion(1), key_range("data/"), 3);
        // certified is false by default
        ns.set_placement_policy(policy);

        let nodes = vec![
            make_node("n1", NodeMode::Store, &[]),
            make_node("n2", NodeMode::Store, &[]),
        ];

        let changed = ns.recalculate_authorities(&nodes);
        assert_eq!(changed, 0);
        assert!(ns.get_authority_definition("data/").is_none());
    }

    #[test]
    fn recalculate_authorities_updates_on_membership_change() {
        use crate::types::NodeMode;

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_certified_policy("user/", &["dc:tokyo"]));

        // Initial: 2 matching nodes
        let nodes_v1 = vec![
            make_node("n1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:tokyo"]),
        ];
        ns.recalculate_authorities(&nodes_v1);
        assert_eq!(
            ns.get_authority_definition("user/")
                .unwrap()
                .authority_nodes
                .len(),
            2
        );

        // After join: 3 matching nodes
        let nodes_v2 = vec![
            make_node("n1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:tokyo"]),
            make_node("n3", NodeMode::Store, &["dc:tokyo"]),
        ];
        let changed = ns.recalculate_authorities(&nodes_v2);
        assert_eq!(changed, 1);
        assert_eq!(
            ns.get_authority_definition("user/")
                .unwrap()
                .authority_nodes
                .len(),
            3
        );
    }

    #[test]
    fn recalculate_authorities_updates_on_node_leave() {
        use crate::types::NodeMode;

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_certified_policy("user/", &["dc:tokyo"]));

        let nodes_v1 = vec![
            make_node("n1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:tokyo"]),
            make_node("n3", NodeMode::Store, &["dc:tokyo"]),
        ];
        ns.recalculate_authorities(&nodes_v1);
        assert_eq!(
            ns.get_authority_definition("user/")
                .unwrap()
                .authority_nodes
                .len(),
            3
        );

        // After leave: n3 is gone
        let nodes_v2 = vec![
            make_node("n1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:tokyo"]),
        ];
        let changed = ns.recalculate_authorities(&nodes_v2);
        assert_eq!(changed, 1);
        assert_eq!(
            ns.get_authority_definition("user/")
                .unwrap()
                .authority_nodes
                .len(),
            2
        );
        assert!(
            !ns.get_authority_definition("user/")
                .unwrap()
                .authority_nodes
                .contains(&NodeId("n3".into()))
        );
    }

    #[test]
    fn recalculate_authorities_no_change_returns_zero() {
        use crate::types::NodeMode;

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_certified_policy("user/", &["dc:tokyo"]));

        let nodes = vec![
            make_node("n1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:tokyo"]),
        ];

        ns.recalculate_authorities(&nodes);
        let version_before = *ns.version();

        // Same nodes → no change
        let changed = ns.recalculate_authorities(&nodes);
        assert_eq!(changed, 0);
        assert_eq!(*ns.version(), version_before);
    }

    #[test]
    fn recalculate_authorities_excludes_subscribe_mode() {
        use crate::types::NodeMode;

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_certified_policy("user/", &[]));

        let nodes = vec![
            make_node("n1", NodeMode::Store, &[]),
            make_node("n2", NodeMode::Subscribe, &[]),
            make_node("n3", NodeMode::Both, &[]),
        ];

        ns.recalculate_authorities(&nodes);
        let def = ns.get_authority_definition("user/").unwrap();
        // Subscribe-only nodes should not be authority candidates
        assert_eq!(def.authority_nodes.len(), 2);
        assert!(def.authority_nodes.contains(&NodeId("n1".into())));
        assert!(def.authority_nodes.contains(&NodeId("n3".into())));
    }

    #[test]
    fn recalculate_authorities_respects_forbidden_tags() {
        use crate::types::NodeMode;
        use crate::types::Tag;

        let mut ns = SystemNamespace::new();
        let policy = PlacementPolicy::new(PolicyVersion(1), key_range("data/"), 3)
            .with_certified(true)
            .with_required_tags([Tag("dc:tokyo".into())].into())
            .with_forbidden_tags([Tag("decommissioned".into())].into());
        ns.set_placement_policy(policy);

        let nodes = vec![
            make_node("n1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:tokyo", "decommissioned"]),
            make_node("n3", NodeMode::Store, &["dc:tokyo"]),
        ];

        ns.recalculate_authorities(&nodes);
        let def = ns.get_authority_definition("data/").unwrap();
        assert_eq!(def.authority_nodes.len(), 2);
        assert!(!def.authority_nodes.contains(&NodeId("n2".into())));
    }

    #[test]
    fn recalculate_authorities_multiple_policies() {
        use crate::types::NodeMode;

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_certified_policy("user/", &["dc:tokyo"]));
        ns.set_placement_policy(make_certified_policy("order/", &["dc:osaka"]));

        let nodes = vec![
            make_node("n1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:osaka"]),
            make_node("n3", NodeMode::Both, &["dc:tokyo", "dc:osaka"]),
        ];

        let changed = ns.recalculate_authorities(&nodes);
        assert_eq!(changed, 2);

        let user_def = ns.get_authority_definition("user/").unwrap();
        assert_eq!(user_def.authority_nodes.len(), 2); // n1, n3
        assert!(user_def.authority_nodes.contains(&NodeId("n1".into())));
        assert!(user_def.authority_nodes.contains(&NodeId("n3".into())));

        let order_def = ns.get_authority_definition("order/").unwrap();
        assert_eq!(order_def.authority_nodes.len(), 2); // n2, n3
        assert!(order_def.authority_nodes.contains(&NodeId("n2".into())));
        assert!(order_def.authority_nodes.contains(&NodeId("n3".into())));
    }

    #[test]
    fn recalculate_authorities_bumps_version_once() {
        use crate::types::NodeMode;

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(make_certified_policy("user/", &[]));
        ns.set_placement_policy(make_certified_policy("order/", &[]));
        let version_before = *ns.version();

        let nodes = vec![
            make_node("n1", NodeMode::Store, &[]),
            make_node("n2", NodeMode::Store, &[]),
        ];

        let changed = ns.recalculate_authorities(&nodes);
        assert_eq!(changed, 2);
        // Version should bump only once (not once per changed definition).
        assert_eq!(ns.version().0, version_before.0 + 1);
    }
}
