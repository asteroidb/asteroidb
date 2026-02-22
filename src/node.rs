use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::types::{NodeId, NodeMode, Tag};

/// A node in the AsteroidDB cluster.
///
/// Nodes are tagged with arbitrary labels for placement policy matching.
/// There is no forced hierarchy (e.g., Region > DC > Node); tags are flat
/// and user-defined (FR-006).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub mode: NodeMode,
    pub tags: HashSet<Tag>,
}

impl Node {
    /// Creates a new node with the given id and mode, and no tags.
    pub fn new(id: NodeId, mode: NodeMode) -> Self {
        Self {
            id,
            mode,
            tags: HashSet::new(),
        }
    }

    /// Adds a tag to this node.
    pub fn add_tag(&mut self, tag: Tag) {
        self.tags.insert(tag);
    }

    /// Removes a tag from this node.
    pub fn remove_tag(&mut self, tag: &Tag) {
        self.tags.remove(tag);
    }

    /// Returns `true` if this node has the given tag.
    pub fn has_tag(&self, tag: &Tag) -> bool {
        self.tags.contains(tag)
    }

    /// Returns `true` if this node has **all** of the given tags.
    pub fn has_all_tags(&self, tags: &HashSet<Tag>) -> bool {
        tags.iter().all(|t| self.tags.contains(t))
    }

    /// Returns `true` if this node has **any** of the given tags.
    pub fn has_any_tag(&self, tags: &HashSet<Tag>) -> bool {
        tags.iter().any(|t| self.tags.contains(t))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(s: &str) -> Tag {
        Tag(s.into())
    }

    fn node_id(s: &str) -> NodeId {
        NodeId(s.into())
    }

    #[test]
    fn new_node_has_no_tags() {
        let n = Node::new(node_id("n1"), NodeMode::Store);
        assert!(n.tags.is_empty());
        assert_eq!(n.id, node_id("n1"));
        assert_eq!(n.mode, NodeMode::Store);
    }

    #[test]
    fn add_and_has_tag() {
        let mut n = Node::new(node_id("n1"), NodeMode::Store);
        n.add_tag(tag("dc:tokyo"));
        assert!(n.has_tag(&tag("dc:tokyo")));
        assert!(!n.has_tag(&tag("dc:osaka")));
    }

    #[test]
    fn remove_tag() {
        let mut n = Node::new(node_id("n1"), NodeMode::Store);
        n.add_tag(tag("dc:tokyo"));
        n.remove_tag(&tag("dc:tokyo"));
        assert!(!n.has_tag(&tag("dc:tokyo")));
    }

    #[test]
    fn remove_nonexistent_tag_is_noop() {
        let mut n = Node::new(node_id("n1"), NodeMode::Store);
        n.remove_tag(&tag("dc:tokyo"));
        assert!(n.tags.is_empty());
    }

    #[test]
    fn has_all_tags() {
        let mut n = Node::new(node_id("n1"), NodeMode::Store);
        n.add_tag(tag("dc:tokyo"));
        n.add_tag(tag("rack:a1"));
        n.add_tag(tag("tier:hot"));

        let required: HashSet<Tag> = [tag("dc:tokyo"), tag("rack:a1")].into();
        assert!(n.has_all_tags(&required));

        let missing: HashSet<Tag> = [tag("dc:tokyo"), tag("dc:osaka")].into();
        assert!(!n.has_all_tags(&missing));
    }

    #[test]
    fn has_all_tags_empty_set() {
        let n = Node::new(node_id("n1"), NodeMode::Store);
        let empty: HashSet<Tag> = HashSet::new();
        assert!(n.has_all_tags(&empty));
    }

    #[test]
    fn has_any_tag() {
        let mut n = Node::new(node_id("n1"), NodeMode::Store);
        n.add_tag(tag("dc:tokyo"));

        let some: HashSet<Tag> = [tag("dc:tokyo"), tag("dc:osaka")].into();
        assert!(n.has_any_tag(&some));

        let none: HashSet<Tag> = [tag("dc:london"), tag("dc:osaka")].into();
        assert!(!n.has_any_tag(&none));
    }

    #[test]
    fn has_any_tag_empty_set() {
        let n = Node::new(node_id("n1"), NodeMode::Store);
        let empty: HashSet<Tag> = HashSet::new();
        assert!(!n.has_any_tag(&empty));
    }

    #[test]
    fn duplicate_tags_are_deduplicated() {
        let mut n = Node::new(node_id("n1"), NodeMode::Store);
        n.add_tag(tag("dc:tokyo"));
        n.add_tag(tag("dc:tokyo"));
        assert_eq!(n.tags.len(), 1);
    }

    #[test]
    fn serde_round_trip() {
        let mut n = Node::new(node_id("sat-7"), NodeMode::Both);
        n.add_tag(tag("orbit:leo"));
        n.add_tag(tag("constellation:a"));

        let json = serde_json::to_string(&n).unwrap();
        let back: Node = serde_json::from_str(&json).unwrap();

        assert_eq!(back.id, node_id("sat-7"));
        assert_eq!(back.mode, NodeMode::Both);
        assert!(back.has_tag(&tag("orbit:leo")));
        assert!(back.has_tag(&tag("constellation:a")));
    }
}
