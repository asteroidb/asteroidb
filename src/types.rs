use serde::{Deserialize, Serialize};

/// Unique identifier for a node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

/// A tag used for placement policies.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Tag(pub String);

/// Key range for prefix-based key space partitioning.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyRange {
    pub prefix: String,
}

/// Operating mode of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeMode {
    Store,
    Subscribe,
    Both,
}

/// Monotonically increasing version for placement policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PolicyVersion(pub u64);

/// Status of a certified write operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CertificationStatus {
    Pending,
    Certified,
    Rejected,
    Timeout,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_equality() {
        let a = NodeId("node-1".into());
        let b = NodeId("node-1".into());
        assert_eq!(a, b);
    }

    #[test]
    fn tag_clone_and_hash() {
        use std::collections::HashSet;
        let t = Tag("dc:tokyo".into());
        let mut set = HashSet::new();
        set.insert(t.clone());
        assert!(set.contains(&t));
    }

    #[test]
    fn key_range_prefix() {
        let kr = KeyRange {
            prefix: "user/".into(),
        };
        assert_eq!(kr.prefix, "user/");
    }

    #[test]
    fn node_mode_variants() {
        assert_ne!(NodeMode::Store, NodeMode::Subscribe);
        assert_ne!(NodeMode::Subscribe, NodeMode::Both);
        assert_ne!(NodeMode::Store, NodeMode::Both);
    }

    #[test]
    fn policy_version_ordering() {
        let v1 = PolicyVersion(1);
        let v2 = PolicyVersion(2);
        assert!(v1 < v2);
    }

    #[test]
    fn certification_status_variants() {
        let statuses = [
            CertificationStatus::Pending,
            CertificationStatus::Certified,
            CertificationStatus::Rejected,
            CertificationStatus::Timeout,
        ];
        for (i, a) in statuses.iter().enumerate() {
            for (j, b) in statuses.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn serde_round_trip_node_id() {
        let id = NodeId("sat-7".into());
        let json = serde_json::to_string(&id).unwrap();
        let back: NodeId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn serde_round_trip_certification_status() {
        let status = CertificationStatus::Certified;
        let json = serde_json::to_string(&status).unwrap();
        let back: CertificationStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(status, back);
    }

    #[test]
    fn serde_round_trip_node_mode() {
        let mode = NodeMode::Both;
        let json = serde_json::to_string(&mode).unwrap();
        let back: NodeMode = serde_json::from_str(&json).unwrap();
        assert_eq!(mode, back);
    }
}
