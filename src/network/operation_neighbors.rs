//! Trusted, opt-in static destinations for operation packet delivery.
//!
//! The manifest is a deployment input, not a discovered membership view.
//! Each accepted packet can copy these destinations into its durable outbox.
//! All senders must receive a consistent topology if the required-neighbor
//! property is expected to hold across the deployment.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::types::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationNeighbor {
    pub node_id: NodeId,
    /// Host and port without a URL scheme or path, as expected by
    /// `OperationTransportClient::send`.
    pub addr: String,
    /// Keys this neighbor is permitted to receive. An empty string prefix
    /// explicitly permits all keys.
    pub key_prefixes: Vec<String>,
    /// Namespace-specific permissions. Empty legacy prefixes with nonempty
    /// scoped prefixes restrict this neighbor to the named namespaces.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scoped_key_prefixes: Vec<OperationOwnedRange>,
}

impl OperationNeighbor {
    pub fn permits(&self, namespace_id: &str, key: &str) -> bool {
        self.key_prefixes
            .iter()
            .any(|prefix| key.starts_with(prefix))
            || self.scoped_key_prefixes.iter().any(|range| {
                range.namespace_id == namespace_id && key.starts_with(&range.key_prefix)
            })
    }
}

/// Immutable neighbors ordered by node ID for deterministic outbox enrollment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationNeighborManifest {
    self_id: NodeId,
    neighbors: BTreeMap<NodeId, OperationNeighbor>,
}

/// Deployment input describing keys this node applies to its local read
/// projection. Custody and forwarding remain independent of ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationOwnedPrefixes {
    prefixes: Vec<String>,
    scoped: Vec<OperationOwnedRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationOwnedRange {
    pub namespace_id: String,
    pub key_prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ConfiguredOwnedRange {
    Legacy(String),
    Scoped(OperationOwnedRange),
}

impl OperationOwnedPrefixes {
    pub fn from_json(json: &str) -> Result<Self, String> {
        let ranges: Vec<ConfiguredOwnedRange> = serde_json::from_str(json)
            .map_err(|error| format!("invalid owned-prefix JSON: {error}"))?;
        let mut prefixes = Vec::new();
        let mut scoped = Vec::new();
        let mut seen_legacy = BTreeSet::new();
        let mut seen_scoped = BTreeSet::new();
        for range in ranges {
            match range {
                ConfiguredOwnedRange::Legacy(prefix) => {
                    if !seen_legacy.insert(prefix.clone()) {
                        return Err(format!("duplicate owned prefix: {prefix}"));
                    }
                    prefixes.push(prefix);
                }
                ConfiguredOwnedRange::Scoped(range) => {
                    if range.namespace_id.is_empty()
                        || range.namespace_id.trim() != range.namespace_id
                        || !seen_scoped
                            .insert((range.namespace_id.clone(), range.key_prefix.clone()))
                    {
                        return Err("invalid or duplicate namespace owned range".into());
                    }
                    scoped.push(range);
                }
            }
        }
        Ok(Self { prefixes, scoped })
    }

    /// Check only legacy prefixes, which apply to every namespace.
    pub fn owns(&self, key: &str) -> bool {
        self.prefixes.iter().any(|prefix| key.starts_with(prefix))
    }

    pub fn owns_in_namespace(&self, namespace_id: &str, key: &str) -> bool {
        self.owns(key)
            || self.scoped.iter().any(|range| {
                range.namespace_id == namespace_id && key.starts_with(&range.key_prefix)
            })
    }

    /// Canonical commitment to the static application assignment. JSON row
    /// order does not change the trust identity.
    pub fn digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(if self.scoped.is_empty() {
            b"asteroidb/operation-owned-prefixes/v1".as_slice()
        } else {
            b"asteroidb/operation-owned-ranges/v2".as_slice()
        });
        hash.update((self.prefixes.len() as u64).to_be_bytes());
        let mut prefixes = self.prefixes.iter().collect::<Vec<_>>();
        prefixes.sort();
        for prefix in prefixes {
            hash.update((prefix.len() as u64).to_be_bytes());
            hash.update(prefix.as_bytes());
        }
        if !self.scoped.is_empty() {
            hash.update((self.scoped.len() as u64).to_be_bytes());
            let mut scoped = self.scoped.iter().collect::<Vec<_>>();
            scoped.sort_by(|a, b| {
                (&a.namespace_id, &a.key_prefix).cmp(&(&b.namespace_id, &b.key_prefix))
            });
            for range in scoped {
                hash.update((range.namespace_id.len() as u64).to_be_bytes());
                hash.update(range.namespace_id.as_bytes());
                hash.update((range.key_prefix.len() as u64).to_be_bytes());
                hash.update(range.key_prefix.as_bytes());
            }
        }
        hash.finalize().into()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum NeighborManifestError {
    #[error("invalid operation neighbor JSON: {0}")]
    Json(String),
    #[error("operation neighbor node ID is empty")]
    EmptyNodeId,
    #[error("operation neighbor address is invalid: {0}")]
    InvalidAddress(String),
    #[error("operation neighbor points to self: {0:?}")]
    SelfNeighbor(NodeId),
    #[error("duplicate operation neighbor node ID: {0:?}")]
    DuplicateNodeId(NodeId),
    #[error("duplicate operation neighbor address: {0}")]
    DuplicateAddress(String),
    #[error("operation neighbor has no key prefixes: {0:?}")]
    EmptyPrefixes(NodeId),
    #[error("duplicate key prefix for operation neighbor {node_id:?}: {prefix}")]
    DuplicatePrefix { node_id: NodeId, prefix: String },
}

impl OperationNeighborManifest {
    pub fn from_json(json: &str, self_id: &NodeId) -> Result<Self, NeighborManifestError> {
        let parsed: Vec<OperationNeighbor> =
            serde_json::from_str(json).map_err(|e| NeighborManifestError::Json(e.to_string()))?;
        let mut neighbors = BTreeMap::new();
        let mut addresses = BTreeSet::new();
        for neighbor in parsed {
            if neighbor.node_id.0.trim().is_empty()
                || neighbor.node_id.0 != neighbor.node_id.0.trim()
            {
                return Err(NeighborManifestError::EmptyNodeId);
            }
            if neighbor.node_id == *self_id {
                return Err(NeighborManifestError::SelfNeighbor(self_id.clone()));
            }
            if neighbor.key_prefixes.is_empty() && neighbor.scoped_key_prefixes.is_empty() {
                return Err(NeighborManifestError::EmptyPrefixes(neighbor.node_id));
            }
            let mut prefixes = BTreeSet::new();
            for prefix in &neighbor.key_prefixes {
                if !prefixes.insert(prefix) {
                    return Err(NeighborManifestError::DuplicatePrefix {
                        node_id: neighbor.node_id.clone(),
                        prefix: prefix.clone(),
                    });
                }
            }
            let mut scoped_prefixes = BTreeSet::new();
            for range in &neighbor.scoped_key_prefixes {
                if range.namespace_id.is_empty()
                    || range.namespace_id.trim() != range.namespace_id
                    || !scoped_prefixes
                        .insert((range.namespace_id.clone(), range.key_prefix.clone()))
                {
                    return Err(NeighborManifestError::DuplicatePrefix {
                        node_id: neighbor.node_id.clone(),
                        prefix: range.key_prefix.clone(),
                    });
                }
            }
            let canonical_addr = canonical_address(&neighbor.addr)
                .ok_or_else(|| NeighborManifestError::InvalidAddress(neighbor.addr.clone()))?;
            if !addresses.insert(canonical_addr) {
                return Err(NeighborManifestError::DuplicateAddress(neighbor.addr));
            }
            if neighbors
                .insert(neighbor.node_id.clone(), neighbor.clone())
                .is_some()
            {
                return Err(NeighborManifestError::DuplicateNodeId(neighbor.node_id));
            }
        }
        Ok(Self {
            self_id: self_id.clone(),
            neighbors,
        })
    }

    pub fn self_id(&self) -> &NodeId {
        &self.self_id
    }

    /// Canonical commitment to this node's configured delivery routes. JSON
    /// row and prefix order do not change the routing identity.
    pub fn digest(&self) -> [u8; 32] {
        fn update_string(hash: &mut Sha256, value: &str) {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }

        let mut hash = Sha256::new();
        hash.update(b"asteroidb/operation-neighbor-manifest/v1");
        update_string(&mut hash, &self.self_id.0);
        hash.update((self.neighbors.len() as u64).to_be_bytes());
        for neighbor in self.neighbors.values() {
            update_string(&mut hash, &neighbor.node_id.0);
            update_string(&mut hash, &neighbor.addr);

            hash.update((neighbor.key_prefixes.len() as u64).to_be_bytes());
            let mut prefixes = neighbor.key_prefixes.iter().collect::<Vec<_>>();
            prefixes.sort();
            for prefix in prefixes {
                update_string(&mut hash, prefix);
            }

            hash.update((neighbor.scoped_key_prefixes.len() as u64).to_be_bytes());
            let mut scoped = neighbor.scoped_key_prefixes.iter().collect::<Vec<_>>();
            scoped.sort_by(|a, b| {
                (&a.namespace_id, &a.key_prefix).cmp(&(&b.namespace_id, &b.key_prefix))
            });
            for range in scoped {
                update_string(&mut hash, &range.namespace_id);
                update_string(&mut hash, &range.key_prefix);
            }
        }
        hash.finalize().into()
    }

    pub fn neighbors(&self) -> impl ExactSizeIterator<Item = &OperationNeighbor> {
        self.neighbors.values()
    }

    pub fn get(&self, node_id: &NodeId) -> Option<&OperationNeighbor> {
        self.neighbors.get(node_id)
    }

    /// Addresses to which `key` must be forwarded. The order is stable by
    /// neighbor node ID, independent of manifest row order.
    pub fn permitted_addrs(&self, key: &str) -> Vec<String> {
        self.neighbors
            .values()
            .filter(|neighbor| {
                neighbor
                    .key_prefixes
                    .iter()
                    .any(|prefix| key.starts_with(prefix))
            })
            .map(|neighbor| neighbor.addr.clone())
            .collect()
    }

    pub fn permitted_addrs_for(&self, namespace_id: &str, key: &str) -> Vec<String> {
        self.permitted_for(namespace_id, key)
            .map(|neighbor| neighbor.addr.clone())
            .collect()
    }

    pub fn permitted_for<'a>(
        &'a self,
        namespace_id: &'a str,
        key: &'a str,
    ) -> impl Iterator<Item = &'a OperationNeighbor> {
        self.neighbors
            .values()
            .filter(move |neighbor| neighbor.permits(namespace_id, key))
    }

    pub fn permitted(&self, key: &str) -> impl Iterator<Item = &OperationNeighbor> {
        self.neighbors.values().filter(move |neighbor| {
            neighbor
                .key_prefixes
                .iter()
                .any(|prefix| key.starts_with(prefix))
        })
    }
}

fn canonical_address(addr: &str) -> Option<String> {
    if addr.is_empty() || addr != addr.trim() {
        return None;
    }
    if let Ok(socket) = addr.parse::<SocketAddr>() {
        return (socket.port() != 0).then(|| socket.to_string());
    }
    let (host, port) = addr.rsplit_once(':')?;
    let port: u16 = port.parse().ok()?;
    if port == 0
        || host.is_empty()
        || host.starts_with('.')
        || host.ends_with('.')
        || host.contains("..")
        || !host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'.')
    {
        return None;
    }
    Some(format!("{}:{port}", host.to_ascii_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neighbor_digest_is_order_independent_and_binds_routes() {
        let first = parse(
            r#"[{"node_id":"b","addr":"b:8002","key_prefixes":["z/","a/"],"scoped_key_prefixes":[{"namespace_id":"users","key_prefix":"profile/"},{"namespace_id":"orders","key_prefix":"item/"}]},{"node_id":"a","addr":"a:8001","key_prefixes":[""]}]"#,
        )
        .unwrap();
        let reordered = parse(
            r#"[{"node_id":"a","addr":"a:8001","key_prefixes":[""]},{"node_id":"b","addr":"b:8002","key_prefixes":["a/","z/"],"scoped_key_prefixes":[{"namespace_id":"orders","key_prefix":"item/"},{"namespace_id":"users","key_prefix":"profile/"}]}]"#,
        )
        .unwrap();
        assert_eq!(first.digest(), reordered.digest());

        let changed_address = parse(
            r#"[{"node_id":"b","addr":"b:8003","key_prefixes":["z/","a/"],"scoped_key_prefixes":[{"namespace_id":"users","key_prefix":"profile/"},{"namespace_id":"orders","key_prefix":"item/"}]},{"node_id":"a","addr":"a:8001","key_prefixes":[""]}]"#,
        )
        .unwrap();
        assert_ne!(first.digest(), changed_address.digest());

        let changed_scope = parse(
            r#"[{"node_id":"b","addr":"b:8002","key_prefixes":["z/","a/"],"scoped_key_prefixes":[{"namespace_id":"users","key_prefix":"profile/"},{"namespace_id":"orders","key_prefix":"other/"}]},{"node_id":"a","addr":"a:8001","key_prefixes":[""]}]"#,
        )
        .unwrap();
        assert_ne!(first.digest(), changed_scope.digest());
    }

    #[test]
    fn scoped_neighbor_permission_separates_namespaces() {
        let manifest = parse(
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[],"scoped_key_prefixes":[{"namespace_id":"other-ns","key_prefix":"item/"}]},{"node_id":"c","addr":"127.0.0.1:8002","key_prefixes":["item/"]}]"#,
        )
        .unwrap();
        assert_eq!(
            manifest.permitted_addrs_for("ns", "item/1"),
            vec!["127.0.0.1:8002"]
        );
        assert_eq!(
            manifest.permitted_addrs_for("other-ns", "item/1"),
            vec!["127.0.0.1:8001", "127.0.0.1:8002"]
        );
        assert!(parse(
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":[],"scoped_key_prefixes":[{"namespace_id":"other-ns","key_prefix":"item/"},{"namespace_id":"other-ns","key_prefix":"item/"}]}]"#,
        )
        .is_err());
    }

    #[test]
    fn scoped_ownership_distinguishes_same_key_across_namespaces() {
        let owned = OperationOwnedPrefixes::from_json(
            r#"[{"namespace_id":"orders","key_prefix":"item/"},{"namespace_id":"users","key_prefix":"profile/"}]"#,
        )
        .unwrap();
        assert!(owned.owns_in_namespace("orders", "item/1"));
        assert!(!owned.owns_in_namespace("users", "item/1"));
        assert!(owned.owns_in_namespace("users", "profile/1"));
        assert!(!owned.owns("item/1"));
        let reversed = OperationOwnedPrefixes::from_json(
            r#"[{"namespace_id":"users","key_prefix":"profile/"},{"namespace_id":"orders","key_prefix":"item/"}]"#,
        )
        .unwrap();
        assert_eq!(owned.digest(), reversed.digest());
        assert!(OperationOwnedPrefixes::from_json(
            r#"[{"namespace_id":"orders","key_prefix":"item/"},{"namespace_id":"orders","key_prefix":"item/"}]"#,
        )
        .is_err());
    }

    #[test]
    fn legacy_ownership_keeps_existing_trust_digest() {
        let owned = OperationOwnedPrefixes::from_json(r#"["users/","orders/"]"#).unwrap();
        let mut expected = Sha256::new();
        expected.update(b"asteroidb/operation-owned-prefixes/v1");
        expected.update(2u64.to_be_bytes());
        for prefix in ["orders/", "users/"] {
            expected.update((prefix.len() as u64).to_be_bytes());
            expected.update(prefix.as_bytes());
        }
        assert_eq!(owned.digest(), <[u8; 32]>::from(expected.finalize()));
    }

    fn parse(json: &str) -> Result<OperationNeighborManifest, NeighborManifestError> {
        OperationNeighborManifest::from_json(json, &NodeId("self".into()))
    }

    #[test]
    fn orders_required_neighbors_by_id() {
        let manifest = parse(
            r#"[{"node_id":"z","addr":"node-z:8002","key_prefixes":["user/"]},{"node_id":"a","addr":"127.0.0.1:8001","key_prefixes":[""]}]"#,
        )
        .unwrap();
        assert_eq!(
            manifest
                .neighbors()
                .map(|neighbor| neighbor.node_id.0.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "z"]
        );
        assert_eq!(
            manifest.get(&NodeId("z".into())).unwrap().addr,
            "node-z:8002"
        );
        assert_eq!(
            manifest.permitted_addrs("user/1"),
            vec!["127.0.0.1:8001", "node-z:8002"]
        );
        assert_eq!(manifest.permitted_addrs("other/1"), vec!["127.0.0.1:8001"]);
    }

    #[test]
    fn rejects_missing_duplicate_and_self_destinations() {
        assert_eq!(parse("[]").unwrap().neighbors().count(), 0);
        assert_eq!(
            parse(r#"[{"node_id":"","addr":"host:1","key_prefixes":[""]}]"#),
            Err(NeighborManifestError::EmptyNodeId)
        );
        assert_eq!(
            parse(r#"[{"node_id":"self","addr":"other:1","key_prefixes":[""]}]"#),
            Err(NeighborManifestError::SelfNeighbor(NodeId("self".into())))
        );
        assert_eq!(
            parse(
                r#"[{"node_id":"a","addr":"a:1","key_prefixes":[""]},{"node_id":"a","addr":"b:2","key_prefixes":[""]}]"#
            ),
            Err(NeighborManifestError::DuplicateNodeId(NodeId("a".into())))
        );
        assert_eq!(
            parse(
                r#"[{"node_id":"a","addr":"EXAMPLE:80","key_prefixes":[""]},{"node_id":"b","addr":"example:80","key_prefixes":[""]}]"#
            ),
            Err(NeighborManifestError::DuplicateAddress("example:80".into()))
        );
        assert_eq!(
            parse(r#"[{"node_id":"a","addr":"a:1","key_prefixes":[]}]"#),
            Err(NeighborManifestError::EmptyPrefixes(NodeId("a".into())))
        );
        assert_eq!(
            parse(r#"[{"node_id":"a","addr":"a:1","key_prefixes":["p/","p/"]}]"#),
            Err(NeighborManifestError::DuplicatePrefix {
                node_id: NodeId("a".into()),
                prefix: "p/".into(),
            })
        );
    }

    #[test]
    fn rejects_addresses_that_change_transport_url() {
        for addr in [
            "",
            "localhost",
            "localhost:0",
            "http://host:80",
            "host:80/path",
            "host:abc",
            " host:80",
            "[::1]:0",
        ] {
            let json = format!(r#"[{{"node_id":"a","addr":"{addr}","key_prefixes":[""]}}]"#);
            assert!(matches!(
                parse(&json),
                Err(NeighborManifestError::InvalidAddress(_))
            ));
        }
    }
}
