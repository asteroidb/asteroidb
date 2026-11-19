//! Trusted, destination-specific routes for operation custody transfers.
//!
//! The route table is a deployment input. A row applies to every key with its
//! prefix; the longest matching prefix wins. The constructor checks every
//! effective prefix class against the declared adjacency and a strictly
//! decreasing rank, so a selected route always reaches its destination.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationRouteEdge {
    pub source: NodeId,
    pub receiver: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_id: Option<String>,
    /// An empty prefix permits every key.
    pub key_prefixes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationRouteRow {
    pub source: NodeId,
    pub destination: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_id: Option<String>,
    pub key_prefix: String,
    pub next_hop: NodeId,
    /// Distance to the final destination, strictly decreasing at each hop.
    pub rank: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationRouteManifest {
    rows: Vec<OperationRouteRow>,
}

/// Deployment owned list of final custody destinations, selected by longest
/// key prefix. A row may contain multiple final destinations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationDestinationRow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_id: Option<String>,
    pub key_prefix: String,
    pub destinations: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationRequiredDestinations {
    rows: Vec<OperationDestinationRow>,
}

impl OperationRequiredDestinations {
    pub fn from_json(json: &str) -> Result<Self, RouteManifestError> {
        let mut rows: Vec<OperationDestinationRow> =
            serde_json::from_str(json).map_err(|e| RouteManifestError::Json(e.to_string()))?;
        rows.sort_by(|a, b| {
            (&a.namespace_id, &a.key_prefix).cmp(&(&b.namespace_id, &b.key_prefix))
        });
        for (i, row) in rows.iter().enumerate() {
            if i > 0
                && rows[i - 1].namespace_id == row.namespace_id
                && rows[i - 1].key_prefix == row.key_prefix
                || row
                    .namespace_id
                    .as_deref()
                    .is_some_and(|id| !valid_namespace(id))
                || row.destinations.is_empty()
                || row.destinations.iter().any(|id| !valid_node(id))
                || row.destinations.iter().collect::<BTreeSet<_>>().len() != row.destinations.len()
            {
                return Err(RouteManifestError::InvalidTopology);
            }
        }
        Ok(Self { rows })
    }

    pub fn destinations(&self, key: &str) -> Option<&[NodeId]> {
        self.destinations_for(None, key)
    }

    pub fn destinations_for(&self, namespace_id: Option<&str>, key: &str) -> Option<&[NodeId]> {
        self.rows
            .iter()
            .filter(|row| {
                key.starts_with(&row.key_prefix)
                    && (row.namespace_id.is_none() || row.namespace_id.as_deref() == namespace_id)
            })
            .max_by_key(|row| (row.key_prefix.len(), row.namespace_id.is_some()))
            .map(|row| row.destinations.as_slice())
    }

    pub fn rows(&self) -> &[OperationDestinationRow] {
        &self.rows
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RouteManifestError {
    #[error("invalid route JSON: {0}")]
    Json(String),
    #[error("invalid route node identity")]
    InvalidNode,
    #[error("route source is its destination or next hop")]
    InvalidHop,
    #[error(
        "duplicate route for source {route_source:?}, destination {destination:?}, prefix {prefix:?}"
    )]
    DuplicateRoute {
        route_source: NodeId,
        destination: NodeId,
        prefix: String,
    },
    #[error("duplicate or invalid topology edge")]
    InvalidTopology,
    #[error("route hop is not an adjacent permitted edge")]
    ForbiddenHop,
    #[error("route has no continuation to its destination")]
    MissingContinuation,
    #[error("route rank does not decrease toward its destination")]
    NonDecreasingRank,
}

impl OperationRouteManifest {
    pub fn from_json(json: &str, edges: &[OperationRouteEdge]) -> Result<Self, RouteManifestError> {
        let rows: Vec<OperationRouteRow> =
            serde_json::from_str(json).map_err(|e| RouteManifestError::Json(e.to_string()))?;
        Self::new(rows, edges)
    }

    pub fn new(
        mut rows: Vec<OperationRouteRow>,
        edges: &[OperationRouteEdge],
    ) -> Result<Self, RouteManifestError> {
        let mut adjacency = BTreeSet::new();
        for edge in edges {
            if !valid_node(&edge.source)
                || !valid_node(&edge.receiver)
                || edge
                    .namespace_id
                    .as_deref()
                    .is_some_and(|id| !valid_namespace(id))
                || edge.source == edge.receiver
                || edge.key_prefixes.is_empty()
                || edge.key_prefixes.iter().collect::<BTreeSet<_>>().len()
                    != edge.key_prefixes.len()
                || !adjacency.insert((
                    edge.source.clone(),
                    edge.receiver.clone(),
                    edge.namespace_id.clone(),
                ))
            {
                return Err(RouteManifestError::InvalidTopology);
            }
        }
        rows.sort_by(|a, b| {
            (&a.source, &a.destination, &a.namespace_id, &a.key_prefix).cmp(&(
                &b.source,
                &b.destination,
                &b.namespace_id,
                &b.key_prefix,
            ))
        });
        for (index, row) in rows.iter().enumerate() {
            if !valid_node(&row.source)
                || !valid_node(&row.destination)
                || !valid_node(&row.next_hop)
                || row
                    .namespace_id
                    .as_deref()
                    .is_some_and(|id| !valid_namespace(id))
            {
                return Err(RouteManifestError::InvalidNode);
            }
            if row.source == row.destination || row.source == row.next_hop {
                return Err(RouteManifestError::InvalidHop);
            }
            if index > 0 {
                let prior = &rows[index - 1];
                if prior.source == row.source
                    && prior.destination == row.destination
                    && prior.namespace_id == row.namespace_id
                    && prior.key_prefix == row.key_prefix
                {
                    return Err(RouteManifestError::DuplicateRoute {
                        route_source: row.source.clone(),
                        destination: row.destination.clone(),
                        prefix: row.key_prefix.clone(),
                    });
                }
            }
            // The entire key class covered by this row must be permitted.
            if !edges.iter().any(|edge| {
                edge.source == row.source
                    && edge.receiver == row.next_hop
                    && (edge.namespace_id.is_none() || edge.namespace_id == row.namespace_id)
                    && edge
                        .key_prefixes
                        .iter()
                        .any(|prefix| row.key_prefix.starts_with(prefix))
            }) {
                return Err(RouteManifestError::ForbiddenHop);
            }
        }
        let manifest = Self { rows };
        // All longest-prefix choices are constant between declared prefix
        // boundaries. Each declared prefix is therefore a complete witness
        // for its key class, including any more-specific override downstream.
        let namespaces: BTreeSet<_> = std::iter::once(None)
            .chain(
                manifest
                    .rows
                    .iter()
                    .filter_map(|row| row.namespace_id.as_deref().map(Some)),
            )
            .collect();
        for namespace_id in namespaces {
            for row in manifest.rows.iter().filter(|row| {
                row.namespace_id.is_none() || row.namespace_id.as_deref() == namespace_id
            }) {
                for witness in manifest
                    .rows
                    .iter()
                    .filter(|other| {
                        other.destination == row.destination
                            && (other.namespace_id.is_none()
                                || other.namespace_id.as_deref() == namespace_id)
                    })
                    .map(|other| other.key_prefix.as_str())
                    .filter(|prefix| prefix.starts_with(&row.key_prefix))
                {
                    if manifest.next_hop_for(&row.source, &row.destination, namespace_id, witness)
                        != Some(row)
                    {
                        continue;
                    }
                    if row.next_hop == row.destination {
                        if row.rank == 0 {
                            return Err(RouteManifestError::NonDecreasingRank);
                        }
                    } else {
                        let next = manifest
                            .next_hop_for(&row.next_hop, &row.destination, namespace_id, witness)
                            .ok_or(RouteManifestError::MissingContinuation)?;
                        if next.rank >= row.rank {
                            return Err(RouteManifestError::NonDecreasingRank);
                        }
                    }
                }
            }
        }
        Ok(manifest)
    }

    pub fn next_hop(
        &self,
        source: &NodeId,
        destination: &NodeId,
        key: &str,
    ) -> Option<&OperationRouteRow> {
        self.next_hop_for(source, destination, None, key)
    }

    pub fn next_hop_for(
        &self,
        source: &NodeId,
        destination: &NodeId,
        namespace_id: Option<&str>,
        key: &str,
    ) -> Option<&OperationRouteRow> {
        self.rows
            .iter()
            .filter(|row| {
                &row.source == source
                    && &row.destination == destination
                    && (row.namespace_id.is_none() || row.namespace_id.as_deref() == namespace_id)
                    && key.starts_with(&row.key_prefix)
            })
            .max_by_key(|row| (row.key_prefix.len(), row.namespace_id.is_some()))
    }

    pub fn rows(&self) -> &[OperationRouteRow] {
        &self.rows
    }
}

fn valid_node(id: &NodeId) -> bool {
    !id.0.is_empty() && id.0.trim() == id.0
}

fn valid_namespace(id: &str) -> bool {
    !id.is_empty() && id.trim() == id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_destinations_override_legacy_at_the_same_prefix() {
        let required = OperationRequiredDestinations::from_json(
            r#"[{"key_prefix":"","destinations":["c"]},{"namespace_id":"other-ns","key_prefix":"","destinations":["d"]}]"#,
        )
        .unwrap();
        assert_eq!(
            required.destinations_for(Some("ns"), "item/1"),
            Some([id("c")].as_slice())
        );
        assert_eq!(
            required.destinations_for(Some("other-ns"), "item/1"),
            Some([id("d")].as_slice())
        );
        assert!(OperationRequiredDestinations::from_json(
            r#"[{"namespace_id":"other-ns","key_prefix":"","destinations":["c"]},{"namespace_id":"other-ns","key_prefix":"","destinations":["d"]}]"#,
        )
        .is_err());
    }

    #[test]
    fn scoped_route_uses_separate_hop_and_checks_continuation() {
        let edges = [
            edge("a", "b", &[""]),
            edge("b", "d", &[""]),
            edge("a", "c", &[""]),
            edge("c", "d", &[""]),
        ];
        let mut rows = vec![
            row("a", "d", "", "b", 2),
            row("b", "d", "", "d", 1),
            OperationRouteRow {
                namespace_id: Some("other-ns".into()),
                ..row("a", "d", "", "c", 2)
            },
            OperationRouteRow {
                namespace_id: Some("other-ns".into()),
                ..row("c", "d", "", "d", 1)
            },
        ];
        let routes = OperationRouteManifest::new(rows.clone(), &edges).unwrap();
        assert_eq!(
            routes
                .next_hop_for(&id("a"), &id("d"), Some("ns"), "item/1")
                .unwrap()
                .next_hop,
            id("b")
        );
        assert_eq!(
            routes
                .next_hop_for(&id("a"), &id("d"), Some("other-ns"), "item/1")
                .unwrap()
                .next_hop,
            id("c")
        );
        rows.pop();
        assert_eq!(
            OperationRouteManifest::new(rows, &edges),
            Err(RouteManifestError::MissingContinuation)
        );
    }

    fn id(value: &str) -> NodeId {
        NodeId(value.into())
    }

    fn edge(source: &str, receiver: &str, prefixes: &[&str]) -> OperationRouteEdge {
        OperationRouteEdge {
            source: id(source),
            receiver: id(receiver),
            namespace_id: None,
            key_prefixes: prefixes.iter().map(|prefix| (*prefix).into()).collect(),
        }
    }

    fn row(
        source: &str,
        destination: &str,
        prefix: &str,
        next: &str,
        rank: u64,
    ) -> OperationRouteRow {
        OperationRouteRow {
            source: id(source),
            destination: id(destination),
            namespace_id: None,
            key_prefix: prefix.into(),
            next_hop: id(next),
            rank,
        }
    }

    #[test]
    fn selects_longest_prefix_independent_of_input_order() {
        let edges = [
            edge("a", "b", &[""]),
            edge("a", "c", &["user/"]),
            edge("b", "d", &[""]),
            edge("c", "d", &["user/"]),
        ];
        let rows = vec![
            row("b", "d", "", "d", 1),
            row("a", "d", "user/", "c", 2),
            row("c", "d", "user/", "d", 1),
            row("a", "d", "", "b", 2),
        ];
        let manifest = OperationRouteManifest::new(rows.clone(), &edges).unwrap();
        assert_eq!(
            manifest
                .next_hop(&id("a"), &id("d"), "user/7")
                .unwrap()
                .next_hop,
            id("c")
        );
        assert_eq!(
            manifest
                .next_hop(&id("a"), &id("d"), "other")
                .unwrap()
                .next_hop,
            id("b")
        );
        assert_eq!(manifest.next_hop(&id("d"), &id("d"), "user/7"), None);
        let mut reversed = rows;
        reversed.reverse();
        assert_eq!(
            manifest,
            OperationRouteManifest::new(reversed, &edges).unwrap()
        );
    }

    #[test]
    fn rejects_missing_continuation_and_cycles() {
        let edges = [edge("a", "b", &[""]), edge("b", "a", &[""])];
        assert_eq!(
            OperationRouteManifest::new(vec![row("a", "d", "", "b", 2)], &edges),
            Err(RouteManifestError::MissingContinuation)
        );
        assert_eq!(
            OperationRouteManifest::new(
                vec![row("a", "d", "", "b", 2), row("b", "d", "", "a", 1)],
                &edges
            ),
            Err(RouteManifestError::NonDecreasingRank)
        );
    }

    #[test]
    fn rejects_duplicate_forbidden_and_non_decreasing_routes() {
        let edges = [edge("a", "b", &["user/"]), edge("b", "d", &["user/"])];
        let same = row("a", "d", "user/", "b", 2);
        assert!(matches!(
            OperationRouteManifest::new(vec![same.clone(), same], &edges),
            Err(RouteManifestError::DuplicateRoute { .. })
        ));
        assert_eq!(
            OperationRouteManifest::new(vec![row("a", "d", "", "b", 2)], &edges),
            Err(RouteManifestError::ForbiddenHop)
        );
        assert_eq!(
            OperationRouteManifest::new(
                vec![
                    row("a", "d", "user/", "b", 1),
                    row("b", "d", "user/", "d", 1)
                ],
                &edges
            ),
            Err(RouteManifestError::NonDecreasingRank)
        );
        assert_eq!(
            OperationRouteManifest::new(vec![row("b", "d", "user/", "d", 0)], &edges),
            Err(RouteManifestError::NonDecreasingRank)
        );
    }

    #[test]
    fn validates_more_specific_downstream_override() {
        let edges = [
            edge("a", "b", &[""]),
            edge("b", "d", &[""]),
            edge("b", "c", &["special/"]),
            edge("c", "d", &["special/"]),
        ];
        let rows = vec![
            row("a", "d", "", "b", 2),
            row("b", "d", "", "d", 1),
            row("b", "d", "special/", "c", 2),
            row("c", "d", "special/", "d", 1),
        ];
        assert_eq!(
            OperationRouteManifest::new(rows, &edges),
            Err(RouteManifestError::NonDecreasingRank)
        );
    }

    #[test]
    fn keeps_destinations_separate_across_unrelated_branches() {
        let edges = [
            edge("a", "b", &[""]),
            edge("b", "d", &[""]),
            edge("a", "c", &[""]),
            edge("c", "e", &[""]),
        ];
        let manifest = OperationRouteManifest::new(
            vec![
                row("a", "d", "", "b", 2),
                row("b", "d", "", "d", 1),
                row("a", "e", "", "c", 2),
                row("c", "e", "", "e", 1),
            ],
            &edges,
        )
        .unwrap();
        assert_eq!(
            manifest
                .next_hop(&id("a"), &id("d"), "key")
                .unwrap()
                .next_hop,
            id("b")
        );
        assert_eq!(
            manifest
                .next_hop(&id("a"), &id("e"), "key")
                .unwrap()
                .next_hop,
            id("c")
        );
        assert!(manifest.next_hop(&id("b"), &id("e"), "key").is_none());
    }
}
