//! Routed, authenticated delivery of complete creation-policy witnesses.
//! The receiver learns the policy without adding the Create value to its
//! application ledger. Every acknowledged hop has durable custody.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Router, middleware};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::http::auth::require_bearer_token;
use crate::network::operation_link_auth::OperationLinkAuth;
use crate::network::operation_neighbors::{OperationNeighborManifest, OperationOwnedPrefixes};
use crate::network::operation_policy_outbox::{
    OperationPolicyOutbox, PendingPolicyWitness, PolicyOutboxError,
};
use crate::network::operation_routes::{OperationRequiredDestinations, OperationRouteManifest};
use crate::network::operation_transport::{
    INTERNAL_OPERATION_BODY_LIMIT, OperationInbox, OperationPacket, OperationReceipt,
    OperationTransportClient, ReceiveError, verify_link_request,
};
use crate::network::retry_cursor::RetryCursor;
use crate::operation::CandidateKvAction;
use crate::types::NodeId;

pub const POLICY_WITNESS_PATH: &str = "/api/internal/operations/policy-witness";
pub const POLICY_SUBMIT_PATH: &str = "/api/operations/policy/submit";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyWitnessTransfer {
    pub version: u32,
    pub sender: NodeId,
    pub receiver: NodeId,
    pub destination: NodeId,
    pub packet: OperationPacket,
}

#[derive(Clone)]
pub struct PolicyWitnessRelay {
    inbox: Arc<Mutex<OperationInbox>>,
    outbox: Arc<Mutex<OperationPolicyOutbox>>,
    neighbors: Arc<OperationNeighborManifest>,
    owned: Arc<OperationOwnedPrefixes>,
    routes: Arc<OperationRouteManifest>,
    required: Arc<OperationRequiredDestinations>,
    node_id: NodeId,
}

#[derive(Debug, thiserror::Error)]
pub enum PolicyRelayError {
    #[error("policy witness is outside the pinned route or destination manifest")]
    Route,
    #[error("policy witness is not a complete Create")]
    NotCreate,
    #[error("policy witness reception failed: {0:?}")]
    Receive(ReceiveError),
    #[error("policy witness outbox failed: {0}")]
    Outbox(#[from] PolicyOutboxError),
    #[error("policy witness compaction failed: {0}")]
    Compaction(#[from] io::Error),
}

impl PolicyWitnessRelay {
    pub async fn new(
        inbox: Arc<Mutex<OperationInbox>>,
        outbox: Arc<Mutex<OperationPolicyOutbox>>,
        neighbors: OperationNeighborManifest,
        owned: OperationOwnedPrefixes,
        routes: OperationRouteManifest,
        required: OperationRequiredDestinations,
    ) -> Result<Self, PolicyRelayError> {
        let node_id = neighbors.self_id().clone();
        let relay = Self {
            inbox,
            outbox,
            neighbors: Arc::new(neighbors),
            owned: Arc::new(owned),
            routes: Arc::new(routes),
            required: Arc::new(required),
            node_id,
        };
        // The outbox's bytes are not trusted merely because their JSON and
        // hash are well formed. Every recovered send must match the separately
        // authenticated inbox witness and the pinned route.
        let entries = {
            let mut outbox = relay.outbox.lock().await;
            outbox.recover_if_poisoned()?;
            outbox.entries()
        };
        let mut inbox = relay.inbox.lock().await;
        relay.verify_saved_sends(&inbox, &entries)?;
        // A crash can leave a validated witness durable before its first
        // outbox record. Any node that knows the authenticated policy may
        // publish it along a configured route to a fixed destination.
        inbox.enable_policy_publishing(Arc::clone(&relay.required));
        let mut outbox = relay.outbox.lock().await;
        relay.enroll_held_witnesses(&mut inbox, &mut outbox)?;
        relay.compact_completed_witnesses(&mut inbox, &mut outbox)?;
        drop(outbox);
        drop(inbox);
        Ok(relay)
    }

    fn verify_saved_sends(
        &self,
        inbox: &OperationInbox,
        entries: &[PendingPolicyWitness],
    ) -> Result<(), PolicyRelayError> {
        for entry in entries {
            let namespace = &entry.identity.subject.namespace_id;
            let key = &entry.identity.declaration.key;
            if !self.destination_allowed(namespace, key, &entry.destination)
                || self
                    .next_hop_for_key(&entry.destination, namespace, key)
                    .is_none_or(|(hop, addr)| hop != entry.next_hop || addr != entry.peer_addr)
                || inbox.policy_witness_identity(namespace, key) != Some(entry.identity.clone())
                || entry.packet.as_ref().is_some_and(|packet| {
                    crate::network::operation_transport::PolicyWitnessIdentity::from(packet)
                        != entry.identity
                })
            {
                return Err(PolicyRelayError::Route);
            }
        }
        Ok(())
    }

    fn enroll_held_witnesses(
        &self,
        inbox: &mut OperationInbox,
        outbox: &mut OperationPolicyOutbox,
    ) -> Result<(), PolicyRelayError> {
        // A regular operation Create also makes its authenticated creation
        // policy available for publication. It need not have been submitted
        // through the separate policy-only endpoint. Persist an independent
        // witness before enrolling its send so the outbox can still be
        // authenticated after the ordinary operation packet is reclaimed.
        for packet in inbox.packets() {
            if !matches!(
                packet.signed.bound.operation.action,
                CandidateKvAction::Create { .. }
            ) {
                continue;
            }
            let bound = &packet.signed.bound;
            if self
                .required
                .destinations_for(
                    Some(&bound.operation.subject.namespace_id),
                    bound.operation.action.key(),
                )
                .is_none()
            {
                continue;
            }
            if inbox
                .policy_witness_identity(
                    &bound.operation.subject.namespace_id,
                    bound.operation.action.key(),
                )
                .is_none()
            {
                inbox.receive_policy_witness(packet)?;
            }
        }
        for packet in inbox.policy_witness_packets() {
            let bound = &packet.signed.bound;
            let namespace = &bound.operation.subject.namespace_id;
            let key = bound.operation.action.key();
            let Some(destinations) = self.required.destinations_for(Some(namespace), key) else {
                continue;
            };
            for destination in destinations {
                if destination == &self.node_id {
                    continue;
                }
                // A final receiver need not have routes to other destinations.
                // Nodes with an available fixed route can help republish the
                // same authenticated policy without altering application data.
                let Some((next_hop, addr)) = self.next_hop(destination, &packet) else {
                    continue;
                };
                outbox.enqueue(destination.clone(), next_hop, addr, packet.clone())?;
            }
        }
        Ok(())
    }

    fn reconcile_durable_enrollments(&self) -> Result<(), PolicyRelayError> {
        let mut inbox = self.inbox.blocking_lock();
        inbox
            .recover_if_poisoned()
            .map_err(|error| PolicyRelayError::Receive(ReceiveError::Persist(error)))?;
        let mut outbox = self.outbox.blocking_lock();
        outbox.recover_if_poisoned()?;
        self.verify_saved_sends(&inbox, &outbox.entries())?;
        self.enroll_held_witnesses(&mut inbox, &mut outbox)?;
        self.compact_completed_witnesses(&mut inbox, &mut outbox)
    }

    /// Recheck the global recovery and trust conditions before sending an
    /// already enrolled witness after an unrelated enrollment has failed.
    fn verify_pending_sends(&self) -> Result<(), PolicyRelayError> {
        let mut inbox = self.inbox.blocking_lock();
        inbox
            .recover_if_poisoned()
            .map_err(|error| PolicyRelayError::Receive(ReceiveError::Persist(error)))?;
        let mut outbox = self.outbox.blocking_lock();
        outbox.recover_if_poisoned()?;
        self.verify_saved_sends(&inbox, &outbox.entries())
    }

    fn compact_completed_witnesses(
        &self,
        inbox: &mut OperationInbox,
        outbox: &mut OperationPolicyOutbox,
    ) -> Result<(), PolicyRelayError> {
        outbox.compact_acked_and_sync()?;
        for packet in inbox.policy_witness_packets() {
            let identity =
                crate::network::operation_transport::PolicyWitnessIdentity::from(&packet);
            let Some(destinations) = self.required.destinations_for(
                Some(&identity.subject.namespace_id),
                &identity.declaration.key,
            ) else {
                continue;
            };
            if destinations.iter().all(|destination| {
                destination == &self.node_id
                    || outbox.has_durable_receipt(
                        destination,
                        &identity.subject,
                        identity.packet_commitment,
                    )
                    // A transit receiver may have no route to another final
                    // destination. An owner is a policy source, so a missing
                    // route cannot discharge its delivery obligation.
                    || (!self.owned.owns_in_namespace(
                        &identity.subject.namespace_id,
                        &identity.declaration.key,
                    ) && self
                        .next_hop_for_key(
                            destination,
                            &identity.subject.namespace_id,
                            &identity.declaration.key,
                        )
                        .is_none())
            }) {
                inbox.compact_policy_witness(
                    &identity.subject.namespace_id,
                    &identity.declaration.key,
                )?;
            }
        }
        Ok(())
    }

    fn destination_allowed(&self, namespace_id: &str, key: &str, destination: &NodeId) -> bool {
        self.required
            .destinations_for(Some(namespace_id), key)
            .is_some_and(|destinations| destinations.contains(destination))
    }

    fn next_hop(&self, destination: &NodeId, packet: &OperationPacket) -> Option<(NodeId, String)> {
        let bound = &packet.signed.bound;
        self.next_hop_for_key(
            destination,
            &bound.operation.subject.namespace_id,
            bound.operation.action.key(),
        )
    }

    fn next_hop_for_key(
        &self,
        destination: &NodeId,
        namespace: &str,
        key: &str,
    ) -> Option<(NodeId, String)> {
        let row = self
            .routes
            .next_hop_for(&self.node_id, destination, Some(namespace), key)?;
        let neighbor = self.neighbors.get(&row.next_hop)?;
        neighbor
            .permits(namespace, key)
            .then(|| (neighbor.node_id.clone(), neighbor.addr.clone()))
    }

    /// A locally authorized owner publishes the Create policy to the fixed
    /// policy destinations. A successful response means local witness and all
    /// initial send obligations are durable.
    pub fn submit(&self, packet: OperationPacket) -> Result<OperationReceipt, PolicyRelayError> {
        let bound = &packet.signed.bound;
        if !matches!(bound.operation.action, CandidateKvAction::Create { .. }) {
            return Err(PolicyRelayError::NotCreate);
        }
        let namespace = &bound.operation.subject.namespace_id;
        let key = bound.operation.action.key();
        if !self.owned.owns_in_namespace(namespace, key) {
            return Err(PolicyRelayError::Route);
        }
        let destinations = self
            .required
            .destinations_for(Some(namespace), key)
            .ok_or(PolicyRelayError::Route)?;
        let mut sends = Vec::new();
        for destination in destinations {
            if destination != &self.node_id {
                let (next_hop, addr) = self
                    .next_hop(destination, &packet)
                    .ok_or(PolicyRelayError::Route)?;
                sends.push((destination.clone(), next_hop, addr));
            }
        }
        let mut inbox = self.inbox.blocking_lock();
        let receipt = inbox.receive_policy_witness(packet.clone())?;
        let mut outbox = self.outbox.blocking_lock();
        for (destination, next_hop, addr) in sends {
            outbox.enqueue(destination, next_hop, addr, packet.clone())?;
        }
        self.compact_completed_witnesses(&mut inbox, &mut outbox)?;
        Ok(receipt)
    }

    /// Called only after the adjacent peer's link signature has been checked.
    pub fn receive(
        &self,
        transfer: PolicyWitnessTransfer,
    ) -> Result<OperationReceipt, PolicyRelayError> {
        if transfer.version != 1
            || transfer.sender == self.node_id
            || transfer.receiver != self.node_id
            || !matches!(
                transfer.packet.signed.bound.operation.action,
                CandidateKvAction::Create { .. }
            )
        {
            return Err(PolicyRelayError::Route);
        }
        let bound = &transfer.packet.signed.bound;
        let namespace = &bound.operation.subject.namespace_id;
        let key = bound.operation.action.key();
        if !self.destination_allowed(namespace, key, &transfer.destination)
            || self
                .routes
                .next_hop_for(
                    &transfer.sender,
                    &transfer.destination,
                    Some(namespace),
                    key,
                )
                .is_none_or(|row| row.next_hop != self.node_id)
        {
            return Err(PolicyRelayError::Route);
        }
        let onward = if transfer.destination != self.node_id {
            Some(
                self.next_hop(&transfer.destination, &transfer.packet)
                    .ok_or(PolicyRelayError::Route)?,
            )
        } else {
            None
        };
        let mut inbox = self.inbox.blocking_lock();
        let receipt = inbox.receive_policy_witness(transfer.packet.clone())?;
        let mut outbox = self.outbox.blocking_lock();
        if let Some((next_hop, addr)) = onward
            && let Err(error) =
                outbox.enqueue(transfer.destination, next_hop, addr, transfer.packet)
        {
            tracing::warn!(error = %error, "durable policy witness awaits send enrollment retry");
        }
        if let Err(error) = self.compact_completed_witnesses(&mut inbox, &mut outbox) {
            tracing::warn!(error = %error, "durable policy witness awaits compaction retry");
        }
        Ok(receipt)
    }

    pub fn outbox(&self) -> Arc<Mutex<OperationPolicyOutbox>> {
        Arc::clone(&self.outbox)
    }
}

impl From<ReceiveError> for PolicyRelayError {
    fn from(error: ReceiveError) -> Self {
        Self::Receive(error)
    }
}

fn error_status(error: PolicyRelayError) -> StatusCode {
    match error {
        PolicyRelayError::Route | PolicyRelayError::NotCreate => StatusCode::FORBIDDEN,
        PolicyRelayError::Receive(ReceiveError::Rejected(_)) => StatusCode::FORBIDDEN,
        PolicyRelayError::Receive(_)
        | PolicyRelayError::Outbox(_)
        | PolicyRelayError::Compaction(_) => {
            tracing::warn!(error = %error, "policy witness persistence failed");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

async fn receive_http(
    State(relay): State<PolicyWitnessRelay>,
    Json(transfer): Json<PolicyWitnessTransfer>,
) -> Result<Json<OperationReceipt>, StatusCode> {
    tokio::task::spawn_blocking(move || relay.receive(transfer))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
        .map_err(error_status)
}

async fn submit_http(
    State(relay): State<PolicyWitnessRelay>,
    Json(packet): Json<OperationPacket>,
) -> Result<Json<OperationReceipt>, StatusCode> {
    tokio::task::spawn_blocking(move || relay.submit(packet))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
        .map_err(error_status)
}

pub fn policy_witness_submit_router(relay: PolicyWitnessRelay) -> Router {
    Router::new()
        .route(POLICY_SUBMIT_PATH, post(submit_http))
        .with_state(relay)
}

pub fn policy_witness_router_authenticated(
    relay: PolicyWitnessRelay,
    token: String,
    link: OperationLinkAuth,
) -> Router {
    Router::new()
        .route(
            POLICY_WITNESS_PATH,
            post(receive_http).layer(DefaultBodyLimit::max(INTERNAL_OPERATION_BODY_LIMIT)),
        )
        .layer(middleware::from_fn(move |request, next| {
            let link = link.clone();
            async move { verify_link_request(request, next, link).await }
        }))
        .layer(middleware::from_fn(move |request, next| {
            let token = token.clone();
            require_bearer_token(request, next, token)
        }))
        .with_state(relay)
}

pub fn spawn_policy_witness_retry_worker(
    relay: PolicyWitnessRelay,
    token: String,
    link: OperationLinkAuth,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = OperationTransportClient::authenticated(token, link);
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut cursor = RetryCursor::default();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                _ = ticker.tick() => {
                    if *shutdown.borrow() { break; }
                    let source = relay.clone();
                    match tokio::task::spawn_blocking(move || source.reconcile_durable_enrollments()).await {
                        Ok(Ok(())) => {},
                        Ok(Err(error)) => {
                            tracing::warn!(error = %error, "policy witness send enrollment failed");
                            let verifier = relay.clone();
                            match tokio::task::spawn_blocking(move || verifier.verify_pending_sends()).await {
                                Ok(Ok(())) => {},
                                unsafe_state => {
                                    tracing::warn!(error = ?unsafe_state, "policy witness retry safety check failed");
                                    continue;
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, "policy witness enrollment task failed");
                            continue;
                        }
                    }
                    let pending = relay.outbox.lock().await.pending();
                    let selected = cursor.next(pending.iter().map(|entry| entry.id.clone()));
                    let Some(entry) = selected.and_then(|id| pending.into_iter().find(|entry| entry.id == id)) else {
                        continue;
                    };
                    let transfer = PolicyWitnessTransfer {
                        version: 1,
                        sender: relay.node_id.clone(),
                        receiver: entry.next_hop.clone(),
                        destination: entry.destination.clone(),
                        packet: entry.packet.expect("pending policy send retains exact packet"),
                    };
                    match client.send_policy_witness(&entry.peer_addr, &transfer).await {
                        Ok(receipt) => {
                            if let Err(error) = relay.outbox.lock().await.record_authenticated_receipt(
                                &entry.id,
                                &transfer.receiver,
                                &receipt,
                            ) {
                                tracing::warn!(error = %error, "policy witness ACK persistence failed");
                            }
                        }
                        Err(error) => tracing::debug!(error = %error, "policy witness retry remains pending"),
                    }
                }
            }
        }
    })
}

pub fn policy_route_digest(
    routes: &OperationRouteManifest,
    required: &OperationRequiredDestinations,
    neighbors: &OperationNeighborManifest,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"asteroidb/policy-witness-routes/v1");
    hash.update(serde_json::to_vec(routes.rows()).expect("validated route manifest serializes"));
    hash.update(serde_json::to_vec(required.rows()).expect("validated destinations serialize"));
    hash.update(neighbors.digest());
    hash.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use ed25519_dalek::SigningKey;
    use tower::ServiceExt;

    use crate::authority::writer_auth::{SignedBoundOperation, SignedCreation, WriterRegistry};
    use crate::hlc::HlcTimestamp;
    use crate::network::operation_outbox::OperationOutbox;
    use crate::network::operation_routes::OperationRouteEdge;
    use crate::network::operation_transport::{AuthenticatedOperationAdmission, OperationRelay};
    use crate::operation::{
        BoundOperation, CertificationMode, CrdtSubject, Declaration, EventDot, OperationLeaf,
    };

    fn id(value: &str) -> NodeId {
        NodeId(value.into())
    }

    fn disabled_create() -> OperationPacket {
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: id("writer"),
                counter: 1,
            },
        };
        let bound = BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: "key".into(),
                    value_bytes: b"private".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 1,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject,
                key: "key".into(),
                mode: CertificationMode::Disabled,
            },
        };
        let key = SigningKey::from_bytes(&[7; 32]);
        let creation = SignedCreation::sign(&bound, &key).unwrap();
        OperationPacket {
            signed: SignedBoundOperation::sign(bound, &key, creation).unwrap(),
        }
    }

    async fn node(
        dir: &std::path::Path,
        name: &str,
        neighbors_json: &str,
        owned_json: &str,
        routes: &OperationRouteManifest,
        required: &OperationRequiredDestinations,
    ) -> PolicyWitnessRelay {
        let node_id = id(name);
        let neighbors = OperationNeighborManifest::from_json(neighbors_json, &node_id).unwrap();
        let owned = OperationOwnedPrefixes::from_json(owned_json).unwrap();
        let mut registry = WriterRegistry::new();
        registry
            .register(
                id("writer"),
                SigningKey::from_bytes(&[7; 32]).verifying_key(),
                ["ns".to_owned()].into(),
            )
            .unwrap();
        let admission = Arc::new(AuthenticatedOperationAdmission::new(
            Arc::new(registry),
            "ns".into(),
        ));
        let inbox = Arc::new(Mutex::new(
            OperationInbox::open(dir.join("inbox"), admission).unwrap(),
        ));
        let outbox = Arc::new(Mutex::new(
            OperationPolicyOutbox::open(
                dir.join("outbox"),
                node_id,
                policy_route_digest(routes, required, &neighbors),
                [8; 32],
            )
            .unwrap(),
        ));
        PolicyWitnessRelay::new(
            inbox,
            outbox,
            neighbors,
            owned,
            routes.clone(),
            required.clone(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn routed_policy_witness_reaches_terminal_without_application_value() {
        let root = tempfile::tempdir().unwrap();
        let edges: Vec<OperationRouteEdge> = serde_json::from_str(
            r#"[{"source":"a","receiver":"b","key_prefixes":[""]},
                 {"source":"b","receiver":"c","key_prefixes":[""]}]"#,
        )
        .unwrap();
        let routes = OperationRouteManifest::from_json(
            r#"[{"source":"a","destination":"c","key_prefix":"","next_hop":"b","rank":2},
                 {"source":"b","destination":"c","key_prefix":"","next_hop":"c","rank":1}]"#,
            &edges,
        )
        .unwrap();
        let required =
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["c"]}]"#)
                .unwrap();
        let a = node(
            &root.path().join("a"),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8011","key_prefixes":[""]}]"#,
            r#"[""]"#,
            &routes,
            &required,
        )
        .await;
        let b = node(
            &root.path().join("b"),
            "b",
            r#"[{"node_id":"a","addr":"127.0.0.1:8010","key_prefixes":[""]},
                 {"node_id":"c","addr":"127.0.0.1:8012","key_prefixes":[""]}]"#,
            "[]",
            &routes,
            &required,
        )
        .await;
        let c = node(
            &root.path().join("c"),
            "c",
            r#"[{"node_id":"b","addr":"127.0.0.1:8011","key_prefixes":[""]}]"#,
            r#"[""]"#,
            &routes,
            &required,
        )
        .await;
        let create = disabled_create();
        tokio::task::spawn_blocking({
            let a = a.clone();
            let create = create.clone();
            move || a.submit(create).unwrap()
        })
        .await
        .unwrap();
        let first = a.outbox.lock().await.pending().pop().unwrap();
        let first_receipt = tokio::task::spawn_blocking({
            let b = b.clone();
            move || {
                b.receive(PolicyWitnessTransfer {
                    version: 1,
                    sender: id("a"),
                    receiver: id("b"),
                    destination: id("c"),
                    packet: first.packet.unwrap(),
                })
                .unwrap()
            }
        })
        .await
        .unwrap();
        a.outbox
            .lock()
            .await
            .record_authenticated_receipt(&first.id, &id("b"), &first_receipt)
            .unwrap();
        tokio::task::spawn_blocking({
            let a = a.clone();
            move || a.reconcile_durable_enrollments().unwrap()
        })
        .await
        .unwrap();
        assert!(
            a.inbox
                .lock()
                .await
                .policy_witness_packet("ns", "key")
                .is_none()
        );
        assert!(a.outbox.lock().await.entries()[0].packet.is_none());
        drop(b);
        let b = node(
            &root.path().join("b"),
            "b",
            r#"[{"node_id":"a","addr":"127.0.0.1:8010","key_prefixes":[""]},
                 {"node_id":"c","addr":"127.0.0.1:8012","key_prefixes":[""]}]"#,
            "[]",
            &routes,
            &required,
        )
        .await;
        let second = b.outbox.lock().await.pending().pop().unwrap();
        let second_receipt = tokio::task::spawn_blocking({
            let c = c.clone();
            move || {
                c.receive(PolicyWitnessTransfer {
                    version: 1,
                    sender: id("b"),
                    receiver: id("c"),
                    destination: id("c"),
                    packet: second.packet.unwrap(),
                })
                .unwrap()
            }
        })
        .await
        .unwrap();
        assert!(
            c.inbox
                .lock()
                .await
                .policy_witness_packet("ns", "key")
                .is_none()
        );
        b.outbox
            .lock()
            .await
            .record_authenticated_receipt(&second.id, &id("c"), &second_receipt)
            .unwrap();
        tokio::task::spawn_blocking({
            let b = b.clone();
            move || b.reconcile_durable_enrollments().unwrap()
        })
        .await
        .unwrap();
        assert!(
            b.inbox
                .lock()
                .await
                .policy_witness_packet("ns", "key")
                .is_none()
        );
        assert!(b.outbox.lock().await.entries()[0].packet.is_none());
        let inbox = c.inbox.lock().await;
        assert!(inbox.get(&create.signed.bound.operation.subject).is_none());
        assert_eq!(
            inbox.ordinary_read("ns", "key"),
            crate::api::operation_view::OperationRead::Unknown
        );
        assert_eq!(
            inbox.certified_read(&[], "ns", "key").result,
            crate::api::operation_view::CertifiedReadResult::NotTargeted
        );
        drop(inbox);
        drop(c);
        let c = node(
            &root.path().join("c"),
            "c",
            r#"[{"node_id":"b","addr":"127.0.0.1:8011","key_prefixes":[""]}]"#,
            r#"[""]"#,
            &routes,
            &required,
        )
        .await;
        assert_eq!(
            c.inbox.lock().await.certified_read(&[], "ns", "key").result,
            crate::api::operation_view::CertifiedReadResult::NotTargeted
        );
    }

    #[tokio::test]
    async fn received_witness_acks_after_durable_save_when_outbox_needs_recovery() {
        let root = tempfile::tempdir().unwrap();
        let edges: Vec<OperationRouteEdge> = serde_json::from_str(
            r#"[{"source":"a","receiver":"b","key_prefixes":[""]},
                 {"source":"b","receiver":"c","key_prefixes":[""]}]"#,
        )
        .unwrap();
        let routes = OperationRouteManifest::from_json(
            r#"[{"source":"a","destination":"c","key_prefix":"","next_hop":"b","rank":2},
                 {"source":"b","destination":"c","key_prefix":"","next_hop":"c","rank":1}]"#,
            &edges,
        )
        .unwrap();
        let required =
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["c"]}]"#)
                .unwrap();
        let b = node(
            root.path(),
            "b",
            r#"[{"node_id":"a","addr":"127.0.0.1:8010","key_prefixes":[""]},
                 {"node_id":"c","addr":"127.0.0.1:8012","key_prefixes":[""]}]"#,
            "[]",
            &routes,
            &required,
        )
        .await;
        b.outbox.lock().await.mark_uncertain_for_test();
        let packet = disabled_create();
        let copy = b.clone();
        let received = packet.clone();
        let receipt = tokio::task::spawn_blocking(move || {
            copy.receive(PolicyWitnessTransfer {
                version: 1,
                sender: id("a"),
                receiver: id("b"),
                destination: id("c"),
                packet: received,
            })
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(receipt.commitment, packet.commitment());
        assert!(
            b.inbox
                .lock()
                .await
                .policy_witness_packet("ns", "key")
                .is_some()
        );
        assert!(b.outbox.lock().await.pending().is_empty());

        let copy = b.clone();
        tokio::task::spawn_blocking(move || copy.reconcile_durable_enrollments())
            .await
            .unwrap()
            .unwrap();
        let pending = b.outbox.lock().await.pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].packet, Some(packet));
    }

    #[tokio::test]
    async fn two_destinations_retain_witness_until_both_ack_across_restart() {
        let root = tempfile::tempdir().unwrap();
        let edges: Vec<OperationRouteEdge> = serde_json::from_str(
            r#"[{"source":"a","receiver":"b","key_prefixes":[""]},
                 {"source":"a","receiver":"c","key_prefixes":[""]}]"#,
        )
        .unwrap();
        let routes = OperationRouteManifest::from_json(
            r#"[{"source":"a","destination":"b","key_prefix":"","next_hop":"b","rank":1},
                 {"source":"a","destination":"c","key_prefix":"","next_hop":"c","rank":1}]"#,
            &edges,
        )
        .unwrap();
        let required = OperationRequiredDestinations::from_json(
            r#"[{"key_prefix":"","destinations":["b","c"]}]"#,
        )
        .unwrap();
        let a_neighbors = r#"[{"node_id":"b","addr":"127.0.0.1:8011","key_prefixes":[""]},
                             {"node_id":"c","addr":"127.0.0.1:8012","key_prefixes":[""]}]"#;
        let a_path = root.path().join("a");
        let a = node(&a_path, "a", a_neighbors, r#"[""]"#, &routes, &required).await;
        let b = node(
            &root.path().join("b"),
            "b",
            r#"[{"node_id":"a","addr":"127.0.0.1:8010","key_prefixes":[""]}]"#,
            "[]",
            &routes,
            &required,
        )
        .await;
        let c = node(
            &root.path().join("c"),
            "c",
            r#"[{"node_id":"a","addr":"127.0.0.1:8010","key_prefixes":[""]}]"#,
            "[]",
            &routes,
            &required,
        )
        .await;
        let create = disabled_create();
        tokio::task::spawn_blocking({
            let a = a.clone();
            let create = create.clone();
            move || a.submit(create).unwrap()
        })
        .await
        .unwrap();
        let pending = a.outbox.lock().await.pending();
        assert_eq!(pending.len(), 2);
        let to_b = pending
            .iter()
            .find(|entry| entry.destination == id("b"))
            .unwrap();
        let receipt_b = tokio::task::spawn_blocking({
            let b = b.clone();
            let packet = to_b.packet.clone().unwrap();
            move || {
                b.receive(PolicyWitnessTransfer {
                    version: 1,
                    sender: id("a"),
                    receiver: id("b"),
                    destination: id("b"),
                    packet,
                })
                .unwrap()
            }
        })
        .await
        .unwrap();
        a.outbox
            .lock()
            .await
            .record_authenticated_receipt(&to_b.id, &id("b"), &receipt_b)
            .unwrap();
        tokio::task::spawn_blocking({
            let a = a.clone();
            move || a.reconcile_durable_enrollments().unwrap()
        })
        .await
        .unwrap();
        assert!(
            a.inbox
                .lock()
                .await
                .policy_witness_packet("ns", "key")
                .is_some()
        );
        assert_eq!(a.outbox.lock().await.pending().len(), 1);

        drop(a);
        let a = node(&a_path, "a", a_neighbors, r#"[""]"#, &routes, &required).await;
        assert!(
            a.inbox
                .lock()
                .await
                .policy_witness_packet("ns", "key")
                .is_some()
        );
        let to_c = a.outbox.lock().await.pending().pop().unwrap();
        assert_eq!(to_c.destination, id("c"));
        let receipt_c = tokio::task::spawn_blocking({
            let c = c.clone();
            let packet = to_c.packet.clone().unwrap();
            move || {
                c.receive(PolicyWitnessTransfer {
                    version: 1,
                    sender: id("a"),
                    receiver: id("c"),
                    destination: id("c"),
                    packet,
                })
                .unwrap()
            }
        })
        .await
        .unwrap();
        a.outbox
            .lock()
            .await
            .record_authenticated_receipt(&to_c.id, &id("c"), &receipt_c)
            .unwrap();
        tokio::task::spawn_blocking({
            let a = a.clone();
            move || a.reconcile_durable_enrollments().unwrap()
        })
        .await
        .unwrap();
        assert!(
            a.inbox
                .lock()
                .await
                .policy_witness_packet("ns", "key")
                .is_none()
        );
        assert!(
            a.outbox
                .lock()
                .await
                .entries()
                .iter()
                .all(|entry| entry.packet.is_none())
        );
        for receiver in [&b, &c] {
            let inbox = receiver.inbox.lock().await;
            assert_eq!(
                inbox.ordinary_read("ns", "key"),
                crate::api::operation_view::OperationRead::Unknown
            );
            assert_eq!(
                inbox.certified_read(&[], "ns", "key").result,
                crate::api::operation_view::CertifiedReadResult::NotTargeted
            );
            assert!(inbox.policy_witness_packet("ns", "key").is_none());
        }
    }

    #[tokio::test]
    async fn direct_data_and_independent_policy_route_share_the_witness() {
        let root = tempfile::tempdir().unwrap();
        let edges: Vec<OperationRouteEdge> =
            serde_json::from_str(r#"[{"source":"a","receiver":"b","key_prefixes":[""]}]"#).unwrap();
        let routes = OperationRouteManifest::from_json(
            r#"[{"source":"a","destination":"b","key_prefix":"","next_hop":"b","rank":1}]"#,
            &edges,
        )
        .unwrap();
        let required =
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["b"]}]"#)
                .unwrap();
        let neighbors_json = r#"[{"node_id":"b","addr":"127.0.0.1:8011","key_prefixes":[""]}]"#;
        let a = node(
            &root.path().join("a"),
            "a",
            neighbors_json,
            r#"[""]"#,
            &routes,
            &required,
        )
        .await;
        let b = node(
            &root.path().join("b"),
            "b",
            r#"[{"node_id":"a","addr":"127.0.0.1:8010","key_prefixes":[""]}]"#,
            "[]",
            &routes,
            &required,
        )
        .await;
        let neighbors = OperationNeighborManifest::from_json(neighbors_json, &id("a")).unwrap();
        let owned = OperationOwnedPrefixes::from_json(r#"[""]"#).unwrap();
        let direct_outbox = Arc::new(Mutex::new(
            OperationOutbox::open_for_node(root.path().join("a/direct-outbox"), id("a")).unwrap(),
        ));
        direct_outbox
            .lock()
            .await
            .ensure_link_manifest([8; 32])
            .unwrap();
        let data = OperationRelay::new(
            Arc::clone(&a.inbox),
            Arc::clone(&direct_outbox),
            neighbors,
            owned,
        )
        .await
        .unwrap();
        let create = disabled_create();
        data.submit_local(create.clone()).await.unwrap();
        let direct = direct_outbox.lock().await.pending().next().unwrap().clone();
        assert!(direct.destination.is_none());
        assert_eq!(direct.receiver, id("b"));

        tokio::task::spawn_blocking({
            let a = a.clone();
            move || a.reconcile_durable_enrollments().unwrap()
        })
        .await
        .unwrap();
        let policy = a.outbox.lock().await.pending().pop().unwrap();
        assert_eq!(policy.destination, id("b"));
        let receipt = tokio::task::spawn_blocking({
            let b = b.clone();
            move || {
                b.receive(PolicyWitnessTransfer {
                    version: 1,
                    sender: id("a"),
                    receiver: id("b"),
                    destination: id("b"),
                    packet: policy.packet.unwrap(),
                })
                .unwrap()
            }
        })
        .await
        .unwrap();
        assert!(!receipt.already_present);
        assert!(
            b.inbox
                .lock()
                .await
                .policy_witness_identity("ns", "key")
                .is_some()
        );
        assert!(b.inbox.lock().await.packets().is_empty());
    }

    #[tokio::test]
    async fn restart_enrolls_witness_saved_before_first_send_record() {
        let root = tempfile::tempdir().unwrap();
        let edges: Vec<OperationRouteEdge> =
            serde_json::from_str(r#"[{"source":"a","receiver":"b","key_prefixes":[""]}]"#).unwrap();
        let routes = OperationRouteManifest::from_json(
            r#"[{"source":"a","destination":"b","key_prefix":"","next_hop":"b","rank":1}]"#,
            &edges,
        )
        .unwrap();
        let required =
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["b"]}]"#)
                .unwrap();
        let path = root.path().join("a");
        let neighbors = r#"[{"node_id":"b","addr":"127.0.0.1:8011","key_prefixes":[""]}]"#;
        let a = node(&path, "a", neighbors, r#"[""]"#, &routes, &required).await;
        tokio::task::spawn_blocking({
            let inbox = Arc::clone(&a.inbox);
            move || {
                inbox
                    .blocking_lock()
                    .receive_policy_witness(disabled_create())
                    .unwrap()
            }
        })
        .await
        .unwrap();
        assert!(a.outbox.lock().await.pending().is_empty());
        drop(a);
        let recovered = node(&path, "a", neighbors, r#"[""]"#, &routes, &required).await;
        let pending = recovered.outbox.lock().await.pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].destination, id("b"));
        assert_eq!(pending[0].packet, Some(disabled_create()));
    }

    #[tokio::test]
    async fn owner_keeps_policy_value_when_required_destination_has_no_route() {
        let root = tempfile::tempdir().unwrap();
        let routes = OperationRouteManifest::from_json("[]", &[]).unwrap();
        let required =
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["b"]}]"#)
                .unwrap();
        let relay = node(root.path(), "a", "[]", r#"[""]"#, &routes, &required).await;
        let packet = disabled_create();
        tokio::task::spawn_blocking({
            let relay = relay.clone();
            let packet = packet.clone();
            move || {
                relay.inbox.blocking_lock().receive(packet).unwrap();
                relay.reconcile_durable_enrollments().unwrap();
            }
        })
        .await
        .unwrap();
        assert!(relay.outbox.lock().await.entries().is_empty());
        let inbox = relay.inbox.lock().await;
        assert_eq!(
            inbox.policy_witness_packet("ns", "key"),
            Some(packet.clone())
        );
        assert!(inbox.verified_compact_policy_witness("ns", "key").is_none());
        drop(inbox);
        drop(relay);
        let reopened = node(root.path(), "a", "[]", r#"[""]"#, &routes, &required).await;
        assert_eq!(
            reopened
                .inbox
                .lock()
                .await
                .policy_witness_packet("ns", "key"),
            Some(packet)
        );
    }

    #[tokio::test]
    async fn ordinary_create_enrolls_policy_delivery_on_tick_and_after_restart() {
        let root = tempfile::tempdir().unwrap();
        let edges: Vec<OperationRouteEdge> =
            serde_json::from_str(r#"[{"source":"a","receiver":"b","key_prefixes":[""]}]"#).unwrap();
        let routes = OperationRouteManifest::from_json(
            r#"[{"source":"a","destination":"b","key_prefix":"","next_hop":"b","rank":1}]"#,
            &edges,
        )
        .unwrap();
        let required =
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["b"]}]"#)
                .unwrap();
        let neighbors = r#"[{"node_id":"b","addr":"127.0.0.1:8011","key_prefixes":[""]}]"#;
        let packet = disabled_create();

        let live_path = root.path().join("live");
        let live = node(&live_path, "a", neighbors, r#"[""]"#, &routes, &required).await;
        tokio::task::spawn_blocking({
            let live = live.clone();
            let packet = packet.clone();
            move || {
                live.inbox.blocking_lock().receive(packet).unwrap();
                live.reconcile_durable_enrollments().unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(
            live.outbox.lock().await.pending()[0].packet,
            Some(packet.clone())
        );
        assert!(
            live.inbox
                .lock()
                .await
                .policy_witness_packet("ns", "key")
                .is_some()
        );
        drop(live);
        let reopened = node(&live_path, "a", neighbors, r#"[""]"#, &routes, &required).await;
        assert_eq!(
            reopened.outbox.lock().await.pending()[0].packet,
            Some(packet.clone())
        );
        drop(reopened);

        let crash_path = root.path().join("crash");
        let before_crash = node(&crash_path, "a", neighbors, r#"[""]"#, &routes, &required).await;
        tokio::task::spawn_blocking({
            let inbox = Arc::clone(&before_crash.inbox);
            let packet = packet.clone();
            move || inbox.blocking_lock().receive(packet).unwrap()
        })
        .await
        .unwrap();
        assert!(before_crash.outbox.lock().await.pending().is_empty());
        drop(before_crash);
        let recovered = node(&crash_path, "a", neighbors, r#"[""]"#, &routes, &required).await;
        assert_eq!(
            recovered.outbox.lock().await.pending()[0].packet,
            Some(packet)
        );
    }

    #[tokio::test]
    async fn relay_recovers_uncertain_outbox_and_continues_enrollment() {
        let root = tempfile::tempdir().unwrap();
        let edges: Vec<OperationRouteEdge> =
            serde_json::from_str(r#"[{"source":"a","receiver":"b","key_prefixes":[""]}]"#).unwrap();
        let routes = OperationRouteManifest::from_json(
            r#"[{"source":"a","destination":"b","key_prefix":"","next_hop":"b","rank":1}]"#,
            &edges,
        )
        .unwrap();
        let required =
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["b"]}]"#)
                .unwrap();
        let relay = node(
            root.path(),
            "a",
            r#"[{"node_id":"b","addr":"127.0.0.1:8011","key_prefixes":[""]}]"#,
            r#"[""]"#,
            &routes,
            &required,
        )
        .await;
        tokio::task::spawn_blocking({
            let relay = relay.clone();
            move || {
                relay
                    .inbox
                    .blocking_lock()
                    .receive_policy_witness(disabled_create())
                    .unwrap();
                relay.outbox.blocking_lock().mark_uncertain_for_test();
                relay.reconcile_durable_enrollments().unwrap();
            }
        })
        .await
        .unwrap();
        let pending = relay.outbox.lock().await.pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].packet, Some(disabled_create()));
    }

    #[tokio::test]
    async fn policy_witness_http_requires_signed_adjacent_identity() {
        let root = tempfile::tempdir().unwrap();
        let edges: Vec<OperationRouteEdge> =
            serde_json::from_str(r#"[{"source":"a","receiver":"b","key_prefixes":[""]}]"#).unwrap();
        let routes = OperationRouteManifest::from_json(
            r#"[{"source":"a","destination":"b","key_prefix":"","next_hop":"b","rank":1}]"#,
            &edges,
        )
        .unwrap();
        let required =
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["b"]}]"#)
                .unwrap();
        let a_neighbors = OperationNeighborManifest::from_json(
            r#"[{"node_id":"b","addr":"127.0.0.1:8011","key_prefixes":[""]}]"#,
            &id("a"),
        )
        .unwrap();
        let b_neighbors_json = r#"[{"node_id":"a","addr":"127.0.0.1:8010","key_prefixes":[""]}]"#;
        let b = node(
            &root.path().join("b"),
            "b",
            b_neighbors_json,
            r#"[""]"#,
            &routes,
            &required,
        )
        .await;
        let a_seed = [1; 32];
        let b_seed = [2; 32];
        let a_keys = format!(
            r#"[{{"node_id":"b","public_key":"{}"}}]"#,
            hex::encode(SigningKey::from_bytes(&b_seed).verifying_key().as_bytes())
        );
        let b_keys = format!(
            r#"[{{"node_id":"a","public_key":"{}"}}]"#,
            hex::encode(SigningKey::from_bytes(&a_seed).verifying_key().as_bytes())
        );
        let a_link =
            OperationLinkAuth::from_manifest(id("a"), &hex::encode(a_seed), &a_keys, &a_neighbors)
                .unwrap();
        let b_neighbors = OperationNeighborManifest::from_json(b_neighbors_json, &id("b")).unwrap();
        let b_link =
            OperationLinkAuth::from_manifest(id("b"), &hex::encode(b_seed), &b_keys, &b_neighbors)
                .unwrap();
        let transfer = PolicyWitnessTransfer {
            version: 1,
            sender: id("a"),
            receiver: id("b"),
            destination: id("b"),
            packet: disabled_create(),
        };
        let bytes = serde_json::to_vec(&transfer).unwrap();
        let (_, signature) = a_link.sign_request(&id("b"), "POST", POLICY_WITNESS_PATH, &bytes);
        let router = policy_witness_router_authenticated(b.clone(), "secret".into(), b_link);
        let unsigned = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(POLICY_WITNESS_PATH)
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(bytes.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unsigned.status(), StatusCode::UNAUTHORIZED);
        let signed = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(POLICY_WITNESS_PATH)
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .header(crate::network::operation_link_auth::SENDER, "a")
                    .header(crate::network::operation_link_auth::RECEIVER, "b")
                    .header(crate::network::operation_link_auth::SIGNATURE, signature)
                    .body(Body::from(bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(signed.status(), StatusCode::OK);
        assert_eq!(
            b.inbox.lock().await.certified_read(&[], "ns", "key").result,
            crate::api::operation_view::CertifiedReadResult::NotTargeted
        );
    }

    #[tokio::test]
    async fn retry_worker_delivers_reopened_outbox_over_authenticated_http() {
        let root = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let b_addr = listener.local_addr().unwrap().to_string();
        let edges: Vec<OperationRouteEdge> =
            serde_json::from_str(r#"[{"source":"a","receiver":"b","key_prefixes":[""]}]"#).unwrap();
        let routes = OperationRouteManifest::from_json(
            r#"[{"source":"a","destination":"b","key_prefix":"","next_hop":"b","rank":1}]"#,
            &edges,
        )
        .unwrap();
        let required =
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["b"]}]"#)
                .unwrap();
        let a_neighbors_json =
            format!(r#"[{{"node_id":"b","addr":"{b_addr}","key_prefixes":[""]}}]"#);
        let b_neighbors_json = r#"[{"node_id":"a","addr":"127.0.0.1:8010","key_prefixes":[""]}]"#;
        let b = node(
            &root.path().join("b"),
            "b",
            b_neighbors_json,
            r#"[""]"#,
            &routes,
            &required,
        )
        .await;
        let a_seed = [1; 32];
        let b_seed = [2; 32];
        let a_keys = format!(
            r#"[{{"node_id":"b","public_key":"{}"}}]"#,
            hex::encode(SigningKey::from_bytes(&b_seed).verifying_key().as_bytes())
        );
        let b_keys = format!(
            r#"[{{"node_id":"a","public_key":"{}"}}]"#,
            hex::encode(SigningKey::from_bytes(&a_seed).verifying_key().as_bytes())
        );
        let a_neighbors =
            OperationNeighborManifest::from_json(&a_neighbors_json, &id("a")).unwrap();
        let b_neighbors = OperationNeighborManifest::from_json(b_neighbors_json, &id("b")).unwrap();
        let a_link =
            OperationLinkAuth::from_manifest(id("a"), &hex::encode(a_seed), &a_keys, &a_neighbors)
                .unwrap();
        let b_link =
            OperationLinkAuth::from_manifest(id("b"), &hex::encode(b_seed), &b_keys, &b_neighbors)
                .unwrap();
        let a_dir = root.path().join("a");
        let a = node(
            &a_dir,
            "a",
            &a_neighbors_json,
            r#"[""]"#,
            &routes,
            &required,
        )
        .await;
        tokio::task::spawn_blocking({
            let a = a.clone();
            move || a.submit(disabled_create()).unwrap()
        })
        .await
        .unwrap();
        assert_eq!(a.outbox.lock().await.pending().len(), 1);
        drop(a);
        let a = node(
            &a_dir,
            "a",
            &a_neighbors_json,
            r#"[""]"#,
            &routes,
            &required,
        )
        .await;
        assert_eq!(a.outbox.lock().await.pending().len(), 1);
        let app = policy_witness_router_authenticated(b.clone(), "secret".into(), b_link);
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let worker =
            spawn_policy_witness_retry_worker(a.clone(), "secret".into(), a_link, shutdown_rx);
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let learned = b.inbox.lock().await.certified_read(&[], "ns", "key").result
                    == crate::api::operation_view::CertifiedReadResult::NotTargeted;
                if learned && a.outbox.lock().await.pending().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        shutdown_tx.send(true).unwrap();
        worker.await.unwrap();
        server.abort();
    }
}
