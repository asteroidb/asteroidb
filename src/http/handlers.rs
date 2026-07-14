use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use tokio::sync::Mutex;

use super::codec::{deserialize_internal, internal_response};

use crate::api::certified::{CertifiedApi, OnTimeout};
use crate::api::eventual::EventualApi;
#[cfg(feature = "native-crypto")]
use crate::authority::bls::{BlsPublicKey, BlsSignature};
#[cfg(not(feature = "native-crypto"))]
use crate::authority::bls_stub::{BlsPublicKey, BlsSignature};
use crate::authority::certificate::{EpochConfig, KeysetRegistry};
use crate::authority::equivocation::{
    EquivocationDetector, MAX_OBSERVED_PER_REQUEST, ObserveOutcome,
};
use crate::control_plane::consensus::ControlPlaneConsensus;
use crate::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use crate::crdt::pn_counter::PnCounter;
use crate::error::CrdtError;
use crate::ops::metrics::{MetricsSnapshot, RuntimeMetrics};
use crate::ops::slo::{SLO_CERTIFIED_READ_P99, SLO_EVENTUAL_READ_P99, SloSnapshot, SloTracker};
use crate::ops::write_atomic;
use crate::placement::PlacementPolicy;
use crate::placement::latency::LatencyModel;
use crate::placement::topology::TopologyView;
use crate::store::kv::CrdtValue;
use crate::store::wal::{SyncPolicy, WalPos, WalSyncer};
use crate::types::{KeyRange, NodeId, PolicyVersion};

use crate::network::PeerRegistry;
use crate::network::membership::{is_metadata_or_link_local, is_safe_peer_address};
use crate::network::sync::{
    DeltaEntry, DeltaSyncRequest, DeltaSyncResponse, KeyDumpResponse, SyncError, SyncRequest,
    SyncResponse,
};

use crate::session::SessionToken;

use super::types::{
    AnnounceRequest, AnnounceResponse, ApiError, AuthorityDefinitionResponse,
    CertifiedReadResponse, CertifiedWriteRequest, CertifiedWriteResponse, CrdtValueJson,
    EquivocationReport, EventualReadQuery, EventualReadResponse, EventualWriteRequest,
    FrontierJson, JoinRequest, JoinResponse, LeaveRequest, LeaveResponse, PeerInfo, PingRequest,
    PingResponse, PlacementPolicyResponse, ProofBundleJson, RemovePolicyRequest,
    SetAuthorityDefinitionRequest, SetPlacementPolicyRequest, StatusResponse, VerifyProofRequest,
    VerifyProofResponse, VersionHistoryResponse, WriteResponse,
};

/// Shared application state for HTTP handlers.
pub struct AppState {
    pub eventual: Arc<Mutex<EventualApi>>,
    pub certified: Arc<Mutex<CertifiedApi>>,
    pub namespace: Arc<RwLock<SystemNamespace>>,
    pub metrics: Arc<RuntimeMetrics>,
    /// Peer registry for node join/leave bootstrap.
    /// `None` when peer tracking is not needed (e.g. unit tests).
    pub peers: Option<Arc<Mutex<PeerRegistry>>>,
    /// File path to persist the peer registry to on join/leave.
    /// `None` disables persistence (e.g. unit tests).
    pub peer_persist_path: Option<PathBuf>,
    /// File path to persist the system namespace after control-plane mutations.
    /// `None` disables persistence (e.g. unit tests).
    pub namespace_persist_path: Option<PathBuf>,
    /// Control-plane consensus for gating namespace updates (FR-009).
    pub consensus: Arc<Mutex<ControlPlaneConsensus>>,
    /// Optional shared token for authenticating internal API requests.
    /// When `Some`, all `/api/internal/*` routes require a matching
    /// `Authorization: Bearer <token>` header.
    pub internal_token: Option<String>,
    /// This node's own ID, used to include the seed in join responses.
    pub self_node_id: Option<NodeId>,
    /// This node's own advertised address, used to include the seed in join responses.
    pub self_addr: Option<String>,
    /// Latency model for multi-region placement optimization.
    /// `None` when latency tracking is not configured.
    pub latency_model: Option<Arc<std::sync::RwLock<LatencyModel>>>,
    /// Known cluster nodes for topology view.
    /// `None` when topology tracking is not configured.
    pub cluster_nodes: Option<Arc<std::sync::RwLock<Vec<crate::node::Node>>>>,
    /// SLO tracker for budget monitoring.
    pub slo_tracker: Arc<SloTracker>,
    /// Keyset registry for registry-based proof verification.
    /// When `Some`, the `/api/certified/verify` endpoint uses
    /// registry-based verification instead of trusting caller-supplied keys.
    pub keyset_registry: Option<Arc<std::sync::RwLock<KeysetRegistry>>>,
    /// Epoch configuration for keyset expiry checks.
    pub epoch_config: EpochConfig,
    /// Current epoch, used for keyset expiry checks during verification.
    pub current_epoch: Arc<std::sync::atomic::AtomicU64>,
    /// When `true`, unsigned frontier pushes to `/api/internal/frontiers`
    /// are rejected. Without a keyset registry this rejects *all* frontier
    /// pushes (fail-closed), since no signature can be verified. Signed
    /// frontiers that fail verification are always rejected regardless of
    /// this flag.
    pub require_signed_frontiers: bool,
    /// Equivocation detector and evidence store. Shared (same `Arc`) with
    /// `NodeRunner` so that evidence detected on the HTTP receive path is
    /// gossiped by the runner's frontier push, and vice versa.
    pub equivocation: Arc<EquivocationDetector>,
    /// When `true`, attestations from authorities with recorded equivocation
    /// evidence are excluded from certificate assembly (their frontiers still
    /// advance — the max-monotone frontier value itself is low-poison).
    /// Opt-in (`ASTEROIDB_EXCLUDE_ACCUSED_AUTHORITIES`); the safe-by-default
    /// posture is detect-and-warn only, because exclusion can drop a scope
    /// below majority and stall certificate production.
    pub exclude_accused_authorities: bool,
    /// Group-commit WAL syncer for the eventual store. `None` when
    /// persistence is disabled. Under `SyncPolicy::Always`, write handlers
    /// wait (OUTSIDE the API lock) for the mutation's WAL record to be
    /// fdatasynced before acknowledging.
    pub eventual_wal: Option<Arc<WalSyncer>>,
    /// Group-commit WAL syncer for the certified store (see `eventual_wal`).
    pub certified_wal: Option<Arc<WalSyncer>>,
}

/// Wait for a mutation's WAL record to become durable before ack
/// (`SyncPolicy::Always` only; other policies acknowledge immediately).
///
/// Called AFTER the API lock is released so a slow fdatasync never blocks
/// other handlers or the sync loop; the group-commit syncer coalesces
/// concurrent waiters into one flush.
async fn wait_wal_durable(
    syncer: &Option<Arc<WalSyncer>>,
    pos: Option<WalPos>,
) -> Result<(), ApiError> {
    if let (Some(syncer), Some(pos)) = (syncer, pos)
        && syncer.policy() == SyncPolicy::Always
    {
        syncer
            .wait_durable(pos)
            .await
            .map_err(|e| ApiError(CrdtError::Storage(format!("WAL sync wait failed: {e}"))))?;
    }
    Ok(())
}

// ---------------------------------------------------------------
// Eventual handlers
// ---------------------------------------------------------------

/// `POST /api/eventual/write`
///
/// Accepts a typed CRDT operation and applies it to the eventual store.
pub async fn eventual_write(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EventualWriteRequest>,
) -> Result<Json<WriteResponse>, ApiError> {
    let written_key = req.key().to_string();

    let mut api = state.eventual.lock().await;

    let ts = match req {
        EventualWriteRequest::CounterInc { key } => api.eventual_counter_inc(&key)?,
        EventualWriteRequest::CounterDec { key } => api.eventual_counter_dec(&key)?,
        EventualWriteRequest::SetAdd { key, element } => api.eventual_set_add(&key, element)?,
        EventualWriteRequest::SetRemove { key, element } => {
            api.eventual_set_remove(&key, &element)?
        }
        EventualWriteRequest::MapSet {
            key,
            map_key,
            map_value,
        } => api.eventual_map_set(&key, map_key, map_value)?,
        EventualWriteRequest::MapDelete { key, map_key } => {
            api.eventual_map_delete(&key, &map_key)?
        }
        EventualWriteRequest::RegisterSet { key, value } => {
            api.eventual_register_set(&key, value)?
        }
    };
    let wal_pos = api.last_wal_pos();
    drop(api);

    // The session token doubles as a durability receipt: under
    // SyncPolicy::Always it must not be handed out until the WAL record
    // is on disk. Waiting happens after the lock is released.
    wait_wal_durable(&state.eventual_wal, wal_pos).await?;

    state.metrics.record_write_op(&written_key);

    Ok(Json(WriteResponse {
        ok: true,
        session_token: Some(SessionToken::from_hlc(&ts).encode()),
    }))
}

/// Upper bound for the `wait_ms` query parameter of `GET /api/eventual/:key`.
pub const MAX_SESSION_WAIT_MS: u64 = 5_000;

/// Polling interval while waiting for a session precondition.
const SESSION_POLL_INTERVAL_MS: u64 = 50;

/// `GET /api/eventual/:key`
///
/// Returns the local CRDT value for the given key.
///
/// Optional session guarantees (read-your-writes / monotonic reads): when
/// the request carries a `session_token` query parameter, the value is only
/// returned if the local replica provably contains all writes covered by
/// the token; otherwise the handler polls for up to `wait_ms` (capped at
/// [`MAX_SESSION_WAIT_MS`]) and then answers 412 `SESSION_NOT_SATISFIED`.
/// Without the parameter the behaviour and response bytes are identical to
/// the pre-session API (no extra cost for token-less reads).
pub async fn get_eventual(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Query(query): Query<EventualReadQuery>,
) -> Result<Json<EventualReadResponse>, ApiError> {
    let key = key.strip_prefix('/').unwrap_or(&key).to_string();
    let start = Instant::now();
    let want_session = query.session_token.is_some();
    // SECURITY: the client-supplied token must NEVER be fed into
    // `Hlc::update` — a forged far-future physical would poison the local
    // clock (clock-advance attack). It is parsed, bounds-checked, and used
    // for comparison only.
    let token = match query.session_token.as_deref() {
        // Absent → legacy behaviour; empty → no precondition, token issuance only.
        None | Some("") => None,
        Some(s) => {
            let token = SessionToken::parse(s)?;
            token.validate_bounds(crate::hlc::wall_clock_ms())?;
            Some(token)
        }
    };
    let deadline = start
        + std::time::Duration::from_millis(query.wait_ms.unwrap_or(0).min(MAX_SESSION_WAIT_MS));

    loop {
        let api = state.eventual.lock().await;
        let satisfied = token.as_ref().is_none_or(|t| api.session_check(&key, t));
        if satisfied {
            // Check-then-read under the same lock so the state cannot
            // change between the session check and the value read.
            let value = api.get_eventual(&key).map(CrdtValueJson::from_crdt_value);
            let session_token = want_session.then(|| {
                let mut response_token = token.clone().unwrap_or_default();
                // The read key's own change position is merged FIRST so it
                // counts as request-derived and survives the entry cap —
                // the origin contributing the observed value must not be
                // silently thinned away (monotonic reads).
                if let Some(key_ts) = api.store().timestamp_for(&key) {
                    response_token.merge_hlc(key_ts);
                }
                // Cover the full VISIBLE state, not just applied_origins:
                // contributions merged through possibly-incomplete
                // (unclaimed) deltas are readable here but make no applied
                // claim; a token that omitted them would let a stale
                // replica satisfy it while serving an older value — a
                // monotonic-reads lie. Over-covering is safe (412s only).
                response_token.merge_frontiers(api.store().visible_origins());
                response_token.encode()
            });
            drop(api);

            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            state
                .slo_tracker
                .record_observation(SLO_EVENTUAL_READ_P99, elapsed_ms);

            return Ok(Json(EventualReadResponse {
                key,
                value,
                session_token,
            }));
        }
        // Release the lock while sleeping so writes and sync can progress.
        drop(api);
        if Instant::now() >= deadline {
            return Err(CrdtError::SessionNotSatisfied { key }.into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(SESSION_POLL_INTERVAL_MS)).await;
    }
}

// ---------------------------------------------------------------
// Certified handlers
// ---------------------------------------------------------------

/// `POST /api/certified/write`
///
/// Writes a value that requires Authority majority certification.
pub async fn certified_write(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CertifiedWriteRequest>,
) -> Result<Json<CertifiedWriteResponse>, ApiError> {
    let on_timeout = match req.on_timeout.as_str() {
        "error" => OnTimeout::Error,
        "pending" => OnTimeout::Pending,
        other => {
            return Err(ApiError(CrdtError::InvalidArgument(format!(
                "invalid on_timeout value: {other}; expected \"error\" or \"pending\""
            ))));
        }
    };

    let crdt_value = json_to_crdt_value(&req.value)?;
    let written_key = req.key.clone();

    let mut api = state.certified.lock().await;
    let status = api.certified_write(req.key, crdt_value, on_timeout)?;
    let wal_pos = api.last_wal_pos();
    drop(api);

    // Durability before ack (SyncPolicy::Always), outside the lock.
    wait_wal_durable(&state.certified_wal, wal_pos).await?;

    state.metrics.record_write_op(&written_key);

    Ok(Json(CertifiedWriteResponse { status }))
}

/// `GET /api/certified/:key`
///
/// Returns the value with certification status, frontier, and proof bundle.
pub async fn get_certified(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Json<CertifiedReadResponse> {
    let key = key.strip_prefix('/').unwrap_or(&key).to_string();
    let start = Instant::now();
    let api = state.certified.lock().await;
    let read = api.get_certified(&key);

    let value = read.value.map(CrdtValueJson::from_crdt_value);
    let frontier = read.frontier.map(|f| FrontierJson {
        physical: f.physical,
        logical: f.logical,
        node_id: f.node_id,
    });

    let proof = read.proof.map(|p| {
        let certificate = p.certificate.as_ref().map(|cert| {
            use super::types::{AuthoritySignatureJson, CertificateJson};
            CertificateJson {
                keyset_version: cert.keyset_version.0,
                signatures: cert
                    .signatures
                    .iter()
                    .map(|s| {
                        let pk_hex: String = s
                            .public_key
                            .as_bytes()
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect();
                        let sig_hex: String = s
                            .signature
                            .to_bytes()
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect();
                        AuthoritySignatureJson {
                            authority_id: s.authority_id.0.clone(),
                            public_key: pk_hex,
                            signature: sig_hex,
                            keyset_version: s.keyset_version.0,
                        }
                    })
                    .collect(),
            }
        });

        // BLS aggregate certificate fields (round-trippable into
        // POST /api/certified/verify with signature_algorithm=Bls12_381).
        let (signature_algorithm, keyset_version, bls_signer_ids, bls_public_keys, bls_agg) =
            if let Some(bls_cert) = p.bls_certificate.as_ref() {
                (
                    Some("Bls12_381".to_string()),
                    Some(bls_cert.keyset_version.0),
                    Some(
                        bls_cert
                            .bls_signer_ids
                            .iter()
                            .map(|n| n.0.clone())
                            .collect::<Vec<String>>(),
                    ),
                    Some(
                        bls_cert
                            .bls_public_keys
                            .iter()
                            .map(|pk| pk.to_hex())
                            .collect::<Vec<String>>(),
                    ),
                    bls_cert
                        .bls_aggregated_signature
                        .as_ref()
                        .map(|sig| sig.to_hex()),
                )
            } else if let Some(cert) = p.certificate.as_ref() {
                (
                    Some("Ed25519".to_string()),
                    Some(cert.keyset_version.0),
                    None,
                    None,
                    None,
                )
            } else {
                (None, None, None, None, None)
            };

        ProofBundleJson {
            key_range_prefix: p.key_range.prefix,
            frontier: FrontierJson {
                physical: p.frontier_hlc.physical,
                logical: p.frontier_hlc.logical,
                node_id: p.frontier_hlc.node_id,
            },
            policy_version: p.policy_version.0,
            contributing_authorities: p
                .contributing_authorities
                .into_iter()
                .map(|n| n.0)
                .collect(),
            total_authorities: p.total_authorities,
            certificate,
            signature_algorithm,
            keyset_version,
            bls_signer_ids,
            bls_public_keys,
            bls_aggregate_signature: bls_agg,
        }
    });

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    state
        .slo_tracker
        .record_observation(SLO_CERTIFIED_READ_P99, elapsed_ms);

    Json(CertifiedReadResponse {
        key,
        value,
        status: read.status,
        frontier,
        proof,
    })
}

/// `GET /api/status/:key`
///
/// Returns the certification status of the latest write for the given key.
pub async fn get_certification_status(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Json<StatusResponse> {
    let key = key.strip_prefix('/').unwrap_or(&key).to_string();
    let api = state.certified.lock().await;
    let status = api.get_certification_status(&key);

    Json(StatusResponse { key, status })
}

// ---------------------------------------------------------------
// Internal frontier handlers
// ---------------------------------------------------------------

/// `POST /api/internal/frontiers`
///
/// Receives frontier updates from a peer and applies them to the local
/// `AckFrontierSet`. Monotonicity is enforced by `AckFrontierSet::update()`.
///
/// When a keyset registry is configured, signed frontiers are verified
/// (registry keys only) before the certified lock is taken; frontiers whose
/// signatures fail verification are dropped. Unsigned frontiers are accepted
/// unless `require_signed_frontiers` is enabled. Without a registry,
/// frontiers are accepted unverified (backwards-compatible) — unless
/// `require_signed_frontiers` is enabled, in which case everything is
/// rejected (fail-closed) since no signature can be verified.
///
/// Independent of signature status, a frontier is only accepted if its
/// authority is a member of the authority set defined for its key range
/// (when such a definition exists): otherwise any registered authority
/// could inflate the majority count of a range it does not own.
pub async fn post_internal_frontiers(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, super::codec::SerializationError> {
    use crate::authority::frontier_sig::{VerifiedAttestation, verify_frontier_signature};

    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());

    let req: crate::network::frontier_sync::FrontierPushRequest =
        deserialize_internal(&body, content_type)?;
    let crate::network::frontier_sync::FrontierPushRequest {
        frontiers,
        signatures,
        observed,
    } = req;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    // Set when new equivocation evidence was recorded, so the evidence store
    // is re-persisted after all sync guards are released.
    let mut evidence_dirty = false;

    // Verify signatures BEFORE taking the certified lock — signature
    // verification is CPU-heavy and must not serialize with other handlers.
    let mut signatures = signatures.into_iter();
    let mut to_apply: Vec<(
        crate::authority::ack_frontier::AckFrontier,
        Option<VerifiedAttestation>,
    )> = Vec::with_capacity(frontiers.len());

    {
        // Authority-set membership gate (FR-008): the sender must be one of
        // the authorities defined for the frontier's key range. Signature
        // verification only proves *who* signed, not that the signer owns
        // the range. Ranges without an authority definition cannot certify
        // writes anyway, so they are accepted for backwards compatibility.
        let ns = state.namespace.read().unwrap_or_else(|e| e.into_inner());
        let is_range_authority = |frontier: &crate::authority::ack_frontier::AckFrontier| -> bool {
            match ns.get_authorities_for_key(&frontier.key_range.prefix) {
                Some(def) => def.authority_nodes.contains(&frontier.authority_id),
                None => true,
            }
        };

        match &state.keyset_registry {
            None => {
                // Without a registry, relayed observations cannot be
                // verified — and unverifiable pairs must never become
                // evidence (they could frame an honest authority).
                for frontier in frontiers {
                    if state.require_signed_frontiers {
                        // Strict mode without a registry must fail closed:
                        // there is no key material to verify any signature,
                        // so accepting frontiers here would silently disable
                        // the security control the operator asked for.
                        tracing::warn!(
                            authority = %frontier.authority_id.0,
                            "rejecting frontier: require_signed_frontiers is set but no keyset registry is configured"
                        );
                        continue;
                    }
                    if !is_range_authority(&frontier) {
                        tracing::warn!(
                            authority = %frontier.authority_id.0,
                            key_range = %frontier.key_range.prefix,
                            "rejecting frontier from non-member of the range's authority set"
                        );
                        continue;
                    }
                    // No registry configured: legacy unverified acceptance.
                    to_apply.push((frontier, None));
                }
            }
            Some(registry_lock) => {
                let registry = registry_lock.read().unwrap_or_else(|e| e.into_inner());
                let current_epoch = state
                    .current_epoch
                    .load(std::sync::atomic::Ordering::Relaxed);
                for frontier in frontiers {
                    let signature = signatures.next().flatten();
                    if !is_range_authority(&frontier) {
                        tracing::warn!(
                            authority = %frontier.authority_id.0,
                            key_range = %frontier.key_range.prefix,
                            "rejecting frontier from non-member of the range's authority set"
                        );
                        continue;
                    }
                    match signature {
                        Some(sig) => match verify_frontier_signature(
                            &frontier,
                            &sig,
                            &registry,
                            current_epoch,
                            &state.epoch_config,
                        ) {
                            Ok(att) => {
                                // Equivocation check on the verified raw
                                // (frontier, signature) pair — the report
                                // signature binds digest_hash, so a
                                // conflicting pair is non-repudiable.
                                if let ObserveOutcome::Equivocation(ev) =
                                    state.equivocation.observe(&frontier, &sig, now_ms)
                                {
                                    warn_equivocation(&ev);
                                    state.metrics.record_equivocation_at(now_ms);
                                    state.metrics.set_accused_authorities(
                                        state.equivocation.accused_count(),
                                    );
                                    evidence_dirty = true;
                                }
                                // Optional exclusion from certificate assembly
                                // (detect-only by default): the frontier value
                                // itself still advances — it is a monotone max
                                // and thus low-poison — but the attestation is
                                // dropped so it cannot contribute to a
                                // certificate. The majority denominator is
                                // unchanged, so this only errs safe.
                                let att = if state.exclude_accused_authorities
                                    && state.equivocation.is_accused(&frontier.authority_id)
                                {
                                    None
                                } else {
                                    Some(att)
                                };
                                to_apply.push((frontier, att));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    authority = %frontier.authority_id.0,
                                    error = %e,
                                    "rejecting frontier with invalid signature"
                                );
                            }
                        },
                        None => {
                            if state.require_signed_frontiers {
                                tracing::warn!(
                                    authority = %frontier.authority_id.0,
                                    "rejecting unsigned frontier (require_signed_frontiers)"
                                );
                            } else {
                                tracing::debug!(
                                    authority = %frontier.authority_id.0,
                                    "accepting unsigned frontier (lenient mode)"
                                );
                                to_apply.push((frontier, None));
                            }
                        }
                    }
                }

                // Split-view lane (CT-gossip Protocol 2): attestations the
                // sender observed elsewhere, relayed for cross-checking.
                // Evidence only — never applied to frontier state. Each
                // relayed pair is re-verified against the registry before it
                // is allowed to become evidence, so a malicious relayer
                // cannot frame an honest authority; a failed verification is
                // *not* an accusation either, because the relayer of a
                // forged pair cannot be identified from the payload.
                for obs in observed.into_iter().take(MAX_OBSERVED_PER_REQUEST) {
                    if !is_range_authority(&obs.frontier) {
                        continue;
                    }
                    // Byte-equivalent echoes skip re-verification (CPU DoS
                    // mitigation): the exact (scope, hlc, digest) is already
                    // indexed and would compare Consistent anyway.
                    if state.equivocation.is_known_exact(&obs.frontier) {
                        continue;
                    }
                    match verify_frontier_signature(
                        &obs.frontier,
                        &obs.signature,
                        &registry,
                        current_epoch,
                        &state.epoch_config,
                    ) {
                        Ok(_) => {
                            state.metrics.record_split_view_observation();
                            if let ObserveOutcome::Equivocation(ev) =
                                state
                                    .equivocation
                                    .observe(&obs.frontier, &obs.signature, now_ms)
                            {
                                warn_equivocation(&ev);
                                state.metrics.record_equivocation_at(now_ms);
                                state
                                    .metrics
                                    .set_accused_authorities(state.equivocation.accused_count());
                                evidence_dirty = true;
                            }
                        }
                        Err(e) => {
                            tracing::debug!(
                                error = %e,
                                "ignoring relayed observation with invalid signature"
                            );
                        }
                    }
                }
            }
        }
    }

    // Persist new evidence after every sync guard is released; the write
    // itself happens on a blocking thread (never inside the detector lock)
    // and concurrent writers are serialized inside `spawn_persist` so an
    // older snapshot can never overwrite a newer one.
    if evidence_dirty {
        state.equivocation.spawn_persist();
    }

    let mut api = state.certified.lock().await;
    let mut accepted = 0;
    for (frontier, attestation) in to_apply {
        if api.update_frontier_verified(frontier, attestation) {
            accepted += 1;
        }
    }
    let resp = crate::network::frontier_sync::FrontierPushResponse { accepted };
    internal_response(&resp, accept)
}

/// Structured operator warning for a newly detected equivocation.
fn warn_equivocation(ev: &crate::authority::equivocation::EquivocationEvidence) {
    tracing::warn!(
        authority = %ev.authority_id.0,
        key_range = %ev.key_range.prefix,
        policy_version = ev.policy_version.0,
        frontier_hlc_physical = ev.frontier_hlc.physical,
        frontier_hlc_logical = ev.frontier_hlc.logical,
        digest_first = %ev.first.frontier.digest_hash,
        digest_second = %ev.second.frontier.digest_hash,
        "EQUIVOCATION DETECTED: authority signed conflicting frontier attestations; evidence stored"
    );
}

/// `GET /api/authority/equivocations`
///
/// Operator-facing read-only endpoint returning all recorded equivocation
/// evidence. Each evidence entry contains both conflicting signed
/// attestations verbatim (hex signatures included), so the response is a
/// third-party-verifiable proof of misbehaviour bundle.
pub async fn get_equivocations(State(state): State<Arc<AppState>>) -> Json<EquivocationReport> {
    let evidence = state.equivocation.evidence();
    Json(EquivocationReport {
        accused_authorities: state
            .equivocation
            .accused()
            .into_iter()
            .map(|n| n.0)
            .collect(),
        evidence_count: evidence.len(),
        evidence_overflow_total: state.equivocation.evidence_overflow_total(),
        evidence,
    })
}

/// `GET /api/internal/frontiers`
///
/// Returns all frontiers currently tracked by this node.
pub async fn get_internal_frontiers(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, super::codec::SerializationError> {
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());

    let api = state.certified.lock().await;
    let frontiers = api.all_frontiers().into_iter().cloned().collect();
    let resp = crate::network::frontier_sync::FrontierPullResponse { frontiers };
    internal_response(&resp, accept)
}

// ---------------------------------------------------------------
// Control-plane handlers
// ---------------------------------------------------------------

/// `GET /api/control-plane/authorities`
///
/// Returns all authority definitions from the system namespace.
pub async fn list_authorities(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<AuthorityDefinitionResponse>> {
    let ns = state.namespace.read().unwrap_or_else(|e| e.into_inner());
    let defs: Vec<AuthorityDefinitionResponse> = ns
        .all_authority_definitions()
        .into_iter()
        .map(|def| AuthorityDefinitionResponse {
            key_range_prefix: def.key_range.prefix.clone(),
            authority_nodes: def.authority_nodes.iter().map(|n| n.0.clone()).collect(),
        })
        .collect();
    Json(defs)
}

/// `GET /api/control-plane/authorities/{prefix}`
///
/// Returns the authority definition for the given key range prefix.
pub async fn get_authority_definition(
    State(state): State<Arc<AppState>>,
    Path(prefix): Path<String>,
) -> Result<Json<AuthorityDefinitionResponse>, ApiError> {
    let ns = state.namespace.read().unwrap_or_else(|e| e.into_inner());
    let def = ns.get_authority_definition(&prefix).ok_or_else(|| {
        ApiError(CrdtError::KeyNotFound(format!(
            "authority definition: {prefix}"
        )))
    })?;
    Ok(Json(AuthorityDefinitionResponse {
        key_range_prefix: def.key_range.prefix.clone(),
        authority_nodes: def.authority_nodes.iter().map(|n| n.0.clone()).collect(),
    }))
}

/// `PUT /api/control-plane/authorities`
///
/// Sets an authority definition in the system namespace.
/// Requires majority approval from authority nodes (FR-009).
pub async fn set_authority_definition(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SetAuthorityDefinitionRequest>,
) -> Result<Json<AuthorityDefinitionResponse>, ApiError> {
    let def = AuthorityDefinition {
        key_range: KeyRange {
            prefix: req.key_range_prefix.clone(),
        },
        authority_nodes: req
            .authority_nodes
            .iter()
            .map(|n| NodeId(n.clone()))
            .collect(),
        auto_generated: false,
    };
    let approvals: Vec<NodeId> = req.approvals.iter().map(|a| NodeId(a.clone())).collect();

    // Hold consensus lock across both validation and mutation to prevent
    // TOCTOU races where a concurrent request could invalidate the approval
    // between the check and the namespace write.
    {
        let consensus = state.consensus.lock().await;
        consensus.propose_authority_update(def.clone(), &approvals)?;
        let mut ns = state.namespace.write().unwrap_or_else(|e| e.into_inner());
        ns.set_authority_definition(def);
    }

    // Persist namespace after mutation.
    persist_namespace(&state).await;

    Ok(Json(AuthorityDefinitionResponse {
        key_range_prefix: req.key_range_prefix,
        authority_nodes: req.authority_nodes,
    }))
}

/// `GET /api/control-plane/policies`
///
/// Returns all placement policies from the system namespace.
pub async fn list_policies(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<PlacementPolicyResponse>> {
    let ns = state.namespace.read().unwrap_or_else(|e| e.into_inner());
    let policies: Vec<PlacementPolicyResponse> = ns
        .all_placement_policies()
        .into_iter()
        .map(|p| PlacementPolicyResponse {
            key_range_prefix: p.key_range.prefix.clone(),
            version: p.version.0,
            replica_count: p.replica_count,
            required_tags: p.required_tags.iter().map(|t| t.0.clone()).collect(),
            forbidden_tags: p.forbidden_tags.iter().map(|t| t.0.clone()).collect(),
            allow_local_write_on_partition: p.allow_local_write_on_partition,
            certified: p.certified,
        })
        .collect();
    Json(policies)
}

/// `GET /api/control-plane/policies/{prefix}`
///
/// Returns the placement policy for the given key range prefix.
pub async fn get_policy(
    State(state): State<Arc<AppState>>,
    Path(prefix): Path<String>,
) -> Result<Json<PlacementPolicyResponse>, ApiError> {
    let ns = state.namespace.read().unwrap_or_else(|e| e.into_inner());
    let p = ns.get_placement_policy(&prefix).ok_or_else(|| {
        ApiError(CrdtError::KeyNotFound(format!(
            "placement policy: {prefix}"
        )))
    })?;
    Ok(Json(PlacementPolicyResponse {
        key_range_prefix: p.key_range.prefix.clone(),
        version: p.version.0,
        replica_count: p.replica_count,
        required_tags: p.required_tags.iter().map(|t| t.0.clone()).collect(),
        forbidden_tags: p.forbidden_tags.iter().map(|t| t.0.clone()).collect(),
        allow_local_write_on_partition: p.allow_local_write_on_partition,
        certified: p.certified,
    }))
}

/// `PUT /api/control-plane/policies`
///
/// Sets a placement policy in the system namespace.
/// Requires majority approval from authority nodes (FR-009).
pub async fn set_placement_policy(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SetPlacementPolicyRequest>,
) -> Result<Json<PlacementPolicyResponse>, ApiError> {
    if req.replica_count < 1 {
        return Err(ApiError(CrdtError::InvalidArgument(
            "replica_count must be at least 1".to_string(),
        )));
    }

    let approvals: Vec<NodeId> = req.approvals.iter().map(|a| NodeId(a.clone())).collect();

    // Build the policy template without a version; the actual version is
    // assigned atomically inside the namespace write lock below to prevent
    // concurrent requests from stamping different policies with the same
    // version number.
    let build_policy = |version: PolicyVersion| {
        let mut policy = PlacementPolicy::new(
            version,
            KeyRange {
                prefix: req.key_range_prefix.clone(),
            },
            req.replica_count,
        );

        if !req.required_tags.is_empty() {
            policy = policy.with_required_tags(
                req.required_tags
                    .iter()
                    .map(|t| crate::types::Tag(t.clone()))
                    .collect(),
            );
        }
        if !req.forbidden_tags.is_empty() {
            policy = policy.with_forbidden_tags(
                req.forbidden_tags
                    .iter()
                    .map(|t| crate::types::Tag(t.clone()))
                    .collect(),
            );
        }
        policy = policy.with_local_write_on_partition(req.allow_local_write_on_partition);
        policy = policy.with_certified(req.certified);
        policy
    };

    // Hold consensus lock across both validation and mutation to prevent
    // TOCTOU races where a concurrent request could invalidate the approval
    // between the check and the namespace write.
    let policy = {
        let provisional = build_policy(PolicyVersion(0));
        let consensus = state.consensus.lock().await;
        consensus.propose_policy_update(provisional, &approvals)?;

        // Atomically read the current version, create the policy, and apply it
        // inside a single write-lock scope to prevent version collisions.
        let mut ns = state.namespace.write().unwrap_or_else(|e| e.into_inner());
        let current_version = ns.version().0;
        let policy = build_policy(PolicyVersion(current_version + 1));
        ns.set_placement_policy(policy.clone()).map_err(ApiError)?;
        policy
    };

    // Persist namespace after mutation.
    persist_namespace(&state).await;

    let resp = PlacementPolicyResponse {
        key_range_prefix: policy.key_range.prefix.clone(),
        version: policy.version.0,
        replica_count: policy.replica_count,
        required_tags: req.required_tags,
        forbidden_tags: req.forbidden_tags,
        allow_local_write_on_partition: policy.allow_local_write_on_partition,
        certified: policy.certified,
    };

    Ok(Json(resp))
}

/// `DELETE /api/control-plane/policies/{prefix}`
///
/// Removes the placement policy for the given key range prefix.
/// Requires majority approval from authority nodes (FR-009).
pub async fn remove_policy(
    State(state): State<Arc<AppState>>,
    Path(prefix): Path<String>,
    Json(req): Json<RemovePolicyRequest>,
) -> Result<Json<PlacementPolicyResponse>, ApiError> {
    let approvals: Vec<NodeId> = req.approvals.iter().map(|a| NodeId(a.clone())).collect();

    // Hold consensus lock across both validation and mutation to prevent
    // TOCTOU races where a concurrent request could invalidate the approval
    // between the check and the namespace write.
    let removed = {
        let consensus = state.consensus.lock().await;
        consensus.propose_policy_removal(&prefix, &approvals)?;
        let mut ns = state.namespace.write().unwrap_or_else(|e| e.into_inner());
        ns.remove_placement_policy(&prefix)
    };

    let removed = removed.ok_or_else(|| {
        ApiError(CrdtError::KeyNotFound(format!(
            "placement policy: {prefix}"
        )))
    })?;

    // Persist namespace after mutation.
    persist_namespace(&state).await;

    Ok(Json(PlacementPolicyResponse {
        key_range_prefix: removed.key_range.prefix.clone(),
        version: removed.version.0,
        replica_count: removed.replica_count,
        required_tags: removed.required_tags.iter().map(|t| t.0.clone()).collect(),
        forbidden_tags: removed.forbidden_tags.iter().map(|t| t.0.clone()).collect(),
        allow_local_write_on_partition: removed.allow_local_write_on_partition,
        certified: removed.certified,
    }))
}

/// `GET /api/control-plane/versions`
///
/// Returns the version history of the system namespace.
pub async fn get_version_history(
    State(state): State<Arc<AppState>>,
) -> Json<VersionHistoryResponse> {
    let ns = state.namespace.read().unwrap_or_else(|e| e.into_inner());
    Json(VersionHistoryResponse {
        current_version: ns.version().0,
        history: ns.version_history().iter().map(|v| v.0).collect(),
    })
}

// ---------------------------------------------------------------
// Verification handler
// ---------------------------------------------------------------

/// `POST /api/certified/verify`
///
/// Accepts a proof bundle and returns the verification result.
/// External clients can use this to independently verify that a
/// proof bundle represents genuine Authority consensus.
///
/// The majority denominator (`total_authorities`) and the eligible signer
/// set are always derived from this node's authority definition for the
/// proof's key range; the caller-supplied `total_authorities` field is
/// ignored so the quorum cannot be understated by the requester.
pub async fn verify_proof(
    State(state): State<Arc<AppState>>,
    Json(req): Json<VerifyProofRequest>,
) -> Result<Json<VerifyProofResponse>, (axum::http::StatusCode, String)> {
    use crate::api::certified::ProofBundle;
    use crate::authority::certificate::{AuthoritySignature, KeysetVersion, MajorityCertificate};
    use crate::authority::verifier;
    use crate::hlc::HlcTimestamp;
    use crate::types::{KeyRange, NodeId, PolicyVersion};

    // Registry-based verification is required; reject if no registry is configured.
    let registry_lock = state.keyset_registry.as_ref().ok_or((
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "keyset registry not configured; cannot verify proofs".to_string(),
    ))?;

    let key_range = KeyRange {
        prefix: req.key_range_prefix,
    };
    let frontier_hlc = HlcTimestamp {
        physical: req.frontier.physical,
        logical: req.frontier.logical,
        node_id: req.frontier.node_id,
    };
    let policy_version = PolicyVersion(req.policy_version);

    // The majority denominator and the eligible signer set come from this
    // node's own authority definition for the key range. Trusting the
    // caller-supplied `total_authorities` would let an attacker shrink the
    // denominator and pass a sub-quorum proof (e.g. 2-of-5 presented as
    // 2-of-3) off as a certified majority.
    let (total_authorities, authority_members) = {
        let ns = state.namespace.read().unwrap_or_else(|e| e.into_inner());
        let Some(def) = ns.get_authorities_for_key(&key_range.prefix) else {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                format!(
                    "no authority definition covers key range '{}'; cannot determine the authority set",
                    key_range.prefix
                ),
            ));
        };
        (
            def.authority_nodes.len(),
            def.authority_nodes
                .iter()
                .cloned()
                .collect::<std::collections::HashSet<NodeId>>(),
        )
    };

    // Determine the signature algorithm from the request.
    let sig_algorithm = match req.signature_algorithm.as_deref() {
        Some("Bls12_381") => crate::authority::certificate::SignatureAlgorithm::Bls12_381,
        _ => crate::authority::certificate::SignatureAlgorithm::Ed25519,
    };

    // BLS aggregate verification path: reconstruct a DualModeCertificate
    // from the dedicated BLS fields and verify against the registry.
    if sig_algorithm == crate::authority::certificate::SignatureAlgorithm::Bls12_381
        && let (Some(agg_hex), Some(signer_ids), Some(pk_hexes)) = (
            &req.bls_aggregate_signature,
            &req.bls_signer_ids,
            &req.bls_public_keys,
        )
    {
        use crate::authority::certificate::DualModeCertificate;

        if signer_ids.len() != pk_hexes.len() {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "bls_signer_ids and bls_public_keys must have the same length".to_string(),
            ));
        }

        let keyset_version = KeysetVersion(
            req.keyset_version
                .or(req.certificate.as_ref().map(|c| c.keyset_version))
                .unwrap_or(1),
        );

        let mut cert = DualModeCertificate::new_bls(
            key_range.clone(),
            frontier_hlc.clone(),
            policy_version,
            keyset_version,
        );
        if let Some(fv) = req.format_version {
            cert.format_version = fv;
        }

        let aggregated = BlsSignature::from_hex(agg_hex).ok_or((
            axum::http::StatusCode::BAD_REQUEST,
            "invalid hex in bls_aggregate_signature".to_string(),
        ))?;
        let mut signers = Vec::with_capacity(signer_ids.len());
        for (id, pk_hex) in signer_ids.iter().zip(pk_hexes.iter()) {
            let pk = BlsPublicKey::from_hex(pk_hex).ok_or((
                axum::http::StatusCode::BAD_REQUEST,
                format!("invalid hex in bls_public_keys for signer {id}"),
            ))?;
            signers.push((NodeId(id.clone()), pk));
        }
        // Every aggregate signer must belong to the range's authority set;
        // an aggregate cannot be partially discounted, so any outside signer
        // invalidates the proof as a whole.
        if signers
            .iter()
            .any(|(id, _)| !authority_members.contains(id))
        {
            return Ok(Json(VerifyProofResponse {
                valid: false,
                has_majority: false,
                contributing_count: signers
                    .iter()
                    .filter(|(id, _)| authority_members.contains(id))
                    .count(),
                required_count: total_authorities / 2 + 1,
            }));
        }
        cert.set_bls_aggregate(signers, aggregated);

        let format_config = req
            .format_version
            .map(|_| crate::authority::certificate::FormatVersionConfig::default());
        let registry = registry_lock.read().unwrap_or_else(|e| e.into_inner());
        let current_epoch = state
            .current_epoch
            .load(std::sync::atomic::Ordering::Relaxed);
        let result = verifier::verify_dual_proof_with_registry(
            &cert,
            total_authorities,
            &registry,
            current_epoch,
            &state.epoch_config,
            format_config.as_ref(),
            0,
        );

        return Ok(Json(VerifyProofResponse {
            valid: result.valid,
            has_majority: result.has_majority,
            contributing_count: result.contributing_count,
            required_count: result.required_count,
        }));
    }

    // Reconstruct the certificate from the HTTP payload, if provided.
    // Apply caller-specified format_version and signature_algorithm so that
    // BLS certificates and non-default format versions can be verified.
    let certificate = req.certificate.and_then(|cert_json| {
        let mut cert = MajorityCertificate::new(
            key_range.clone(),
            frontier_hlc.clone(),
            policy_version,
            KeysetVersion(cert_json.keyset_version),
        );
        if let Some(fv) = req.format_version {
            cert.format_version = fv;
        }
        cert.signature_algorithm = sig_algorithm;

        for sig_json in &cert_json.signatures {
            // Signatures from authorities outside the range's authority set
            // do not count toward the majority, however valid they are.
            if !authority_members.contains(&NodeId(sig_json.authority_id.clone())) {
                continue;
            }
            let pk_bytes = hex_to_bytes_32(&sig_json.public_key)?;
            let sig_bytes = hex_to_bytes_64(&sig_json.signature)?;
            let public_key = ed25519_dalek::VerifyingKey::from_bytes(&pk_bytes).ok()?;
            let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);
            cert.add_signature(AuthoritySignature {
                authority_id: NodeId(sig_json.authority_id.clone()),
                public_key,
                signature,
                keyset_version: KeysetVersion(sig_json.keyset_version),
            });
        }
        Some(cert)
    });

    // Build format config when the caller provides a format_version so that
    // the verifier performs version compatibility checks.
    let format_config = req
        .format_version
        .map(|_| crate::authority::certificate::FormatVersionConfig::default());

    let bundle = ProofBundle {
        key_range,
        frontier_hlc,
        policy_version,
        contributing_authorities: req
            .contributing_authorities
            .into_iter()
            .map(NodeId)
            .filter(|id| authority_members.contains(id))
            .collect(),
        total_authorities,
        certificate,
        bls_certificate: None,
    };

    let registry = registry_lock.read().unwrap_or_else(|e| e.into_inner());
    let current_epoch = state
        .current_epoch
        .load(std::sync::atomic::Ordering::Relaxed);
    let result = verifier::verify_proof_with_registry(
        &bundle,
        &registry,
        current_epoch,
        &state.epoch_config,
        format_config.as_ref(),
        0,
    );

    Ok(Json(VerifyProofResponse {
        valid: result.valid,
        has_majority: result.has_majority,
        contributing_count: result.contributing_count,
        required_count: result.required_count,
    }))
}

/// Decode a hex string into a 32-byte array. Returns `None` on failure.
fn hex_to_bytes_32(hex: &str) -> Option<[u8; 32]> {
    let bytes = hex_to_bytes(hex)?;
    bytes.try_into().ok()
}

/// Decode a hex string into a 64-byte array. Returns `None` on failure.
fn hex_to_bytes_64(hex: &str) -> Option<[u8; 64]> {
    let bytes = hex_to_bytes(hex)?;
    bytes.try_into().ok()
}

/// Decode a hex string into a byte vector. Returns `None` on failure.
fn hex_to_bytes(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

// ---------------------------------------------------------------
// Internal sync handlers
// ---------------------------------------------------------------

/// `POST /api/internal/sync`
///
/// Receives CRDT values from a remote peer and merges them into the
/// local eventual store using `merge_remote`.
pub async fn internal_sync(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, super::codec::SerializationError> {
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());

    let req: SyncRequest = deserialize_internal(&body, content_type)?;

    let mut api = state.eventual.lock().await;
    let mut merged = 0;
    let mut errors = Vec::new();

    for (key, value) in &req.entries {
        match api.merge_remote(key.clone(), value) {
            Ok(()) => merged += 1,
            Err(e) => {
                errors.push(SyncError {
                    key: key.clone(),
                    error: e.to_string(),
                });
            }
        }
    }
    let wal_pos = api.last_wal_pos();
    drop(api);

    // One durability wait for the whole batch (SyncPolicy::Always): the
    // pushing peer advances its push frontier on our ack, so acking
    // un-synced merges could strand them on a crash. A wait error is only
    // possible when the flusher is gone (which fail-stops the process on
    // fsync errors), so it is logged rather than surfaced.
    if let Err(e) = wait_wal_durable(&state.eventual_wal, wal_pos).await {
        tracing::warn!(error = ?e.0, "internal sync WAL durability wait failed");
    }

    let resp = SyncResponse { merged, errors };
    internal_response(&resp, accept)
}

/// `GET /api/internal/keys`
///
/// Returns all key-value pairs from the eventual store together with the
/// store's current frontier HLC. Used by remote peers for pull-based
/// anti-entropy sync. The frontier allows the requester to correctly
/// initialise its peer frontier tracking after a full sync.
pub async fn internal_keys(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, super::codec::SerializationError> {
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());

    let api = state.eventual.lock().await;
    let store = api.store();
    let mut entries = std::collections::HashMap::new();
    let mut timestamps = std::collections::HashMap::new();
    for (k, v, hlc) in store.all_entries_with_hlc() {
        entries.insert(k.clone(), v.clone());
        timestamps.insert(k.clone(), hlc.clone());
    }
    // Include entries without tracked timestamps (rare, but possible for
    // stores migrated from older versions). Only iterate entries not already
    // covered by `all_entries_with_hlc()` to avoid redundant cloning.
    for (k, v) in store.all_entries() {
        if !entries.contains_key(k) {
            entries.insert(k.clone(), v.clone());
        }
    }
    let frontier = store.current_frontier();
    // Session-guarantee metadata must be snapshotted in the same lock
    // scope as `entries`/`frontier`: adoption on the receiving side is
    // only sound when the applied frontier describes exactly this state.
    let applied_origins = store.applied_origins().clone();
    let merge_failed_keys: Vec<String> = store.merge_failed_keys().iter().cloned().collect();
    let visible_origins = store.visible_origins().clone();

    let resp = KeyDumpResponse {
        entries,
        frontier,
        timestamps,
        applied_origins,
        merge_failed_keys,
        visible_origins,
    };
    internal_response(&resp, accept)
}

/// `POST /api/internal/sync/delta`
///
/// Receives a delta sync request with a frontier timestamp and returns
/// all entries modified after that frontier. Used for incremental
/// anti-entropy sync.
pub async fn internal_delta_sync(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, super::codec::SerializationError> {
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());

    let req: DeltaSyncRequest = deserialize_internal(&body, content_type)?;

    let api = state.eventual.lock().await;
    let store = api.store();

    let entries: Vec<DeltaEntry> = store
        .delta_entries_since(&req.frontier)
        .into_iter()
        .map(|(key, value, hlc)| DeltaEntry { key, value, hlc })
        .collect();

    let sender_frontier = store.current_frontier();
    // Snapshot the session-guarantee metadata in the same lock scope as
    // the delta entries (see internal_keys). `pruned_floor` lets the
    // receiver decide whether adopting `applied_origins` is sound.
    let applied_origins = store.applied_origins().clone();
    let merge_failed_keys: Vec<String> = store.merge_failed_keys().iter().cloned().collect();
    let pruned_floor = store.pruned_floor().cloned();
    let visible_origins = store.visible_origins().clone();

    let resp = DeltaSyncResponse {
        entries,
        sender_frontier,
        applied_origins,
        merge_failed_keys,
        pruned_floor,
        visible_origins,
    };
    internal_response(&resp, accept)
}

// ---------------------------------------------------------------
// Internal join/leave handlers
// ---------------------------------------------------------------

/// `POST /api/internal/join`
///
/// A new node sends its configuration to this (seed) node to join the
/// cluster. The seed node adds the joining node to its peer registry
/// and returns the current peer list plus a system namespace snapshot.
pub async fn internal_join(
    State(state): State<Arc<AppState>>,
    Json(req): Json<JoinRequest>,
) -> Result<Json<JoinResponse>, ApiError> {
    use crate::network::PeerConfig;

    // Validate the caller-supplied address format to prevent SSRF.
    validate_peer_address(&req.address).map_err(|e| ApiError(CrdtError::InvalidArgument(e)))?;
    // Reject cloud metadata / link-local targets even if format is valid.
    if is_metadata_or_link_local(&req.address) {
        return Err(ApiError(CrdtError::InvalidArgument(format!(
            "peer address is a reserved link-local endpoint: {}",
            req.address
        ))));
    }

    let peers_registry = state.peers.as_ref().ok_or_else(|| {
        ApiError(CrdtError::Internal(
            "peer registry not configured".to_string(),
        ))
    })?;

    let joining_node_id = NodeId(req.node_id.clone());

    // Add the joining node (or update its address if already known) and
    // snapshot peer list under one lock acquisition, then release the lock
    // before performing blocking I/O.
    let (peer_list, persist_snapshot) = {
        let mut registry = peers_registry.lock().await;

        // If the peer already exists, update its address in case it restarted
        // with a new IP.
        if registry.get_peer(&joining_node_id).is_some() {
            registry.update_address(&joining_node_id, &req.address);
        } else {
            registry
                .add_peer(PeerConfig {
                    node_id: joining_node_id.clone(),
                    addr: req.address.clone(),
                })
                .map_err(|e| ApiError(CrdtError::InvalidArgument(e.to_string())))?;
        }

        // Snapshot the serialised state while we hold the lock.
        let snapshot = state
            .peer_persist_path
            .as_ref()
            .and_then(|_| serde_json::to_string_pretty(&*registry).ok());

        // Build the peer list response while the lock is held.
        let mut list: Vec<PeerInfo> = registry
            .all_peers_owned()
            .into_iter()
            .map(|p| PeerInfo {
                node_id: p.node_id.0,
                address: p.addr.clone(),
            })
            .collect();

        // Include the seed node (self) so the joiner has a complete peer view.
        if let (Some(self_id), Some(self_addr)) = (&state.self_node_id, &state.self_addr) {
            let already_present = list.iter().any(|p| p.node_id == self_id.0);
            if !already_present {
                list.push(PeerInfo {
                    node_id: self_id.0.clone(),
                    address: self_addr.clone(),
                });
            }
        }

        list.sort_by(|a, b| a.node_id.cmp(&b.node_id));

        (list, snapshot)
    };
    // Lock is released here — persist outside the critical section.

    if let Some(path) = &state.peer_persist_path
        && let Some(json) = persist_snapshot
    {
        let path = path.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || write_atomic(&path, json.as_bytes()))
            .await
            .unwrap_or_else(|e| Err(e.to_string()))
        {
            eprintln!("warning: failed to persist peer registry: {e}");
        }
    }

    let ns_snapshot = {
        let ns = state.namespace.read().unwrap_or_else(|e| e.into_inner());
        serde_json::to_value(&*ns).unwrap_or(serde_json::Value::Null)
    };

    Ok(Json(JoinResponse {
        peers: peer_list,
        namespace: ns_snapshot,
    }))
}

/// `POST /api/internal/leave`
///
/// A node sends its ID to gracefully depart the cluster. The receiving
/// node removes the departing node from its peer registry.
pub async fn internal_leave(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LeaveRequest>,
) -> Result<Json<LeaveResponse>, ApiError> {
    let peers_registry = state.peers.as_ref().ok_or_else(|| {
        ApiError(CrdtError::Internal(
            "peer registry not configured".to_string(),
        ))
    })?;

    let leaving_node_id = NodeId(req.node_id);

    // Remove peer and snapshot serialised state under the lock, then
    // release before performing blocking I/O.
    let (removed, persist_snapshot) = {
        let mut registry = peers_registry.lock().await;
        let removed = registry
            .remove_peer(&leaving_node_id)
            .map_err(|e| ApiError(CrdtError::InvalidArgument(e.to_string())))?;

        let snapshot = if removed.is_some() {
            state
                .peer_persist_path
                .as_ref()
                .and_then(|_| serde_json::to_string_pretty(&*registry).ok())
        } else {
            None
        };

        (removed, snapshot)
    };
    // Lock is released here.

    if let Some(path) = &state.peer_persist_path
        && let Some(json) = persist_snapshot
    {
        let path = path.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || write_atomic(&path, json.as_bytes()))
            .await
            .unwrap_or_else(|e| Err(e.to_string()))
        {
            eprintln!("warning: failed to persist peer registry: {e}");
        }
    }

    Ok(Json(LeaveResponse {
        success: removed.is_some(),
    }))
}

// ---------------------------------------------------------------
// Internal announce handler
// ---------------------------------------------------------------

/// `POST /api/internal/announce`
///
/// Receives a membership announcement from a peer. If the peer is
/// joining, it is added to the local peer registry. If leaving, it
/// is removed.
pub async fn internal_announce(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AnnounceRequest>,
) -> Result<Json<AnnounceResponse>, ApiError> {
    use crate::network::PeerConfig;

    // Validate the caller-supplied address format to prevent SSRF.
    validate_peer_address(&req.address).map_err(|e| ApiError(CrdtError::InvalidArgument(e)))?;
    // Reject cloud metadata / link-local targets even if format is valid.
    if is_metadata_or_link_local(&req.address) {
        return Err(ApiError(CrdtError::InvalidArgument(format!(
            "peer address is a reserved link-local endpoint: {}",
            req.address
        ))));
    }

    let peers_registry = state.peers.as_ref().ok_or_else(|| {
        ApiError(CrdtError::Internal(
            "peer registry not configured".to_string(),
        ))
    })?;

    let announcing_node_id = NodeId(req.node_id.clone());

    let (result, persist_snapshot) = if req.joining {
        let mut registry = peers_registry.lock().await;
        // If the peer is already known, update its address in case it
        // restarted with a new IP, then return success.
        if registry.get_peer(&announcing_node_id).is_some() {
            registry.update_address(&announcing_node_id, &req.address);
            let snapshot = state
                .peer_persist_path
                .as_ref()
                .and_then(|_| serde_json::to_string_pretty(&*registry).ok());
            drop(registry);
            // Persist outside the lock if the address changed.
            if let Some(path) = &state.peer_persist_path
                && let Some(json) = snapshot
            {
                let path = path.clone();
                let _ =
                    tokio::task::spawn_blocking(move || write_atomic(&path, json.as_bytes())).await;
            }
            return Ok(Json(AnnounceResponse { accepted: true }));
        }
        let result = match registry.add_peer(PeerConfig {
            node_id: announcing_node_id,
            addr: req.address,
        }) {
            Ok(()) => Ok(Json(AnnounceResponse { accepted: true })),
            // If it is our own ID, silently accept.
            Err(crate::network::PeerError::SelfInPeerList(_)) => {
                Ok(Json(AnnounceResponse { accepted: true }))
            }
            Err(e) => Err(ApiError(CrdtError::InvalidArgument(e.to_string()))),
        };
        let snapshot = state
            .peer_persist_path
            .as_ref()
            .and_then(|_| serde_json::to_string_pretty(&*registry).ok());
        (result, snapshot)
    } else {
        let mut registry = peers_registry.lock().await;
        let result = match registry.remove_peer(&announcing_node_id) {
            Ok(_) => Ok(Json(AnnounceResponse { accepted: true })),
            Err(crate::network::PeerError::SelfInPeerList(_)) => {
                Ok(Json(AnnounceResponse { accepted: true }))
            }
            Err(e) => Err(ApiError(CrdtError::InvalidArgument(e.to_string()))),
        };
        let snapshot = state
            .peer_persist_path
            .as_ref()
            .and_then(|_| serde_json::to_string_pretty(&*registry).ok());
        (result, snapshot)
    };

    // Persist outside the lock.
    if let Some(path) = &state.peer_persist_path
        && let Some(json) = persist_snapshot
    {
        let path = path.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || write_atomic(&path, json.as_bytes()))
            .await
            .unwrap_or_else(|e| Err(e.to_string()))
        {
            eprintln!("warning: failed to persist peer registry after announce: {e}");
        }
    }

    result
}

// ---------------------------------------------------------------
// Internal ping handler
// ---------------------------------------------------------------

/// `POST /api/internal/ping`
///
/// Lightweight gossip endpoint for peer list exchange. The sender
/// provides its known peers; the receiver reconciles and returns
/// its own known peers. Both sides can detect and fill gaps.
pub async fn internal_ping(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PingRequest>,
) -> Result<Json<PingResponse>, ApiError> {
    use crate::network::PeerConfig;

    // Validate the sender's address format.
    validate_peer_address(&req.sender_addr).map_err(|e| ApiError(CrdtError::InvalidArgument(e)))?;

    let peers_registry = state.peers.as_ref().ok_or_else(|| {
        ApiError(CrdtError::Internal(
            "peer registry not configured".to_string(),
        ))
    })?;

    // Maximum number of new peers that can be added from a single ping
    // exchange to limit peer-list poisoning.
    const MAX_NEW_PEERS_PER_PING: usize = 10;

    // Only reconcile peers from the sender's list if the sender is already
    // a known peer (or is self). This prevents unauthenticated nodes from
    // injecting arbitrary peers via ping.
    let sender_nid = NodeId(req.sender_id.clone());
    let peers_changed = {
        let mut registry = peers_registry.lock().await;
        let mut changed = false;

        let sender_is_known = registry.get_peer(&sender_nid).is_some()
            || state.self_node_id.as_ref() == Some(&sender_nid);

        // Update or add the sender. Only add an unknown sender if the
        // request passed bearer-token authentication (i.e. internal_token
        // is configured and was validated by the middleware). Without auth,
        // unknown nodes must not be able to inject themselves into the
        // registry via a bare ping.
        // Apply the SSRF guard before writing sender_addr into the registry.
        if registry.get_peer(&sender_nid).is_some() {
            if is_safe_peer_address(&req.sender_addr)
                && registry.update_address(&sender_nid, &req.sender_addr)
            {
                changed = true;
            }
        } else if state.internal_token.as_ref().is_some_and(|t| !t.is_empty()) {
            if is_safe_peer_address(&req.sender_addr)
                && registry
                    .add_peer(PeerConfig {
                        node_id: sender_nid.clone(),
                        addr: req.sender_addr.clone(),
                    })
                    .is_ok()
            {
                changed = true;
            }
        } else {
            tracing::warn!(
                sender = %req.sender_id,
                "ping from unknown sender rejected: no auth configured"
            );
        }

        // Only accept peer list from known senders.
        if sender_is_known {
            let mut new_peers_added: usize = 0;
            for peer_info in &req.known_peers {
                // Validate address format first.
                if validate_peer_address(&peer_info.address).is_err() {
                    continue;
                }
                let peer_nid = NodeId(peer_info.node_id.clone());
                // Determine whether the address is IP-format (vs. hostname).
                // Docker deployments often use hostname addresses (e.g.
                // "asteroidb-node-2:3000") loaded from config files. Those
                // hostnames are safe within Docker's network but is_safe_peer_address
                // cannot distinguish them from cloud-metadata hostnames. For IP
                // addresses the SSRF check is always applied; for hostnames of
                // EXISTING peers we allow updates (the peer was already trusted
                // when it was added). New peer additions always require the full
                // SSRF check regardless of address format.
                let is_ip_addr = peer_info.address.parse::<std::net::SocketAddr>().is_ok();
                if registry.get_peer(&peer_nid).is_some() {
                    // Existing peer: block only if address is an IP that fails SSRF.
                    // Hostname addresses for existing peers are passed through to
                    // preserve gossip-based address refresh in Docker deployments.
                    if is_ip_addr && !is_safe_peer_address(&peer_info.address) {
                        continue;
                    }
                    if registry.update_address(&peer_nid, &peer_info.address) {
                        changed = true;
                    }
                } else if new_peers_added < MAX_NEW_PEERS_PER_PING {
                    // New peer: full SSRF check to prevent injecting unknown endpoints.
                    if !is_safe_peer_address(&peer_info.address) {
                        continue;
                    }
                    // Ignore errors (e.g. self-in-peer-list, duplicates).
                    if registry
                        .add_peer(PeerConfig {
                            node_id: peer_nid,
                            addr: peer_info.address.clone(),
                        })
                        .is_ok()
                    {
                        changed = true;
                        new_peers_added += 1;
                    }
                }
            }
        }

        changed
    };

    // Persist registry if it changed (Codex P2).
    if peers_changed && let Some(path) = &state.peer_persist_path {
        let snapshot = {
            let registry = peers_registry.lock().await;
            serde_json::to_string_pretty(&*registry).ok()
        };
        if let Some(json) = snapshot {
            let path = path.clone();
            if let Err(e) =
                tokio::task::spawn_blocking(move || write_atomic(&path, json.as_bytes()))
                    .await
                    .unwrap_or_else(|e| Err(e.to_string()))
            {
                eprintln!("warning: failed to persist peer registry after ping: {e}");
            }
        }
    }

    // Build our own peer list to return.
    let my_peers = {
        let registry = peers_registry.lock().await;
        let mut list: Vec<PeerInfo> = registry
            .all_peers_owned()
            .into_iter()
            .map(|p| PeerInfo {
                node_id: p.node_id.0,
                address: p.addr,
            })
            .collect();

        // Include self so the remote gets a complete view.
        if let (Some(self_id), Some(self_addr)) = (&state.self_node_id, &state.self_addr) {
            let already_present = list.iter().any(|p| p.node_id == self_id.0);
            if !already_present {
                list.push(PeerInfo {
                    node_id: self_id.0.clone(),
                    address: self_addr.clone(),
                });
            }
        }

        list.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        list
    };

    Ok(Json(PingResponse {
        known_peers: my_peers,
    }))
}

// ---------------------------------------------------------------
// Topology handler
// ---------------------------------------------------------------

/// `GET /api/topology`
///
/// Returns the cluster topology view grouped by region, including
/// inter-region latency information.
pub async fn get_topology(State(state): State<Arc<AppState>>) -> Json<TopologyView> {
    let nodes = state
        .cluster_nodes
        .as_ref()
        .map(|n| n.read().unwrap_or_else(|e| e.into_inner()).clone())
        .unwrap_or_default();

    let latency_model = state
        .latency_model
        .as_ref()
        .map(|m| m.read().unwrap_or_else(|e| e.into_inner()).clone())
        .unwrap_or_default();

    Json(TopologyView::build(&nodes, &latency_model))
}

// ---------------------------------------------------------------
// Metrics handler
// ---------------------------------------------------------------

/// `GET /api/metrics`
///
/// Returns a snapshot of runtime operational metrics (pending count,
/// certification latency, frontier skew, sync failure rate).
pub async fn get_metrics(State(state): State<Arc<AppState>>) -> Json<MetricsSnapshot> {
    Json(state.metrics.snapshot())
}

/// `GET /api/slo`
///
/// Returns a snapshot of all SLO budgets for operational monitoring.
pub async fn get_slo(State(state): State<Arc<AppState>>) -> Json<SloSnapshot> {
    Json(state.slo_tracker.snapshot())
}

/// `GET /healthz`
///
/// Simple health check endpoint for load balancers and orchestrators.
/// Returns 200 OK with a static JSON body. Placed outside auth middleware
/// so that unauthenticated probes succeed.
pub async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

/// Validate that `addr` is a bare host:port address.
///
/// Rejects addresses containing schemes (`http://`, `ftp://`), paths, or query
/// strings. Only `host:port` or `ip:port` format is accepted. This prevents
/// SSRF via caller-supplied addresses in join/leave/announce/ping requests.
fn validate_peer_address(addr: &str) -> Result<(), String> {
    // Must not contain scheme indicators.
    if addr.contains("://") {
        return Err(format!("address must not contain a scheme: {addr}"));
    }
    // Must not contain userinfo (the '@' character is used in RFC 3986 authority
    // as "userinfo@host". Allowing it lets an attacker craft addresses like
    // "attacker@169.254.169.254:80" where parse_host returns "attacker@169.254.169.254",
    // which fails IpAddr parsing (Err => safe), but reqwest resolves it as
    // host=169.254.169.254 — defeating all IP-level SSRF guards.
    if addr.contains('@') {
        return Err(format!("address must not contain '@': {addr}"));
    }
    // Must not contain path or query components.
    if addr.contains('/') || addr.contains('?') || addr.contains('#') {
        return Err(format!(
            "address must not contain path or query components: {addr}"
        ));
    }
    // Must contain at least one ':' separating host from port.
    // For IPv6, the port section follows the last colon after ']'.
    let port_sep = if addr.starts_with('[') {
        // IPv6 bracket notation: [::1]:3000
        addr.rfind("]:")
            .map(|i| i + 1)
            .ok_or_else(|| format!("invalid IPv6 bracket address: {addr}"))?
    } else {
        addr.rfind(':')
            .ok_or_else(|| format!("address must be host:port format: {addr}"))?
    };

    let port_str = &addr[port_sep + 1..];
    if port_str.is_empty() {
        return Err(format!("address must include a port number: {addr}"));
    }
    port_str
        .parse::<u16>()
        .map_err(|_| format!("invalid port number in address: {addr}"))?;

    // Host part must not be empty.
    let host = &addr[..port_sep];
    if host.is_empty() {
        return Err(format!("address must include a host: {addr}"));
    }
    // Must not contain whitespace.
    if addr.chars().any(|c| c.is_whitespace()) {
        return Err(format!("address must not contain whitespace: {addr}"));
    }

    Ok(())
}

/// Persist the system namespace to disk (if a path is configured).
///
/// Serialises the namespace under a read-lock, then writes atomically via
/// `write_atomic` on a blocking thread. Errors are logged but not propagated
/// because namespace persistence is best-effort.
async fn persist_namespace(state: &AppState) {
    if let Some(path) = &state.namespace_persist_path {
        let json = {
            let ns = state.namespace.read().unwrap_or_else(|e| e.into_inner());
            match serde_json::to_string_pretty(&*ns) {
                Ok(j) => j,
                Err(e) => {
                    eprintln!("warning: failed to serialise system namespace: {e}");
                    return;
                }
            }
        };
        let path = path.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || write_atomic(&path, json.as_bytes()))
            .await
            .unwrap_or_else(|e| Err(e.to_string()))
        {
            eprintln!("warning: failed to persist system namespace: {e}");
        }
    }
}

/// Maximum allowed absolute value for counter initialization via HTTP API.
///
/// Values exceeding this limit are rejected with `InvalidArgument` to prevent
/// resource exhaustion. This is a defense-in-depth measure; the O(1) constructor
/// already prevents CPU-based DoS.
const MAX_COUNTER_MAGNITUDE: i64 = 1_000_000_000;

/// Convert a JSON CRDT value representation into an internal `CrdtValue`.
///
/// For Counter, creates a PnCounter with the specified value using O(1)
/// `PnCounter::from_value` (not by looping). For Set/Map/Register,
/// constructs the appropriate CRDT type from the provided data.
fn json_to_crdt_value(json: &CrdtValueJson) -> Result<CrdtValue, CrdtError> {
    use crate::crdt::lww_register::LwwRegister;
    use crate::crdt::or_map::OrMap;
    use crate::crdt::or_set::OrSet;
    use crate::hlc::Hlc;
    use crate::types::NodeId;

    let writer = NodeId("http-writer".into());

    match json {
        CrdtValueJson::Counter { value } => {
            if value.unsigned_abs() > MAX_COUNTER_MAGNITUDE as u64 {
                return Err(CrdtError::InvalidArgument(format!(
                    "counter magnitude {} exceeds maximum allowed value {MAX_COUNTER_MAGNITUDE}",
                    value.unsigned_abs()
                )));
            }
            let counter = PnCounter::from_value(&writer, *value);
            Ok(CrdtValue::Counter(counter))
        }
        CrdtValueJson::Set { elements } => {
            let mut set = OrSet::new();
            for elem in elements {
                set.add(elem.clone(), &writer);
            }
            Ok(CrdtValue::Set(set))
        }
        CrdtValueJson::Map { entries } => {
            let mut map = OrMap::new();
            let mut clock = Hlc::new("http-writer".into());
            for (k, v) in entries {
                let ts = clock.now()?;
                map.set(k.clone(), v.clone(), ts, &writer);
            }
            Ok(CrdtValue::Map(map))
        }
        CrdtValueJson::Register { value } => {
            let mut reg = LwwRegister::new();
            if let Some(v) = value {
                let mut clock = Hlc::new("http-writer".into());
                let ts = clock.now()?;
                reg.set(v.clone(), ts);
            }
            Ok(CrdtValue::Register(reg))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_to_crdt_counter_positive() {
        let json = CrdtValueJson::Counter { value: 5 };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Counter(c) => assert_eq!(c.value(), 5),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_negative() {
        let json = CrdtValueJson::Counter { value: -3 };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Counter(c) => assert_eq!(c.value(), -3),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_zero() {
        let json = CrdtValueJson::Counter { value: 0 };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Counter(c) => assert_eq!(c.value(), 0),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_set() {
        let json = CrdtValueJson::Set {
            elements: vec!["a".into(), "b".into()],
        };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Set(s) => {
                assert!(s.contains(&"a".to_string()));
                assert!(s.contains(&"b".to_string()));
                assert_eq!(s.len(), 2);
            }
            other => panic!("expected Set, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_map() {
        let mut entries = std::collections::HashMap::new();
        entries.insert("name".into(), "db".into());
        let json = CrdtValueJson::Map { entries };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Map(m) => {
                assert_eq!(m.get(&"name".to_string()), Some(&"db".to_string()));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_register_with_value() {
        let json = CrdtValueJson::Register {
            value: Some("hello".into()),
        };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Register(r) => {
                assert_eq!(r.get(), Some(&"hello".to_string()));
            }
            other => panic!("expected Register, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_register_empty() {
        let json = CrdtValueJson::Register { value: None };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Register(r) => {
                assert_eq!(r.get(), None);
            }
            other => panic!("expected Register, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_large_value_is_o1() {
        // This would take O(999_999_999) iterations with the old loop-based
        // approach. With from_value it completes instantly.
        let json = CrdtValueJson::Counter { value: 999_999_999 };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Counter(c) => assert_eq!(c.value(), 999_999_999),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_large_negative_is_o1() {
        let json = CrdtValueJson::Counter {
            value: -999_999_999,
        };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Counter(c) => assert_eq!(c.value(), -999_999_999),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_exceeds_max_magnitude() {
        let json = CrdtValueJson::Counter {
            value: 1_000_000_001,
        };
        let err = json_to_crdt_value(&json).unwrap_err();
        match err {
            CrdtError::InvalidArgument(msg) => {
                assert!(msg.contains("exceeds maximum"), "unexpected message: {msg}");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_negative_exceeds_max_magnitude() {
        let json = CrdtValueJson::Counter {
            value: -1_000_000_001,
        };
        let err = json_to_crdt_value(&json).unwrap_err();
        match err {
            CrdtError::InvalidArgument(msg) => {
                assert!(msg.contains("exceeds maximum"), "unexpected message: {msg}");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn json_to_crdt_counter_at_max_magnitude_is_ok() {
        // Exactly at the boundary should succeed.
        let json = CrdtValueJson::Counter {
            value: 1_000_000_000,
        };
        let val = json_to_crdt_value(&json).unwrap();
        match val {
            CrdtValue::Counter(c) => assert_eq!(c.value(), 1_000_000_000),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    // ---------------------------------------------------------------
    // validate_peer_address tests
    // ---------------------------------------------------------------

    #[test]
    fn validate_peer_address_accepts_docker_hostnames_with_hyphens() {
        // Docker container names use hyphens (e.g. asteroidb-node-2:3000).
        assert!(validate_peer_address("asteroidb-node-1:3000").is_ok());
        assert!(validate_peer_address("asteroidb-node-2:3000").is_ok());
        assert!(validate_peer_address("asteroidb-node-3:3000").is_ok());
    }

    #[test]
    fn validate_peer_address_accepts_ip_port() {
        assert!(validate_peer_address("127.0.0.1:3000").is_ok());
        assert!(validate_peer_address("0.0.0.0:3000").is_ok());
        assert!(validate_peer_address("192.168.1.1:8080").is_ok());
    }

    #[test]
    fn validate_peer_address_accepts_ipv6() {
        assert!(validate_peer_address("[::1]:3000").is_ok());
    }

    #[test]
    fn validate_peer_address_rejects_userinfo_at_sign() {
        // @ in address allows userinfo injection: attacker@169.254.169.254:80
        // passes IP parsing (Err->safe) but reqwest connects to 169.254.169.254.
        assert!(validate_peer_address("attacker@169.254.169.254:80").is_err());
        assert!(validate_peer_address("user@host:3000").is_err());
        assert!(validate_peer_address("user@127.0.0.1:80").is_err());
    }

    #[test]
    fn validate_peer_address_rejects_scheme() {
        assert!(validate_peer_address("http://localhost:3000").is_err());
        assert!(validate_peer_address("ftp://host:22").is_err());
    }

    #[test]
    fn validate_peer_address_rejects_path() {
        assert!(validate_peer_address("localhost:3000/secret").is_err());
    }

    #[test]
    fn validate_peer_address_rejects_missing_port() {
        assert!(validate_peer_address("localhost").is_err());
    }
}
