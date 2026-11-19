use sha2::{Digest, Sha256};
use std::io::Write;
use std::sync::{Arc, RwLock};

use tokio::sync::Mutex;

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
#[cfg(feature = "native-crypto")]
use asteroidb_poc::authority::bls::{BlsProofOfPossession, BlsPublicKey};
#[cfg(not(feature = "native-crypto"))]
use asteroidb_poc::authority::bls_stub::{BlsProofOfPossession, BlsPublicKey};
use asteroidb_poc::authority::certificate::{EpochManager, KeysetRegistry, KeysetVersion};
use asteroidb_poc::authority::creation_policy::CreationPolicyManifest;
use asteroidb_poc::authority::equivocation::EquivocationDetector;
use asteroidb_poc::authority::frontier_sig::NodeSigner;
use asteroidb_poc::authority::operation_evidence::EvidenceVerifier;
use asteroidb_poc::authority::operation_merkle::OperationScope;
use asteroidb_poc::authority::operation_worker::OperationAuthorityWorker;
use asteroidb_poc::authority::static_operation_trust::StaticOperationEvidenceStore;
use asteroidb_poc::authority::writer_auth::WriterRegistry;
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::system_namespace::{
    AuthorityDefinition, OperationAssignment, SystemNamespace,
};
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router_with_operation_mode;
use asteroidb_poc::network::membership::MembershipClient;
use asteroidb_poc::network::operation_certificate_outbox::{
    CertificateOutbox, spawn_certificate_retry_worker_with_auth,
};
use asteroidb_poc::network::operation_certificate_transport::{
    AuthorityTrustCheckError, MultiOperationCertificateState, OperationCertificateState,
    multi_certificate_router, multi_certified_submit_router, multi_public_operation_status_router,
    public_operation_uncertified_router, static_read_trust_digest,
};
use asteroidb_poc::network::operation_link_auth::OperationLinkAuth;
use asteroidb_poc::network::operation_neighbors::{
    OperationNeighborManifest, OperationOwnedPrefixes,
};
use asteroidb_poc::network::operation_outbox::{
    OperationOutbox, spawn_operation_retry_worker_with_auth,
};
use asteroidb_poc::network::operation_policy_outbox::OperationPolicyOutbox;
use asteroidb_poc::network::operation_policy_transport::{
    PolicyWitnessRelay, policy_route_digest, policy_witness_router_authenticated,
    policy_witness_submit_router, spawn_policy_witness_retry_worker,
};
use asteroidb_poc::network::operation_routes::{
    OperationRequiredDestinations, OperationRouteEdge, OperationRouteManifest,
};
use asteroidb_poc::network::operation_support_transport::{
    SupportOutbox, spawn_support_retry_worker_with_auth,
};
use asteroidb_poc::network::operation_transport::{
    AuthenticatedOperationAdmission, AuthorityTerminalRole, OperationInbox, OperationRelay,
    relay_router, relay_router_authenticated, repair_routed_manifest_if_pristine,
    routed_authority_manifest_digest, routed_custody_manifest_digest,
    spawn_operation_completion_worker_with_auth, submit_router,
    unified_operation_view_router_multi, validate_routed_relay_configuration,
};
use asteroidb_poc::network::sync::SyncClient;
use asteroidb_poc::network::{NodeConfig, PeerRegistry};
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::runtime::persistence::PersistenceConfig;
use asteroidb_poc::runtime::{BlsConfig, NodeRunner, NodeRunnerConfig};
use asteroidb_poc::store::wal::WalSyncer;
use asteroidb_poc::types::{KeyRange, NodeId};

/// A parsed `ASTEROIDB_AUTHORITY_KEYS` entry: node ID, Ed25519 verifying key,
/// and (optionally) a BLS public key with its verified proof of possession.
type AuthorityKeyEntry = (
    NodeId,
    ed25519_dalek::VerifyingKey,
    Option<(BlsPublicKey, BlsProofOfPossession)>,
);

fn open_old_certified_for_mode(
    node_id: NodeId,
    namespace: Arc<RwLock<SystemNamespace>>,
    config: &PersistenceConfig,
    operation_mode: bool,
) -> std::io::Result<(CertifiedApi, Option<Arc<WalSyncer>>)> {
    if operation_mode {
        Ok((CertifiedApi::new(node_id, namespace), None))
    } else {
        asteroidb_poc::runtime::persistence::recover_certified(node_id, namespace, config)
    }
}

fn open_old_eventual_for_mode(
    node_id: NodeId,
    config: &PersistenceConfig,
    operation_mode: bool,
) -> std::io::Result<(EventualApi, Option<Arc<WalSyncer>>)> {
    if operation_mode {
        Ok((EventualApi::new(node_id), None))
    } else {
        asteroidb_poc::runtime::persistence::recover_eventual(node_id, config)
    }
}

enum OperationAuthorityPassError {
    Worker(String),
    RetryableShared(String),
    Shared(String),
    Gc(String),
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfiguredCertificateScope {
    #[serde(default)]
    namespace_id: String,
    key_prefix: String,
    faults: usize,
}

fn parse_certificate_scopes(
    multi: Option<&str>,
    legacy_prefix: Option<&str>,
    legacy_faults: Option<&str>,
    default_namespace_id: &str,
) -> Result<(Vec<ConfiguredCertificateScope>, bool), String> {
    if let Some(raw) = multi {
        if legacy_prefix.is_some() || legacy_faults.is_some() {
            return Err(
                "ASTEROIDB_OPERATION_CERT_SCOPES conflicts with legacy certificate scope variables"
                    .into(),
            );
        }
        let mut scopes: Vec<ConfiguredCertificateScope> = serde_json::from_str(raw)
            .map_err(|error| format!("invalid ASTEROIDB_OPERATION_CERT_SCOPES: {error}"))?;
        if scopes.is_empty() {
            return Err("ASTEROIDB_OPERATION_CERT_SCOPES must contain at least one scope".into());
        }
        let mut prefixes = std::collections::BTreeSet::new();
        for scope in &mut scopes {
            if scope.namespace_id.is_empty() {
                scope.namespace_id = default_namespace_id.into();
            }
            if !prefixes.insert((scope.namespace_id.clone(), scope.key_prefix.clone())) {
                return Err(
                    "ASTEROIDB_OPERATION_CERT_SCOPES contains a duplicate namespace and prefix"
                        .into(),
                );
            }
        }
        return Ok((scopes, true));
    }
    match (legacy_prefix, legacy_faults) {
        (Some(key_prefix), Some(faults)) => {
            let faults = faults.parse().map_err(|_| "invalid ASTEROIDB_OPERATION_CERT_FAULTS")?;
            Ok((vec![ConfiguredCertificateScope { namespace_id: default_namespace_id.into(), key_prefix: key_prefix.into(), faults }], false))
        }
        (None, None) => Ok((Vec::new(), false)),
        _ => Err("ASTEROIDB_OPERATION_CERT_SCOPE_PREFIX and ASTEROIDB_OPERATION_CERT_FAULTS must be set together".into()),
    }
}

fn required_operation_creation_policy(
    json: Option<&str>,
) -> Result<Arc<CreationPolicyManifest>, String> {
    let json = json.ok_or("ASTEROIDB_WRITER_KEYS requires ASTEROIDB_OPERATION_CREATION_POLICY")?;
    CreationPolicyManifest::from_json(json)
        .map(Arc::new)
        .map_err(|error| format!("invalid ASTEROIDB_OPERATION_CREATION_POLICY: {error}"))
}

fn policy_route_configuration(
    edges_json: Option<&str>,
    routes_json: Option<&str>,
    destinations: Option<&OperationRequiredDestinations>,
    data_routes: Option<&OperationRouteManifest>,
) -> Result<Option<OperationRouteManifest>, String> {
    let explicit = match (edges_json, routes_json) {
        (None, None) => None,
        (Some(edges_json), Some(routes_json)) => {
            let edges: Vec<OperationRouteEdge> =
                serde_json::from_str(edges_json).map_err(|error| {
                    format!("invalid ASTEROIDB_OPERATION_POLICY_ROUTE_EDGES: {error}")
                })?;
            Some(
                OperationRouteManifest::from_json(routes_json, &edges).map_err(|error| {
                    format!("invalid ASTEROIDB_OPERATION_POLICY_ROUTES: {error}")
                })?,
            )
        }
        _ => {
            return Err("ASTEROIDB_OPERATION_POLICY_ROUTE_EDGES and ASTEROIDB_OPERATION_POLICY_ROUTES must be set together".into());
        }
    };
    if destinations.is_none() && explicit.is_some() {
        return Err("policy routes require ASTEROIDB_OPERATION_POLICY_DESTINATIONS".into());
    }
    let routes = explicit.or_else(|| data_routes.cloned());
    if destinations.is_some() && routes.is_none() {
        return Err(
            "policy-only delivery requires policy routes or routed operation configuration".into(),
        );
    }
    Ok(routes)
}

/// Validate every existing operation pin before creating a missing one.
/// In particular, an old `.routing` must reject a changed route without
/// leaving new creation-policy or link-identity pins behind.
// Keep each immutable startup digest explicit at this fail-closed boundary.
#[allow(clippy::too_many_arguments)]
fn pin_operation_startup_manifests(
    inbox: &OperationInbox,
    outbox: &mut OperationOutbox,
    creation_digest: Option<[u8; 32]>,
    link_digest: Option<[u8; 32]>,
    route_digest: Option<[u8; 32]>,
    owned_digest: [u8; 32],
    authority_digest: Option<[u8; 32]>,
    routed_configuration: Option<(
        &OperationNeighborManifest,
        &OperationRouteManifest,
        &[AuthorityTerminalRole],
        &SystemNamespace,
    )>,
) -> Result<(), String> {
    if let Some((neighbors, routes, authorities, _)) = routed_configuration {
        validate_routed_relay_configuration(neighbors, routes, authorities)
            .map_err(|error| error.to_string())?;
    }
    match creation_digest {
        Some(digest) => inbox
            .verify_creation_policy_manifest(digest)
            .map_err(|error| error.to_string())?,
        None => inbox
            .ensure_no_creation_policy_manifest()
            .map_err(|error| error.to_string())?,
    }
    if let Some(digest) = link_digest {
        inbox
            .verify_link_manifest(digest)
            .map_err(|error| error.to_string())?;
        outbox
            .verify_link_manifest(digest)
            .map_err(|error| error.to_string())?;
    }
    inbox
        .verify_owned_manifest(owned_digest)
        .map_err(|error| error.to_string())?;
    if let Some(digest) = authority_digest {
        inbox
            .verify_authority_manifest(digest)
            .map_err(|error| error.to_string())?;
    }
    if let Some(digest) = route_digest {
        repair_routed_manifest_if_pristine(inbox, outbox, digest)
            .map_err(|error| error.to_string())?;
    }
    if let Some((_, _, _, mounted_policy)) = routed_configuration {
        inbox
            .ensure_mounted_authority_policy(mounted_policy)
            .map_err(|error| error.to_string())?;
    }
    if let Some(digest) = creation_digest {
        inbox
            .ensure_creation_policy_manifest(digest)
            .map_err(|error| error.to_string())?;
    }
    if let Some(digest) = link_digest {
        inbox
            .ensure_link_manifest(digest)
            .map_err(|error| error.to_string())?;
        outbox
            .ensure_link_manifest(digest)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn certificate_scope_root(
    data_dir: &std::path::Path,
    namespace_id: &str,
    prefix: &str,
) -> std::path::PathBuf {
    let mut hash = Sha256::new();
    hash.update(b"asteroidb operation certificate scope v1\0");
    hash.update((namespace_id.len() as u64).to_le_bytes());
    hash.update(namespace_id.as_bytes());
    hash.update((prefix.len() as u64).to_le_bytes());
    hash.update(prefix.as_bytes());
    data_dir
        .join("operation_certificate_scopes")
        .join(hex::encode(hash.finalize()))
}

const CERTIFICATE_SCOPE_USED_MAGIC: &[u8] = b"ASTSCOPE-USED-1";

fn certificate_scope_used_marker(
    data_dir: &std::path::Path,
    scope_root: &std::path::Path,
) -> std::path::PathBuf {
    let name = scope_root
        .file_name()
        .expect("scope root is named by its digest")
        .to_string_lossy();
    data_dir.join(format!(".operation-certificate-scope-{name}.used"))
}

fn pin_certificate_scope_used(
    data_dir: &std::path::Path,
    scope_root: &std::path::Path,
) -> Result<(), String> {
    let marker = certificate_scope_used_marker(data_dir, scope_root);
    if marker.exists() {
        if std::fs::read(&marker).map_err(|error| error.to_string())?
            != CERTIFICATE_SCOPE_USED_MAGIC
        {
            return Err("saved certificate scope marker is invalid".into());
        }
        return Ok(());
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker)
        .map_err(|error| error.to_string())?;
    file.write_all(CERTIFICATE_SCOPE_USED_MAGIC)
        .map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    std::fs::File::open(data_dir)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn nonempty_directory(path: &std::path::Path) -> bool {
    match std::fs::read_dir(path) {
        Ok(mut entries) => entries.next().is_some(),
        Err(_) => path.exists(),
    }
}

fn validate_certificate_state_layout(
    data_dir: &std::path::Path,
    scopes: &[ConfiguredCertificateScope],
    multi_scope_mode: bool,
) -> Result<(), String> {
    let mut evidence_used_marker = false;
    if data_dir.exists() {
        for entry in std::fs::read_dir(data_dir).map_err(|error| error.to_string())? {
            let name = entry.map_err(|error| error.to_string())?.file_name();
            if name.to_str().is_some_and(|name| {
                name.starts_with(".operation-evidence-") && name.ends_with(".used")
            }) {
                evidence_used_marker = true;
                break;
            }
        }
    }
    let legacy_state = [
        "operation_evidence",
        "operation_supports",
        ".operation_supports.operation-supports.used",
        "operation_support_outbox",
        "operation_certificate_outbox",
        "operation_authority",
        ".operation_authority.operation-authority.used",
    ]
    .iter()
    .any(|name| nonempty_directory(&data_dir.join(name)))
        || evidence_used_marker;
    if multi_scope_mode && legacy_state {
        return Err("legacy certificate state exists; migrate it before enabling ASTEROIDB_OPERATION_CERT_SCOPES".into());
    }
    if !multi_scope_mode && scopes.is_empty() && legacy_state {
        return Err("legacy certificate state exists; configure its scope before starting".into());
    }
    let multi_root = data_dir.join("operation_certificate_scopes");
    let configured: std::collections::BTreeSet<_> = if multi_scope_mode {
        scopes
            .iter()
            .map(|scope| certificate_scope_root(data_dir, &scope.namespace_id, &scope.key_prefix))
            .collect()
    } else {
        std::collections::BTreeSet::new()
    };
    let configured_markers: std::collections::BTreeSet<_> = configured
        .iter()
        .map(|scope_root| certificate_scope_used_marker(data_dir, scope_root))
        .collect();
    if data_dir.exists() {
        for entry in std::fs::read_dir(data_dir).map_err(|error| error.to_string())? {
            let path = entry.map_err(|error| error.to_string())?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with(".operation-certificate-scope-") && name.ends_with(".used")
                })
            {
                if !configured_markers.contains(&path) {
                    return Err(
                        "saved certificate scope is absent from current configuration".into(),
                    );
                }
                if std::fs::read(&path).map_err(|error| error.to_string())?
                    != CERTIFICATE_SCOPE_USED_MAGIC
                {
                    return Err("saved certificate scope marker is invalid".into());
                }
            }
        }
    }
    for scope_root in &configured {
        let marker = certificate_scope_used_marker(data_dir, scope_root);
        if marker.exists() && !nonempty_directory(scope_root) {
            return Err("previously used certificate scope directory is missing".into());
        }
    }
    if !multi_root.exists() {
        return Ok(());
    }
    let entries = std::fs::read_dir(&multi_root)
        .map_err(|error| format!("cannot inspect certificate scope state: {error}"))?;
    for entry in entries {
        let path = entry
            .map_err(|error| format!("cannot inspect certificate scope state: {error}"))?
            .path();
        if !configured.contains(&path) && nonempty_directory(&path) {
            return Err(if multi_scope_mode {
                "saved certificate scope is absent from ASTEROIDB_OPERATION_CERT_SCOPES"
            } else {
                "multi-scope certificate state exists; migrate it before enabling legacy certificate scope variables"
            }
            .into());
        }
    }
    Ok(())
}

fn operation_scope_keyset(
    registry: &KeysetRegistry,
    authority_ids: &[NodeId],
) -> Result<KeysetRegistry, String> {
    let requested: std::collections::BTreeSet<_> = authority_ids.iter().cloned().collect();
    if requested.is_empty() || requested.len() != authority_ids.len() {
        return Err("operation scope has empty or duplicate Authority IDs".into());
    }
    let version = registry.current_version();
    let epoch = registry
        .registered_epoch(&version)
        .ok_or("operation scope has no trusted Authority keyset")?;
    let keys = registry
        .get_keys(&version)
        .ok_or("operation scope has no trusted Authority keyset")?;
    let selected: Vec<_> = keys
        .iter()
        .filter(|(id, _)| requested.contains(id))
        .cloned()
        .collect();
    let selected_ids: std::collections::BTreeSet<_> = selected.iter().map(|(id, _)| id).collect();
    if selected.len() != requested.len() || selected_ids.len() != requested.len() {
        return Err("operation Authority IDs are missing or duplicated in trusted keyset".into());
    }
    let mut scoped = KeysetRegistry::new();
    scoped
        .register_keyset(version, epoch, selected)
        .map_err(|error| format!("cannot construct operation Authority keyset: {error}"))?;
    Ok(scoped)
}

/// Check every configured certificate scope before startup writes any operation
/// manifest or reconciles routed custody. Return the keysets used by recovery
/// so the preflight and mounted stores use the same trusted inputs.
fn preflight_certificate_scopes(
    scopes: &[ConfiguredCertificateScope],
    namespace: &SystemNamespace,
    global_registry: &KeysetRegistry,
    owned: &OperationOwnedPrefixes,
) -> Result<Vec<KeysetRegistry>, String> {
    scopes
        .iter()
        .map(|configured| {
            let scope = OperationScope {
                namespace_id: configured.namespace_id.clone(),
                key_prefix: configured.key_prefix.clone(),
            };
            let authority = namespace
                .operation_authority_definition(&scope.namespace_id, &scope.key_prefix)
                .ok_or("operation scope has no exact Authority definition")?;
            let scoped = operation_scope_keyset(global_registry, &authority.authority_nodes)?;
            let version = scoped.current_version();
            if scoped.registered_epoch(&version) != Some(0) {
                return Err("static operation Authority keyset must register at epoch zero".into());
            }
            let n = scoped
                .get_keys(&version)
                .ok_or("operation scope has no trusted Authority keyset")?
                .len();
            if configured
                .faults
                .checked_mul(3)
                .is_none_or(|triple| triple >= n)
            {
                return Err("operation scope requires n > 3f Authorities".into());
            }
            static_read_trust_digest(namespace, &scope, &scoped, owned)?;
            Ok(scoped)
        })
        .collect()
}

/// Parse peer authority public keys from `ASTEROIDB_AUTHORITY_KEYS`.
///
/// Format: comma-separated
/// `<node-id>=<ed25519 hex (64 chars)>[/<bls hex (96 chars)>/<pop hex (192 chars)>]`
/// entries, e.g. `auth-2=ab..cd,auth-3=ef..01/89..76/aa..bb`. The third
/// segment is a BLS proof of possession (PoP): a signature over the public
/// key itself proving the distributor holds the secret key, which blocks
/// rogue-key attacks against BLS aggregate verification. Each node prints
/// its own ready-to-distribute entry (including the PoP) at startup.
///
/// With `strict = false` (default), entries that fail to parse are logged
/// and skipped so a single bad key cannot prevent startup, and a legacy
/// two-segment entry (BLS key without PoP) degrades to Ed25519-only. With
/// `strict = true` (`ASTEROIDB_REQUIRE_SIGNED_FRONTIERS`), any malformed
/// entry — missing '=', invalid Ed25519 key, or a missing/invalid PoP —
/// is a hard error, so a typo cannot silently drop a peer's keys.
///
/// This is the static key distribution channel required for verifying peer
/// frontier signatures (FR-008); without it only self-signed frontiers verify.
fn parse_authority_keys_env(strict: bool) -> Result<Vec<AuthorityKeyEntry>, String> {
    let Ok(raw) = std::env::var("ASTEROIDB_AUTHORITY_KEYS") else {
        return Ok(Vec::new());
    };
    parse_authority_keys(&raw, strict)
}

/// Parse the `ASTEROIDB_AUTHORITY_KEYS` value (separated for unit testing).
///
/// With `strict = false` this always returns `Ok`; in strict mode any
/// malformed entry (missing '=', invalid Ed25519 key, or a BLS key
/// distributed without a valid proof of possession) produces `Err`.
fn parse_authority_keys(raw: &str, strict: bool) -> Result<Vec<AuthorityKeyEntry>, String> {
    let mut keys = Vec::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((id, key_part)) = entry.split_once('=') else {
            if strict {
                return Err(format!("entry '{entry}' is missing '='"));
            }
            eprintln!("warning: ASTEROIDB_AUTHORITY_KEYS entry '{entry}' missing '='; skipping");
            continue;
        };
        let segments: Vec<&str> = key_part.splitn(3, '/').map(str::trim).collect();

        let ed_vk = hex::decode(segments[0])
            .ok()
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            .and_then(|arr| ed25519_dalek::VerifyingKey::from_bytes(&arr).ok());
        let Some(ed_vk) = ed_vk else {
            if strict {
                return Err(format!("entry for '{id}' has an invalid Ed25519 key"));
            }
            eprintln!(
                "warning: ASTEROIDB_AUTHORITY_KEYS entry for '{id}' has an invalid Ed25519 key; skipping"
            );
            continue;
        };

        let bls = match segments.len() {
            1 => None,
            2 => {
                // Legacy format: BLS key without proof of possession.
                if strict {
                    return Err(format!(
                        "entry for '{id}' has a BLS key without a proof-of-possession"
                    ));
                }
                eprintln!(
                    "warning: ASTEROIDB_AUTHORITY_KEYS entry for '{id}' has a BLS key without a \
                     proof-of-possession; ignoring the BLS part (Ed25519-only)"
                );
                None
            }
            _ => match parse_bls_with_pop(segments[1], segments[2]) {
                Ok(pair) => Some(pair),
                Err(reason) => {
                    if strict {
                        return Err(format!("entry for '{id}' {reason}"));
                    }
                    eprintln!(
                        "warning: ASTEROIDB_AUTHORITY_KEYS entry for '{id}' {reason}; skipping entry"
                    );
                    continue;
                }
            },
        };

        keys.push((NodeId(id.trim().to_string()), ed_vk, bls));
    }
    Ok(keys)
}

/// Parse and validate a `<bls hex>/<pop hex>` pair (native build).
///
/// Both values must decode to valid group elements and the proof of
/// possession must verify against the public key (rogue-key defense).
#[cfg(feature = "native-crypto")]
fn parse_bls_with_pop(
    bls_hex: &str,
    pop_hex: &str,
) -> Result<(BlsPublicKey, BlsProofOfPossession), String> {
    let pk = BlsPublicKey::from_hex(bls_hex).ok_or_else(|| "has an invalid BLS key".to_string())?;
    let pop = BlsProofOfPossession::from_hex(pop_hex)
        .ok_or_else(|| "has an invalid BLS proof-of-possession".to_string())?;
    if !asteroidb_poc::authority::bls::verify_pop(&pk, &pop) {
        return Err("has a BLS proof-of-possession that fails verification".to_string());
    }
    Ok((pk, pop))
}

/// Syntactic-only validation of a `<bls hex>/<pop hex>` pair (stub build).
///
/// Stub builds cannot verify a PoP cryptographically, and non-native
/// verification ignores the BLS lane entirely, so only the hex shape is
/// checked (96 / 192 hex chars) to keep the env format consistent across
/// build flavours.
#[cfg(not(feature = "native-crypto"))]
fn parse_bls_with_pop(
    bls_hex: &str,
    pop_hex: &str,
) -> Result<(BlsPublicKey, BlsProofOfPossession), String> {
    fn is_hex(s: &str, len: usize) -> bool {
        s.len() == len && s.bytes().all(|b| b.is_ascii_hexdigit())
    }
    if !is_hex(bls_hex, 96) {
        return Err("has an invalid BLS key".to_string());
    }
    if !is_hex(pop_hex, 192) {
        return Err("has an invalid BLS proof-of-possession".to_string());
    }
    Ok((
        BlsPublicKey(bls_hex.to_string()),
        BlsProofOfPossession(pop_hex.to_string()),
    ))
}

/// Parse authority node IDs from `ASTEROIDB_AUTHORITY_NODES` env var (comma-separated),
/// falling back to the default `["auth-1", "auth-2", "auth-3"]`.
///
/// Empty entries (`a,,b`, a trailing comma, `","`) and repeats are dropped
/// with a warning, mirroring `parse_control_plane_nodes`. Both would wedge the
/// seeded catch-all range rather than merely look untidy: the majority
/// denominator is the length of this list, while ack frontiers are keyed by
/// `(key_range, policy_version, authority_id)`, so a repeated ID inflates the
/// denominator without being able to supply a second frontier, and a blank ID
/// names a node that can never report at all.
fn parse_authority_nodes(raw: Option<&str>) -> Vec<NodeId> {
    let default = || {
        vec![
            NodeId("auth-1".into()),
            NodeId("auth-2".into()),
            NodeId("auth-3".into()),
        ]
    };
    let Some(val) = raw.filter(|v| !v.trim().is_empty()) else {
        return default();
    };

    let mut seen = std::collections::HashSet::new();
    let mut nodes = Vec::new();
    let mut skipped = 0usize;
    for part in val.split(',') {
        let part = part.trim();
        if part.is_empty() || !seen.insert(part.to_string()) {
            skipped += 1;
            continue;
        }
        nodes.push(NodeId(part.to_string()));
    }
    if skipped > 0 {
        eprintln!(
            "warning: ASTEROIDB_AUTHORITY_NODES contains {skipped} empty or \
             duplicate entry(ies); ignoring them"
        );
    }
    if nodes.is_empty() {
        eprintln!(
            "warning: ASTEROIDB_AUTHORITY_NODES has no usable entries; \
             falling back to the default authority set"
        );
        return default();
    }
    nodes
}

fn authority_nodes() -> Vec<NodeId> {
    parse_authority_nodes(std::env::var("ASTEROIDB_AUTHORITY_NODES").ok().as_deref())
}

/// Outcome of parsing `ASTEROIDB_CONTROL_PLANE_NODES`
/// (see [`parse_control_plane_nodes`]).
enum ControlPlaneNodesParse {
    /// At least one non-empty entry.
    Voters(std::collections::BTreeSet<NodeId>),
    /// Unset, or set to whitespace only: standalone default (`[self]`).
    Unset,
    /// Set, but EVERY comma-separated entry is empty (e.g. `","`): a
    /// definite configuration error — the caller must fail-stop rather
    /// than silently fall back to a lone self-voter.
    AllEntriesEmpty,
}

/// Pure parser for `ASTEROIDB_CONTROL_PLANE_NODES` (comma-separated node
/// IDs, static membership). Empty entries (trailing comma, `a,,b`, …) are
/// skipped with a warning instead of materializing a phantom `NodeId("")`
/// voter — a phantom voter silently changes the majority denominator and
/// is fed into the Raft configuration fencing as a bogus member.
fn parse_control_plane_nodes(raw: Option<&str>) -> ControlPlaneNodesParse {
    let Some(val) = raw else {
        return ControlPlaneNodesParse::Unset;
    };
    if val.trim().is_empty() {
        return ControlPlaneNodesParse::Unset;
    }
    let mut voters = std::collections::BTreeSet::new();
    let mut skipped = 0usize;
    for part in val.split(',') {
        let part = part.trim();
        if part.is_empty() {
            skipped += 1;
        } else {
            voters.insert(NodeId(part.to_string()));
        }
    }
    if skipped > 0 {
        eprintln!(
            "warning: ASTEROIDB_CONTROL_PLANE_NODES contains {skipped} empty \
             entry(ies) (stray comma?); ignoring them"
        );
    }
    if voters.is_empty() {
        ControlPlaneNodesParse::AllEntriesEmpty
    } else {
        ControlPlaneNodesParse::Voters(voters)
    }
}

/// Resolve the control-plane Raft voter set from
/// `ASTEROIDB_CONTROL_PLANE_NODES`. Defaults to `[self]` when unset so a
/// standalone node elects itself immediately and stays writable; a set but
/// entirely-empty value (e.g. `","`) is a fail-stop configuration error
/// (falling back to `[self]` would silently self-elect with majority=1 and
/// diverge from the intended cluster).
///
/// NOTE: deliberately NOT derived from the gossip `PeerRegistry` — its
/// membership shrinks on ping failures, which would silently change the
/// majority definition and break election safety.
fn control_plane_nodes(self_id: &NodeId) -> std::collections::BTreeSet<NodeId> {
    let raw = std::env::var("ASTEROIDB_CONTROL_PLANE_NODES").ok();
    match parse_control_plane_nodes(raw.as_deref()) {
        ControlPlaneNodesParse::Voters(voters) => voters,
        ControlPlaneNodesParse::Unset => [self_id.clone()].into_iter().collect(),
        ControlPlaneNodesParse::AllEntriesEmpty => {
            eprintln!(
                "error: ASTEROIDB_CONTROL_PLANE_NODES is set but contains no non-empty \
                 entries; fix the variable, or unset it for standalone mode"
            );
            std::process::exit(1);
        }
    }
}

/// Parse a millisecond duration env var with a default.
fn env_duration_ms(name: &str, default_ms: u64) -> std::time::Duration {
    let ms = std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default_ms);
    std::time::Duration::from_millis(ms)
}

/// Wait for a shutdown signal.
///
/// On Unix, this resolves on either SIGINT (Ctrl-C) or SIGTERM (the default
/// signal sent by `kubectl delete pod` / `docker stop`).  On non-Unix targets
/// (e.g. Windows) only SIGINT is available, so we fall back to that alone.
#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut sigterm) => {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
        }
        Err(e) => {
            // SIGTERM registration can fail under restrictive seccomp profiles or
            // when the process starts before the tokio runtime is fully active.
            // Fall back to SIGINT-only shutdown to avoid a startup panic that
            // would skip the graceful fan_out_leave membership announcement.
            eprintln!("warn: SIGTERM handler registration failed ({e}); falling back to SIGINT");
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// Build the `tracing` filter from an explicit `RUST_LOG` value.
///
/// Defaults to INFO when `RUST_LOG` is absent, empty, or made entirely of
/// unparseable directives. `EnvFilter::from_default_env()` defaults to
/// ERROR-only instead, and neither the `Dockerfile` nor either compose file
/// sets `RUST_LOG` — so the shipped stacks emitted no WARN and no INFO at
/// all, hiding every operational warning the runtime raises.
///
/// An explicit `RUST_LOG` still wins: `parse_lossy` injects the default
/// directive only when the parsed directive set comes out empty, so
/// `RUST_LOG=error` (and every other value) behaves exactly as before.
///
/// Taking the value as a parameter (rather than reading the environment
/// inside) keeps this deterministically testable.
fn log_env_filter(rust_log: Option<&str>) -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
        .parse_lossy(rust_log.unwrap_or_default())
}

#[tokio::main]
async fn main() {
    // Initialize structured logging. Verbosity is INFO by default and is
    // controlled by the RUST_LOG env var when set (e.g. RUST_LOG=warn or
    // RUST_LOG=asteroidb_poc=debug).
    tracing_subscriber::fmt()
        .with_env_filter(log_env_filter(std::env::var("RUST_LOG").ok().as_deref()))
        .init();

    if std::env::var("ASTEROIDB_GC_HOLE_JUMP")
        .ok()
        .is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true"))
    {
        eprintln!(
            "error: ASTEROIDB_GC_HOLE_JUMP is unavailable: peer eviction can invalidate its inbound GC evidence"
        );
        std::process::exit(1);
    }

    // Load configuration: either from a config file or from individual env vars.
    // `self_node` carries this node's real mode and tags; it used to be
    // discarded here, which left nothing in the process that knew them.
    let (self_node, bind_addr, advertise_addr, config_peer_registry) =
        match std::env::var("ASTEROIDB_CONFIG") {
            Ok(config_path) => match NodeConfig::load(&config_path) {
                Ok(config) => {
                    let self_node = config.node;
                    let bind_addr = config.bind_addr.to_string();
                    // Prefer ASTEROIDB_ADVERTISE_ADDR env var, then config field, then bind_addr.
                    let advertise_addr = std::env::var("ASTEROIDB_ADVERTISE_ADDR")
                        .ok()
                        .or(config.advertise_addr)
                        .unwrap_or_else(|| bind_addr.clone());
                    let peer_registry = config.peers;
                    (self_node, bind_addr, advertise_addr, Some(peer_registry))
                }
                Err(e) => {
                    eprintln!("error: failed to load config file '{config_path}': {e}");
                    std::process::exit(1);
                }
            },
            Err(_) => {
                let bind_addr = std::env::var("ASTEROIDB_BIND_ADDR")
                    .unwrap_or_else(|_| "127.0.0.1:3000".into());
                let node_id_str =
                    std::env::var("ASTEROIDB_NODE_ID").unwrap_or_else(|_| "node-1".into());
                // Without a config file there is no source for mode or tags,
                // and no env var supplies them. `Both` with no tags is what
                // `generate_cluster_configs` and every shipped config use.
                let self_node = asteroidb_poc::node::Node::new(
                    NodeId(node_id_str),
                    asteroidb_poc::types::NodeMode::Both,
                );
                // Prefer ASTEROIDB_ADVERTISE_ADDR env var, then fall back to bind_addr.
                let advertise_addr =
                    std::env::var("ASTEROIDB_ADVERTISE_ADDR").unwrap_or_else(|_| bind_addr.clone());
                (self_node, bind_addr, advertise_addr, None)
            }
        };

    let node_id = self_node.id.clone();

    println!("AsteroidDB starting... (node_id={})", node_id.0);

    let auth_nodes = authority_nodes();

    // Determine persistence directory (used for peer registry, store snapshots,
    // and system namespace persistence).
    let data_dir = std::path::PathBuf::from(
        std::env::var("ASTEROIDB_DATA_DIR").unwrap_or_else(|_| "./data".into()),
    );

    let ns_persist_path = data_dir.join("system_namespace.json");
    let (mut ns, ns_is_fresh) = match SystemNamespace::load(&ns_persist_path) {
        Ok(Some(loaded)) => {
            println!("Loaded system namespace from {}", ns_persist_path.display(),);
            (loaded, false)
        }
        Ok(None) => (SystemNamespace::new(), true),
        Err(e) => {
            eprintln!(
                "warning: failed to load system namespace from {}: {e}; starting fresh",
                ns_persist_path.display(),
            );
            (SystemNamespace::new(), true)
        }
    };
    // Recover from the empty-inventory bug: before `recalculate_authorities`
    // refused to write an empty candidate set, a node with no cluster
    // inventory rewrote every certified prefix's authority definition to
    // `authority_nodes: [], auto_generated: true` and persisted it. Such a
    // definition certifies nothing and blocks tombstone GC on its range, so
    // dropping it is strictly better than keeping it: with no definition, a
    // correct one is re-derived (or replicated in) as soon as it can be.
    //
    // This runs AFTER the load, never inside it: rejecting the state at load
    // time would fall into the "starting fresh" arm above and discard every
    // policy and authority on this node.
    let swept = ns.sweep_empty_auto_authorities();
    if !swept.is_empty() {
        tracing::warn!(
            prefixes = ?swept,
            "dropped persisted authority definitions with an empty node set. \
             If any of these were set manually, re-register them with \
             PUT /api/control-plane/authorities (see ops-guide 14.5.1)"
        );
    }

    // Seed the catch-all authority definition only on FIRST boot. Doing it
    // unconditionally would bump the namespace version on every restart,
    // silently diverging this node's replicated control-plane state machine
    // from its peers'. On a fresh cluster the seed is folded into the first
    // Raft leader's `Bootstrap` entry and replicated from there.
    if ns_is_fresh {
        ns.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: String::new(),
            },
            authority_nodes: auth_nodes.clone(),
            auto_generated: false,
        });
    }

    let namespace = Arc::new(RwLock::new(ns));

    // Build shared runtime metrics.
    let metrics = Arc::new(RuntimeMetrics::default());

    // Decide the data-plane mode before opening either old store. Operation
    // mode has its own durable inbox and must not depend on old WAL files.
    let writer_keys_json = match std::env::var("ASTEROIDB_WRITER_KEYS") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            eprintln!("error: ASTEROIDB_WRITER_KEYS is not UTF-8");
            std::process::exit(1);
        }
    };
    let operation_mode = writer_keys_json.is_some();

    // Crash recovery: load each store's snapshot and replay its WAL, then
    // open fresh WAL writers. This must complete before the listener binds
    // and before any peer sync starts, so clients and peers only ever see
    // the recovered state. Recovery failures (damaged snapshot, mid-log
    // WAL corruption) are fail-stop with a runbook pointer — silently
    // starting empty would overwrite an intact replica's durable state.
    let persistence_cfg = asteroidb_poc::runtime::PersistenceConfig::from_env(data_dir.clone());

    // Share a single CertifiedApi between HTTP handlers and NodeRunner
    // so that certification status updates are visible to both.
    let (certified_recovered, certified_wal_syncer) = open_old_certified_for_mode(
        node_id.clone(),
        Arc::clone(&namespace),
        &persistence_cfg,
        operation_mode,
    )
    .unwrap_or_else(|e| {
        eprintln!(
            "error: certified store recovery failed: {e}\n\
             See docs/ops-guide.md (Crash recovery runbook) before retrying."
        );
        std::process::exit(1);
    });
    let certified_api = Arc::new(Mutex::new(certified_recovered));
    let peer_persist_path = PeerRegistry::persist_path(&data_dir);

    // Share a single EventualApi between HTTP handlers and NodeRunner
    // so that HTTP writes are visible to the anti-entropy sync loop.
    let (eventual_recovered, eventual_wal_syncer) =
        open_old_eventual_for_mode(node_id.clone(), &persistence_cfg, operation_mode)
            .unwrap_or_else(|e| {
                eprintln!(
                    "error: eventual store recovery failed: {e}\n\
                     See docs/ops-guide.md (Crash recovery runbook) before retrying."
                );
                std::process::exit(1);
            });
    let eventual_api = Arc::new(Mutex::new(eventual_recovered));

    // Build peer registry: if a config file provided peers, use those;
    // otherwise try to load persisted state from disk; finally fall back
    // to an empty registry (nodes join dynamically via POST /api/internal/join).
    let shared_peers = if let Some(registry) = config_peer_registry {
        Arc::new(Mutex::new(registry))
    } else {
        // No config file — try loading persisted peer registry from disk.
        let registry = if peer_persist_path.exists() {
            match PeerRegistry::load(&peer_persist_path) {
                Ok(loaded) => {
                    if *loaded.self_id() == node_id {
                        println!(
                            "Loaded peer registry from {} ({} peers, generation {})",
                            peer_persist_path.display(),
                            loaded.peer_count(),
                            loaded.generation(),
                        );
                        loaded
                    } else {
                        eprintln!(
                            "warning: saved peer registry has self_id={}, expected {}; ignoring",
                            loaded.self_id().0,
                            node_id.0,
                        );
                        PeerRegistry::new(node_id.clone(), vec![])
                            .expect("empty peer list is always valid")
                    }
                }
                Err(e) => {
                    eprintln!(
                        "warning: failed to load peer registry from {}: {e}; starting with empty registry",
                        peer_persist_path.display(),
                    );
                    PeerRegistry::new(node_id.clone(), vec![])
                        .expect("empty peer list is always valid")
                }
            }
        } else {
            PeerRegistry::new(node_id.clone(), vec![]).expect("empty peer list is always valid")
        };
        Arc::new(Mutex::new(registry))
    };

    // Optional shared token for authenticating internal API requests.
    // Treat an empty string the same as unset — Docker Compose substitutes
    // `${ASTEROIDB_INTERNAL_TOKEN}` as "" when the host variable is not
    // defined, which would otherwise activate the auth middleware with a
    // degenerate empty token, breaking inter-node communication in CI.
    let internal_token = std::env::var("ASTEROIDB_INTERNAL_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());

    // -----------------------------------------------------------------
    // Control-plane Raft consensus (FR-009).
    //
    // Replaces the MVP approval-count check with real log replication:
    // policy/authority mutations are committed by a majority of the STATIC
    // voter set (ASTEROIDB_CONTROL_PLANE_NODES) and applied in commit order
    // on every voter. Membership changes require a full-cluster restart
    // with the updated env (see docs/ops-guide.md).
    // -----------------------------------------------------------------
    let cp_voters = control_plane_nodes(&node_id);
    let raft_config = asteroidb_poc::control_plane::raft::RaftConfig {
        election_timeout_min: env_duration_ms("ASTEROIDB_RAFT_ELECTION_TIMEOUT_MIN_MS", 5_000),
        election_timeout_max: env_duration_ms("ASTEROIDB_RAFT_ELECTION_TIMEOUT_MAX_MS", 10_000),
        heartbeat_interval: env_duration_ms("ASTEROIDB_RAFT_HEARTBEAT_MS", 1_000),
        propose_timeout: env_duration_ms("ASTEROIDB_RAFT_PROPOSE_TIMEOUT_MS", 30_000),
        log_max: std::env::var("ASTEROIDB_RAFT_LOG_MAX")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(4096),
        // Observer namespace pull (M-17): non-voters periodically fetch the
        // voters' committed control-plane state so their namespace — and
        // the policy version their authority signatures carry — keeps
        // following policy bumps. 0 disables the pull (repro/testing only).
        observer_pull_interval: env_duration_ms("ASTEROIDB_OBSERVER_NS_PULL_MS", 5_000),
    };
    if raft_config.election_timeout_max < raft_config.election_timeout_min {
        eprintln!(
            "error: ASTEROIDB_RAFT_ELECTION_TIMEOUT_MAX_MS must be >= \
             ASTEROIDB_RAFT_ELECTION_TIMEOUT_MIN_MS"
        );
        std::process::exit(1);
    }
    let raft_static_peers = std::env::var("ASTEROIDB_RAFT_PEERS")
        .map(|v| asteroidb_poc::network::raft_transport::HttpRaftTransport::parse_static_peers(&v))
        .unwrap_or_default();
    let raft_transport = Arc::new(
        asteroidb_poc::network::raft_transport::HttpRaftTransport::new(
            raft_static_peers.clone(),
            Some(Arc::clone(&shared_peers)),
            internal_token.clone(),
        ),
    );
    // Raft persistence lives under data_dir/raft/ and is ALWAYS enabled
    // (even with ASTEROIDB_PERSISTENCE=off): currentTerm/votedFor/log
    // durability is an election-safety requirement, not an optimization.
    let raft_storage = Arc::new(
        asteroidb_poc::control_plane::raft::storage::FileRaftStorage::new(data_dir.join("raft")),
    );
    let raft_node = match asteroidb_poc::control_plane::raft::RaftNode::new(
        node_id.clone(),
        cp_voters.clone(),
        raft_config,
        raft_storage,
        raft_transport,
        Arc::clone(&namespace),
        Some(ns_persist_path.clone()),
    ) {
        Ok(node) => node,
        Err(e) => {
            // Fail-stop: booting with damaged Raft state could double-vote
            // in an already-voted term (split-brain risk).
            eprintln!(
                "error: failed to load control-plane raft state: {e}\n\
                 See docs/ops-guide.md (Control-plane Raft recovery runbook) before retrying."
            );
            std::process::exit(1);
        }
    };
    if !cp_voters.contains(&node_id) {
        let pull_ms = raft_node.config().observer_pull_interval.as_millis() as u64;
        eprintln!(
            "warning: this node ({}) is NOT in ASTEROIDB_CONTROL_PLANE_NODES {:?}; \
             it runs as a control-plane observer: it rejects policy/authority \
             mutations and follows the voters' committed namespace via periodic \
             pull (ASTEROIDB_OBSERVER_NS_PULL_MS={pull_ms}, 0=disabled)",
            node_id.0,
            cp_voters.iter().map(|v| v.0.as_str()).collect::<Vec<_>>(),
        );
        // Observer-authority lifeline warning (M-17): when this non-voter
        // is named in a local authority definition, its signatures only
        // keep counting toward certification quorums as long as the
        // namespace pull keeps its policy versions fresh.
        let authority_prefixes: Vec<String> = {
            let ns = namespace.read().unwrap_or_else(|e| e.into_inner());
            ns.all_authority_definitions()
                .into_iter()
                .filter(|def| def.authority_nodes.contains(&node_id))
                .map(|def| def.key_range.prefix.clone())
                .collect()
        };
        if !authority_prefixes.is_empty() {
            eprintln!(
                "warning: this observer node is a certification AUTHORITY for \
                 prefixes {authority_prefixes:?}: the namespace pull \
                 (interval {pull_ms}ms) is the lifeline of its signature \
                 validity — if the pull goes stale, the next policy bump \
                 silently removes this node's contribution from the \
                 certification quorum (watch observer_ns_last_pull_unix_ms \
                 in GET /api/control-plane/raft/status and \
                 attestation_stale_version_total on the voters)"
            );
        }
    }
    if cp_voters.len() > 1 && raft_static_peers.is_empty() {
        eprintln!(
            "warning: multi-node control plane configured without ASTEROIDB_RAFT_PEERS; \
             raft peer addresses will only resolve after gossip discovery (join), so \
             leader election may be delayed on a cold start"
        );
    }
    if cp_voters.len() == 1 && !raft_static_peers.is_empty() {
        // The voter set silently defaulted to [self] while raft peers are
        // configured — almost certainly a forgotten ASTEROIDB_CONTROL_PLANE_NODES
        // on this node of a multi-node deployment. Such a node self-elects
        // with majority=1 and diverges from the real cluster (its RPCs are
        // rejected by correctly configured peers, see raft::core).
        eprintln!(
            "warning: ASTEROIDB_RAFT_PEERS is set but the control-plane voter set is \
             just this node ({}); if this is a multi-node deployment, set \
             ASTEROIDB_CONTROL_PLANE_NODES identically on EVERY node — a lone default \
             self-elects with majority=1 and diverges from the real cluster. If this \
             node is meant to be a non-voting OBSERVER, leaving \
             ASTEROIDB_CONTROL_PLANE_NODES unset is also wrong: the node then \
             considers itself a single-node voter cluster and the observer \
             namespace pull (M-17) never starts — set the variable to the real \
             voter set (excluding this node)",
            node_id.0,
        );
    }
    let consensus = Arc::new(Mutex::new(ControlPlaneConsensus::with_raft(Arc::clone(
        &raft_node,
    ))));

    // SLO tracker shared between HTTP handlers and NodeRunner.
    let slo_tracker = Arc::new(asteroidb_poc::ops::slo::SloTracker::new());

    // Shared latency model and topology view for placement policies and the
    // /api/topology endpoint. The same Arc instances are shared between
    // AppState (read by HTTP handlers) and NodeRunner (updated by sync/ping).
    let shared_latency_model = Arc::new(std::sync::RwLock::new(
        asteroidb_poc::placement::latency::LatencyModel::new(),
    ));
    // Seeded with this node so that `GET /api/topology` is correct even
    // before the first inventory refresh; the runner keeps it in step with
    // the peer registry from there.
    let shared_cluster_nodes: Arc<std::sync::RwLock<Vec<asteroidb_poc::node::Node>>> =
        Arc::new(std::sync::RwLock::new(vec![self_node.clone()]));
    let shared_topology_view = Arc::new(std::sync::RwLock::new(
        asteroidb_poc::placement::topology::TopologyView::build(
            &[],
            &asteroidb_poc::placement::latency::LatencyModel::new(),
        ),
    ));

    // Parse the optional signing key seed from the environment.
    // The seed derives the node's Ed25519 frontier-signing key on every
    // build; with `native-crypto` it additionally derives the BLS keypair
    // and enables BLS certificate mode.
    let signing_seed: Option<[u8; 32]> = std::env::var("ASTEROIDB_BLS_SEED").ok().map(|hex_seed| {
        let bytes = hex::decode(&hex_seed).unwrap_or_else(|e| {
            eprintln!("error: ASTEROIDB_BLS_SEED contains invalid hex: {e}");
            std::process::exit(1);
        });
        let mut seed = [0u8; 32];
        let len = bytes.len().min(32);
        seed[..len].copy_from_slice(&bytes[..len]);
        seed
    });
    #[cfg(feature = "native-crypto")]
    let bls_config = signing_seed.map(|seed| BlsConfig { seed });
    #[cfg(not(feature = "native-crypto"))]
    let bls_config: Option<BlsConfig> = None;

    // Wire keyset_registry and current_epoch from EpochManager when BLS is configured.
    let epoch_config = asteroidb_poc::authority::certificate::EpochConfig::default();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let epoch_manager = EpochManager::new(epoch_config.clone(), now_secs);
    let current_epoch_val = epoch_manager.current_epoch(now_secs);

    // Build the node signer from the seed. The signer owns the Ed25519
    // signing key (and, with native-crypto, the BLS keypair) for the whole
    // process lifetime — it is the single derivation point for key material.
    // Without native-crypto the signer is Ed25519-only, so non-native builds
    // still participate in the signing pipeline.
    #[cfg(feature = "native-crypto")]
    let node_signer = signing_seed
        .as_ref()
        .map(|seed| Arc::new(NodeSigner::from_seed(node_id.clone(), seed, true)));
    #[cfg(not(feature = "native-crypto"))]
    let node_signer = signing_seed
        .as_ref()
        .map(|seed| Arc::new(NodeSigner::from_seed(node_id.clone(), seed)));

    // Print this node's ready-to-distribute ASTEROIDB_AUTHORITY_KEYS entry
    // (including the BLS proof of possession on native-crypto builds) so
    // operators can copy it into their peers' configuration.
    if let Some(signer) = &node_signer {
        let ed_hex = hex::encode(signer.verifying_key().as_bytes());
        #[cfg(feature = "native-crypto")]
        let entry = match (signer.bls_public_key(), signer.bls_proof_of_possession()) {
            (Some(pk), Some(pop)) => {
                format!("{}={ed_hex}/{}/{}", node_id.0, pk.to_hex(), pop.to_hex())
            }
            _ => format!("{}={ed_hex}", node_id.0),
        };
        #[cfg(not(feature = "native-crypto"))]
        let entry = format!("{}={ed_hex}", node_id.0);
        println!("Authority key entry for ASTEROIDB_AUTHORITY_KEYS distribution: {entry}");
    }

    // Opt-in strict mode: reject unsigned frontier pushes and require a valid
    // proof of possession for every BLS key distributed via
    // ASTEROIDB_AUTHORITY_KEYS. Read before parsing the peer keys so the
    // parser can fail loudly instead of degrading silently.
    let require_signed_frontiers = std::env::var("ASTEROIDB_REQUIRE_SIGNED_FRONTIERS")
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true"
        })
        .unwrap_or(false);

    // Peer authority keys: "auth-2=<ed25519 hex64>[/<bls hex96>/<pop hex192>],auth-3=...".
    let peer_authority_keys = match parse_authority_keys_env(require_signed_frontiers) {
        Ok(keys) => keys,
        Err(msg) => {
            eprintln!(
                "error: ASTEROIDB_REQUIRE_SIGNED_FRONTIERS is set but ASTEROIDB_AUTHORITY_KEYS \
                 {msg}. Fix the entry (format <node-id>=<ed25519 hex>[/<bls hex>/<pop hex>]; \
                 each node prints its own ready-to-distribute entry at startup) or unset \
                 strict mode."
            );
            std::process::exit(1);
        }
    };

    // Build the keyset registry from the signer's public keys (when signing
    // is enabled) plus any peer authority public keys distributed via
    // ASTEROIDB_AUTHORITY_KEYS. A registry is built whenever either source
    // provides keys, so a verify-only node (peer keys but no local seed) can
    // still verify peer frontier signatures. The same registry instance is
    // shared between AppState (verification) and NodeRunner (signing keyset
    // resolution).
    let keyset_registry = if node_signer.is_none() && peer_authority_keys.is_empty() {
        None
    } else {
        let mut registry = KeysetRegistry::new();

        let mut ed_keys: Vec<(NodeId, ed25519_dalek::VerifyingKey)> = Vec::new();
        #[cfg(feature = "native-crypto")]
        let mut bls_keys: Vec<(String, BlsPublicKey, BlsProofOfPossession)> = Vec::new();

        if let Some(signer) = &node_signer {
            ed_keys.push((node_id.clone(), signer.verifying_key()));
            #[cfg(feature = "native-crypto")]
            if let Some((pk, pop)) = signer
                .bls_public_key()
                .zip(signer.bls_proof_of_possession())
            {
                bls_keys.push((node_id.0.clone(), pk, pop));
            }
        }

        for (peer_id, ed_vk, bls_pair) in peer_authority_keys {
            if peer_id == node_id {
                continue; // own keys already derived from the seed
            }
            #[cfg(feature = "native-crypto")]
            if let Some((pk, pop)) = bls_pair {
                bls_keys.push((peer_id.0.clone(), pk, pop));
            }
            #[cfg(not(feature = "native-crypto"))]
            let _ = bls_pair;
            ed_keys.push((peer_id, ed_vk));
        }

        registry
            .register_keyset(KeysetVersion(1), current_epoch_val, ed_keys)
            .expect("initial keyset registration should succeed");
        #[cfg(feature = "native-crypto")]
        if !bls_keys.is_empty() {
            // register_bls_keys re-verifies every proof of possession
            // (defense-in-depth: peer PoPs were already checked at parse
            // time and our own PoP is derived from our own key), so this
            // expect cannot fire for well-formed inputs.
            registry
                .register_bls_keys(&KeysetVersion(1), bls_keys)
                .expect("BLS key registration should succeed");
        }
        Some(Arc::new(std::sync::RwLock::new(registry)))
    };

    // Strict mode is meaningless without a keyset registry: the frontier
    // handler would have no keys to verify against and would either
    // silently accept everything (the historical fail-open) or reject
    // everything. Fail loudly at startup instead of degrading silently.
    if !operation_mode && require_signed_frontiers && keyset_registry.is_none() {
        eprintln!(
            "error: ASTEROIDB_REQUIRE_SIGNED_FRONTIERS is set but no keyset registry could be \
             built. Set ASTEROIDB_BLS_SEED (signing key seed) and/or ASTEROIDB_AUTHORITY_KEYS \
             (peer public keys) so that signed frontiers can actually be verified."
        );
        std::process::exit(1);
    }

    let current_epoch = Arc::new(std::sync::atomic::AtomicU64::new(current_epoch_val));

    // Equivocation detector: one shared instance for the HTTP receive path
    // (AppState) and the runner's gossip/self-report path — sharing the Arc
    // is what makes evidence detected via HTTP ride the outgoing gossip.
    // Evidence is persisted next to the other node state so it survives
    // restarts (the attestation pool itself is volatile).
    let equivocation = Arc::new(EquivocationDetector::new(
        (!operation_mode).then(|| data_dir.join("equivocation_evidence.json")),
    ));
    // Initialize the accused-authorities gauge from the restored evidence
    // store: without this, a restart resets the gauge to 0 until the next
    // *new* detection, while GET /api/authority/equivocations still reports
    // the persisted accusations — silently blinding gauge-based alerting
    // during an ongoing incident.
    metrics.set_accused_authorities(equivocation.accused_count());

    // Opt-in: exclude attestations from accused authorities from certificate
    // assembly. Off by default — detection never enforces on its own.
    let exclude_accused_authorities = std::env::var("ASTEROIDB_EXCLUDE_ACCUSED_AUTHORITIES")
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true"
        })
        .unwrap_or(false);

    // Build shared HTTP state.
    let state = Arc::new(AppState {
        eventual: Arc::clone(&eventual_api),
        certified: Arc::clone(&certified_api),
        namespace: Arc::clone(&namespace),
        metrics: Arc::clone(&metrics),
        peers: Some(Arc::clone(&shared_peers)),
        peer_persist_path: Some(peer_persist_path),
        namespace_persist_path: Some(ns_persist_path.clone()),
        consensus,
        internal_token: internal_token.clone(),
        self_node_id: Some(node_id.clone()),
        self_addr: Some(advertise_addr.clone()),
        latency_model: Some(Arc::clone(&shared_latency_model)),
        cluster_nodes: Some(Arc::clone(&shared_cluster_nodes)),
        slo_tracker: Arc::clone(&slo_tracker),
        keyset_registry: keyset_registry.clone(),
        epoch_config: epoch_config.clone(),
        current_epoch: Arc::clone(&current_epoch),
        require_signed_frontiers,
        equivocation: Arc::clone(&equivocation),
        exclude_accused_authorities,
        eventual_wal: eventual_wal_syncer.clone(),
        certified_wal: certified_wal_syncer.clone(),
    });

    if std::env::var_os("ASTEROIDB_OPERATION_LEGACY_CREATION_POLICY").is_some() {
        eprintln!(
            "error: ASTEROIDB_OPERATION_LEGACY_CREATION_POLICY is unsupported; configure ASTEROIDB_OPERATION_CREATION_POLICY"
        );
        std::process::exit(1);
    }
    let mut app = router_with_operation_mode(state, operation_mode);
    let mut operation_retry = None;
    let mut operation_policy_retry = None;
    let mut operation_support_retry = Vec::new();
    let mut operation_certificate_retry = Vec::new();
    let mut operation_authority = Vec::new();
    // Operation ingest and public operation reads are enabled only with an
    // explicit writer-key manifest, namespace scope, and internal bearer token.
    // Operation mode fences the old HTTP data plane and does not open its WAL.
    if let Some(writer_keys_json) = writer_keys_json {
        let Some(token) = internal_token.clone() else {
            eprintln!("error: ASTEROIDB_WRITER_KEYS requires ASTEROIDB_INTERNAL_TOKEN");
            std::process::exit(1);
        };
        let namespace_id = std::env::var("ASTEROIDB_OPERATION_NAMESPACE_ID")
            .ok()
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| {
                eprintln!("error: ASTEROIDB_WRITER_KEYS requires ASTEROIDB_OPERATION_NAMESPACE_ID");
                std::process::exit(1);
            });
        let (certificate_scopes, multi_scope_mode) = parse_certificate_scopes(
            std::env::var("ASTEROIDB_OPERATION_CERT_SCOPES")
                .ok()
                .as_deref(),
            std::env::var("ASTEROIDB_OPERATION_CERT_SCOPE_PREFIX")
                .ok()
                .as_deref(),
            std::env::var("ASTEROIDB_OPERATION_CERT_FAULTS")
                .ok()
                .as_deref(),
            &namespace_id,
        )
        .unwrap_or_else(|error| {
            eprintln!("error: {error}");
            std::process::exit(1);
        });
        if let Ok(raw) = std::env::var("ASTEROIDB_OPERATION_ASSIGNMENTS") {
            let assignments: Vec<OperationAssignment> =
                serde_json::from_str(&raw).unwrap_or_else(|error| {
                    eprintln!("error: invalid ASTEROIDB_OPERATION_ASSIGNMENTS: {error}");
                    std::process::exit(1);
                });
            let mut seen = std::collections::BTreeSet::new();
            let mut control_plane = namespace.write().unwrap_or_else(|_| {
                eprintln!("error: system namespace is poisoned");
                std::process::exit(1);
            });
            for assignment in assignments {
                let scope = (
                    assignment.namespace_id.clone(),
                    assignment.policy.key_range.prefix.clone(),
                );
                if !seen.insert(scope) {
                    eprintln!("error: duplicate ASTEROIDB_OPERATION_ASSIGNMENTS scope");
                    std::process::exit(1);
                }
                control_plane
                    .set_operation_assignment(assignment)
                    .unwrap_or_else(|error| {
                        eprintln!("error: invalid ASTEROIDB_OPERATION_ASSIGNMENTS: {error}");
                        std::process::exit(1);
                    });
            }
        }
        let operation_namespace_ids: std::collections::BTreeSet<_> =
            std::iter::once(namespace_id.clone())
                .chain(
                    certificate_scopes
                        .iter()
                        .map(|scope| scope.namespace_id.clone()),
                )
                .collect();
        let registry = WriterRegistry::from_json(&writer_keys_json).unwrap_or_else(|e| {
            eprintln!("error: invalid ASTEROIDB_WRITER_KEYS: {e}");
            std::process::exit(1);
        });
        let neighbors_json = std::env::var("ASTEROIDB_OPERATION_NEIGHBORS").unwrap_or_else(|_| {
            eprintln!("error: ASTEROIDB_WRITER_KEYS requires ASTEROIDB_OPERATION_NEIGHBORS (use [] for a leaf node)");
            std::process::exit(1);
        });
        let neighbors = OperationNeighborManifest::from_json(&neighbors_json, &node_id)
            .unwrap_or_else(|e| {
                eprintln!("error: invalid ASTEROIDB_OPERATION_NEIGHBORS: {e}");
                std::process::exit(1);
            });
        let owned_json = std::env::var("ASTEROIDB_OPERATION_OWNED_PREFIXES").unwrap_or_else(|_| {
            eprintln!("error: ASTEROIDB_WRITER_KEYS requires ASTEROIDB_OPERATION_OWNED_PREFIXES (use [] for a transit-only node)");
            std::process::exit(1);
        });
        let owned = OperationOwnedPrefixes::from_json(&owned_json).unwrap_or_else(|e| {
            eprintln!("error: invalid ASTEROIDB_OPERATION_OWNED_PREFIXES: {e}");
            std::process::exit(1);
        });
        let writer_registry = Arc::new(registry);
        let creation_policy_json = std::env::var("ASTEROIDB_OPERATION_CREATION_POLICY").ok();
        let creation_policy = required_operation_creation_policy(creation_policy_json.as_deref())
            .unwrap_or_else(|error| {
                eprintln!("error: {error}");
                std::process::exit(1);
            });
        let routed_config = match (
            std::env::var("ASTEROIDB_OPERATION_ROUTE_EDGES").ok(),
            std::env::var("ASTEROIDB_OPERATION_ROUTES").ok(),
            std::env::var("ASTEROIDB_OPERATION_REQUIRED_DESTINATIONS").ok(),
        ) {
            (None, None, None) => None,
            (Some(edges_json), Some(routes_json), Some(required_json)) => {
                let edges: Vec<OperationRouteEdge> = serde_json::from_str(&edges_json)
                    .unwrap_or_else(|e| {
                        eprintln!("error: invalid ASTEROIDB_OPERATION_ROUTE_EDGES: {e}");
                        std::process::exit(1);
                    });
                let routes = OperationRouteManifest::from_json(&routes_json, &edges)
                    .unwrap_or_else(|e| {
                        eprintln!("error: invalid ASTEROIDB_OPERATION_ROUTES: {e}");
                        std::process::exit(1);
                    });
                let required = OperationRequiredDestinations::from_json(&required_json)
                    .unwrap_or_else(|e| {
                        eprintln!("error: invalid ASTEROIDB_OPERATION_REQUIRED_DESTINATIONS: {e}");
                        std::process::exit(1);
                    });
                Some((routes, required))
            }
            _ => {
                eprintln!(
                    "error: routed custody requires ASTEROIDB_OPERATION_ROUTE_EDGES, ASTEROIDB_OPERATION_ROUTES and ASTEROIDB_OPERATION_REQUIRED_DESTINATIONS together"
                );
                std::process::exit(1);
            }
        };
        let policy_destinations = std::env::var("ASTEROIDB_OPERATION_POLICY_DESTINATIONS")
            .ok()
            .map(|json| {
                OperationRequiredDestinations::from_json(&json).unwrap_or_else(|error| {
                    eprintln!("error: invalid ASTEROIDB_OPERATION_POLICY_DESTINATIONS: {error}");
                    std::process::exit(1);
                })
            });
        let policy_routes = policy_route_configuration(
            std::env::var("ASTEROIDB_OPERATION_POLICY_ROUTE_EDGES")
                .ok()
                .as_deref(),
            std::env::var("ASTEROIDB_OPERATION_POLICY_ROUTES")
                .ok()
                .as_deref(),
            policy_destinations.as_ref(),
            routed_config.as_ref().map(|(routes, _)| routes),
        )
        .unwrap_or_else(|error| {
            eprintln!("error: {error}");
            std::process::exit(1);
        });
        let operation_link = if routed_config.is_some() || policy_destinations.is_some() {
            let seed = std::env::var("ASTEROIDB_OPERATION_LINK_SEED").unwrap_or_else(|_| {
                eprintln!("error: routed operation or policy-only delivery requires ASTEROIDB_OPERATION_LINK_SEED");
                std::process::exit(1);
            });
            let keys = std::env::var("ASTEROIDB_OPERATION_LINK_KEYS").unwrap_or_else(|_| {
                eprintln!("error: routed operation or policy-only delivery requires ASTEROIDB_OPERATION_LINK_KEYS");
                std::process::exit(1);
            });
            Some(
                OperationLinkAuth::from_manifest(node_id.clone(), &seed, &keys, &neighbors)
                    .unwrap_or_else(|e| {
                        eprintln!("error: invalid operation link identity: {e}");
                        std::process::exit(1);
                    }),
            )
        } else {
            None
        };
        if policy_destinations.is_none()
            && nonempty_directory(&data_dir.join("operation_policy_outbox"))
        {
            eprintln!(
                "error: saved policy-only outbox requires ASTEROIDB_OPERATION_POLICY_DESTINATIONS"
            );
            std::process::exit(1);
        }
        let outbox = Arc::new(Mutex::new(
            (if routed_config.is_some() {
                OperationOutbox::open_routed_for_node(
                    data_dir.join("operation_outbox"),
                    node_id.clone(),
                )
            } else {
                OperationOutbox::open_for_node(data_dir.join("operation_outbox"), node_id.clone())
            })
            .unwrap_or_else(|e| {
                eprintln!("error: operation outbox recovery failed: {e}");
                std::process::exit(1);
            }),
        ));
        let routed_operation_mode = routed_config.is_some();
        if let Err(error) =
            validate_certificate_state_layout(&data_dir, &certificate_scopes, multi_scope_mode)
        {
            eprintln!("error: {error}");
            std::process::exit(1);
        }
        let scope_keysets = if certificate_scopes.is_empty() {
            Vec::new()
        } else {
            let global_registry = keyset_registry.as_ref().unwrap_or_else(|| {
                eprintln!("error: operation certificates require trusted Authority keys");
                std::process::exit(1);
            });
            let global_guard = global_registry.read().unwrap_or_else(|_| {
                eprintln!("error: Authority keyset registry is poisoned");
                std::process::exit(1);
            });
            let namespace_guard = namespace.read().unwrap_or_else(|_| {
                eprintln!("error: system namespace is poisoned");
                std::process::exit(1);
            });
            preflight_certificate_scopes(
                &certificate_scopes,
                &namespace_guard,
                &global_guard,
                &owned,
            )
            .unwrap_or_else(|error| {
                eprintln!("error: invalid operation certificate scope: {error}");
                std::process::exit(1);
            })
        };
        let mounted_namespace = routed_operation_mode.then(|| {
            OperationInbox::read_mounted_authority_policy_at(&data_dir.join("operation_inbox"))
                .unwrap_or_else(|error| {
                    eprintln!("error: saved operation Authority policy recovery failed: {error}");
                    std::process::exit(1);
                })
                .unwrap_or_else(|| {
                    namespace
                        .read()
                        .unwrap_or_else(|_| {
                            eprintln!("error: system namespace is poisoned");
                            std::process::exit(1);
                        })
                        .clone()
                })
        });
        let mut certificate_capacity_scopes: Vec<(OperationScope, KeysetRegistry)> =
            certificate_scopes
                .iter()
                .zip(&scope_keysets)
                .map(|(configured, keyset)| {
                    (
                        OperationScope {
                            namespace_id: configured.namespace_id.clone(),
                            key_prefix: configured.key_prefix.clone(),
                        },
                        keyset.clone(),
                    )
                })
                .collect();
        if let Some(mounted) = &mounted_namespace
            && !mounted.all_operation_assignments().is_empty()
        {
            let global_registry = keyset_registry.as_ref().unwrap_or_else(|| {
                eprintln!("error: routed operation certificates require trusted Authority keys");
                std::process::exit(1);
            });
            let global_guard = global_registry.read().unwrap_or_else(|_| {
                eprintln!("error: Authority keyset registry is poisoned");
                std::process::exit(1);
            });
            for assignment in mounted.all_operation_assignments() {
                let scope = OperationScope {
                    namespace_id: assignment.namespace_id.clone(),
                    key_prefix: assignment.policy.key_range.prefix.clone(),
                };
                if certificate_capacity_scopes
                    .iter()
                    .any(|(known, _)| known == &scope)
                {
                    continue;
                }
                let scoped =
                    operation_scope_keyset(&global_guard, &assignment.authority.authority_nodes)
                        .unwrap_or_else(|error| {
                            eprintln!("error: invalid operation capacity scope: {error}");
                            std::process::exit(1);
                        });
                certificate_capacity_scopes.push((scope, scoped));
            }
        }
        let admission = Arc::new(
            AuthenticatedOperationAdmission::new_for_namespaces_with_creation_policy(
                Arc::clone(&writer_registry),
                operation_namespace_ids,
                Arc::clone(&creation_policy),
            )
            .with_certificate_capacity_scopes(certificate_capacity_scopes),
        );
        let recovered_inbox = OperationInbox::open(data_dir.join("operation_inbox"), admission)
            .unwrap_or_else(|e| {
                eprintln!("error: operation inbox recovery failed: {e:?}");
                std::process::exit(1);
            });
        let inbox = Arc::new(Mutex::new(recovered_inbox));
        let authority_terminals: Vec<_> = if routed_operation_mode && node_signer.is_some() {
            certificate_scopes
                .iter()
                .filter_map(|configured| {
                    let namespace_snapshot = mounted_namespace.as_ref()?.clone();
                    let authority_here = namespace_snapshot
                        .operation_authority_definition(
                            &configured.namespace_id,
                            &configured.key_prefix,
                        )
                        .is_some_and(|definition| definition.authority_nodes.contains(&node_id));
                    authority_here.then_some(AuthorityTerminalRole {
                        namespace: namespace_snapshot,
                        scope: OperationScope {
                            namespace_id: configured.namespace_id.clone(),
                            key_prefix: configured.key_prefix.clone(),
                        },
                    })
                })
                .collect()
        } else {
            Vec::new()
        };
        let route_digest = routed_config
            .as_ref()
            .map(|(routes, required)| routed_custody_manifest_digest(routes, required, &neighbors))
            .transpose()
            .unwrap_or_else(|error| {
                eprintln!("error: invalid operation route manifest: {error}");
                std::process::exit(1);
            });
        let authority_digest = mounted_namespace
            .as_ref()
            .map(|mounted| {
                routed_authority_manifest_digest(&node_id, &authority_terminals, Some(mounted))
            })
            .transpose()
            .unwrap_or_else(|error| {
                eprintln!("error: invalid operation Authority assignment: {error}");
                std::process::exit(1);
            });
        if let Some(policy_required) = &policy_destinations {
            let routes = policy_routes
                .as_ref()
                .expect("policy-only delivery requires routes");
            OperationPolicyOutbox::verify_pin(
                data_dir.join("operation_policy_outbox"),
                &node_id,
                policy_route_digest(routes, policy_required, &neighbors),
                operation_link
                    .as_ref()
                    .expect("routed configuration has link authentication")
                    .manifest_digest(),
            )
            .unwrap_or_else(|error| {
                eprintln!("error: policy-only outbox manifest recovery failed: {error}");
                std::process::exit(1);
            });
        }
        {
            let inbox_guard = inbox.lock().await;
            let mut outbox_guard = outbox.lock().await;
            pin_operation_startup_manifests(
                &inbox_guard,
                &mut outbox_guard,
                Some(creation_policy.digest()),
                operation_link.as_ref().map(|link| link.manifest_digest()),
                route_digest,
                owned.digest(),
                authority_digest,
                routed_config.as_ref().map(|(routes, _)| {
                    (
                        &neighbors,
                        routes,
                        authority_terminals.as_slice(),
                        mounted_namespace
                            .as_ref()
                            .expect("routed mode has a mounted namespace"),
                    )
                }),
            )
            .unwrap_or_else(|error| {
                eprintln!("error: operation startup manifest recovery failed: {error}");
                std::process::exit(1);
            });
        }
        if let Some(policy_required) = policy_destinations {
            let routes = policy_routes
                .as_ref()
                .expect("policy-only delivery requires routes");
            let link = operation_link
                .as_ref()
                .expect("routed configuration has link authentication")
                .clone();
            let policy_outbox = Arc::new(Mutex::new(
                OperationPolicyOutbox::open(
                    data_dir.join("operation_policy_outbox"),
                    node_id.clone(),
                    policy_route_digest(routes, &policy_required, &neighbors),
                    link.manifest_digest(),
                )
                .unwrap_or_else(|error| {
                    eprintln!("error: policy-only outbox recovery failed: {error}");
                    std::process::exit(1);
                }),
            ));
            let policy_relay = PolicyWitnessRelay::new(
                Arc::clone(&inbox),
                policy_outbox,
                neighbors.clone(),
                owned.clone(),
                routes.clone(),
                policy_required,
            )
            .await
            .unwrap_or_else(|error| {
                eprintln!("error: policy-only delivery reconciliation failed: {error}");
                std::process::exit(1);
            });
            app = app.merge(policy_witness_submit_router(policy_relay.clone()));
            app = app.merge(policy_witness_router_authenticated(
                policy_relay.clone(),
                token.clone(),
                link.clone(),
            ));
            operation_policy_retry = Some((policy_relay, token.clone(), link));
        }
        // A reclaimed local origin may also have been this node's Authority
        // signing input. Verify durable root/admission logs and seed the
        // inbox's exact issuance witnesses before relay startup examines its
        // root tombstones. The workers remain locked and are attached to
        // certificate states below.
        let mut preopened_authorities = Vec::with_capacity(certificate_scopes.len());
        for (configured, scoped_registry) in certificate_scopes.iter().zip(&scope_keysets) {
            let Some(signer) = node_signer.as_ref() else {
                preopened_authorities.push(None);
                continue;
            };
            let namespace_snapshot = namespace
                .read()
                .unwrap_or_else(|_| {
                    eprintln!("error: system namespace is poisoned");
                    std::process::exit(1);
                })
                .clone();
            let scope = OperationScope {
                namespace_id: configured.namespace_id.clone(),
                key_prefix: configured.key_prefix.clone(),
            };
            let authority_here = namespace_snapshot
                .operation_authority_definition(&scope.namespace_id, &scope.key_prefix)
                .is_some_and(|definition| definition.authority_nodes.contains(&node_id));
            if !authority_here {
                preopened_authorities.push(None);
                continue;
            }
            let worker_dir = if multi_scope_mode {
                certificate_scope_root(&data_dir, &configured.namespace_id, &configured.key_prefix)
                    .join("operation_authority")
            } else {
                data_dir.join("operation_authority")
            };
            let keyset_snapshot = scoped_registry.clone();
            let keyset_version = keyset_snapshot.current_version();
            let worker = OperationAuthorityWorker::open_with_creation_policy(
                worker_dir,
                namespace_snapshot,
                configured.namespace_id.clone(),
                scope,
                Arc::clone(signer),
                Arc::clone(&writer_registry),
                Some(Arc::clone(&creation_policy)),
                Arc::new(keyset_snapshot),
                keyset_version,
            )
            .unwrap_or_else(|e| {
                eprintln!("error: operation Authority recovery failed: {e}");
                std::process::exit(1);
            });
            {
                let mut guard = inbox.lock().await;
                worker
                    .seed_verified_local_issuance(&mut guard)
                    .unwrap_or_else(|e| {
                        eprintln!("error: operation Authority issuance recovery failed: {e}");
                        std::process::exit(1);
                    });
            }
            preopened_authorities.push(Some(worker));
        }
        let relay = match routed_config {
            Some((routes, required)) => {
                OperationRelay::new_routed_with_authorities_and_policy(
                    inbox,
                    Arc::clone(&outbox),
                    neighbors.clone(),
                    owned.clone(),
                    routes,
                    required,
                    authority_terminals,
                    mounted_namespace.expect("routed mode has a mounted namespace"),
                    Arc::clone(&namespace),
                )
                .await
            }
            None => {
                OperationRelay::new(inbox, Arc::clone(&outbox), neighbors.clone(), owned.clone())
                    .await
            }
        }
        .unwrap_or_else(|e| {
            eprintln!("error: operation delivery reconciliation failed: {e}");
            std::process::exit(1);
        });
        let certificate_inbox = relay.inbox();
        let submit_relay = relay.clone();
        let mut certificate_states = Vec::new();
        // This also exposes public ordinary reads over the same owned inbox.
        app = app.merge(submit_router(relay.clone()));
        app = app.merge(match &operation_link {
            Some(link) => relay_router_authenticated(relay, token.clone(), link.clone()),
            None => relay_router(relay, token.clone()),
        });
        for (index, (configured, scoped_registry)) in certificate_scopes
            .into_iter()
            .zip(scope_keysets)
            .enumerate()
        {
            let scope_namespace_id = configured.namespace_id;
            let prefix = configured.key_prefix;
            let faults = configured.faults;
            let scope_root = multi_scope_mode
                .then(|| certificate_scope_root(&data_dir, &scope_namespace_id, &prefix));
            let scope_dir = |legacy: &str| {
                scope_root
                    .as_ref()
                    .map_or_else(|| data_dir.join(legacy), |root| root.join(legacy))
            };
            let scope = OperationScope {
                namespace_id: scope_namespace_id.clone(),
                key_prefix: prefix,
            };
            let trusted_registry = Arc::new(std::sync::RwLock::new(scoped_registry));
            let store = {
                let keyset_guard = trusted_registry.read().unwrap_or_else(|_| {
                    eprintln!("error: Authority keyset registry is poisoned");
                    std::process::exit(1);
                });
                let namespace_guard = namespace.read().unwrap_or_else(|_| {
                    eprintln!("error: system namespace is poisoned");
                    std::process::exit(1);
                });
                let policy_digest =
                    static_read_trust_digest(&namespace_guard, &scope, &keyset_guard, &owned)
                        .unwrap_or_else(|e| {
                            eprintln!("error: invalid static operation Authority scope: {e}");
                            std::process::exit(1);
                        });
                let verifier = EvidenceVerifier {
                    registry: &keyset_guard,
                    scope: &scope,
                    current_epoch: 0,
                    epoch_config: &epoch_config,
                    faults,
                };
                StaticOperationEvidenceStore::open(
                    scope_dir("operation_evidence"),
                    &verifier,
                    policy_digest,
                )
                .unwrap_or_else(|e| {
                    eprintln!("error: operation evidence recovery failed: {e}");
                    std::process::exit(1);
                })
            };
            let supports = {
                let keyset_guard = trusted_registry.read().unwrap_or_else(|_| {
                    eprintln!("error: Authority keyset registry is poisoned");
                    std::process::exit(1);
                });
                asteroidb_poc::authority::operation_support_store::OperationSupportStore::open(
                    scope_dir("operation_supports"),
                    &keyset_guard,
                    &scope,
                    &epoch_config,
                )
                .unwrap_or_else(|e| {
                    eprintln!("error: operation support recovery failed: {e}");
                    std::process::exit(1);
                })
            };
            supports
                .check_neighbor_manifest(neighbors.digest())
                .unwrap_or_else(|e| {
                    eprintln!("error: operation support neighbor recovery failed: {e}");
                    std::process::exit(1);
                });
            if let Some(root) = &scope_root {
                pin_certificate_scope_used(&data_dir, root).unwrap_or_else(|error| {
                    eprintln!("error: certificate scope marker failed: {error}");
                    std::process::exit(1);
                });
            }
            {
                let inbox = certificate_inbox.lock().await;
                for receipt in supports.compact_receipts() {
                    if inbox.receipt(&receipt.subject).as_ref() != Some(receipt) {
                        eprintln!(
                            "error: compact operation support differs from authenticated ledger receipt"
                        );
                        std::process::exit(1);
                    }
                }
            }
            let mut support_outbox = SupportOutbox::open(scope_dir("operation_support_outbox"))
                .unwrap_or_else(|e| {
                    eprintln!("error: operation support outbox recovery failed: {e}");
                    std::process::exit(1);
                });
            if let Some(link) = &operation_link {
                support_outbox
                    .ensure_link_manifest(link.manifest_digest())
                    .unwrap_or_else(|e| {
                        eprintln!("error: operation support link identity recovery failed: {e}");
                        std::process::exit(1);
                    });
            }
            for (operation, support) in supports.packets() {
                let packet = asteroidb_poc::network::operation_support_transport::SupportPacket {
                    operation,
                    support,
                };
                for addr in neighbors.permitted_addrs_for(
                    &packet.operation.operation.subject.namespace_id,
                    &packet.operation.declaration.key,
                ) {
                    support_outbox
                        .ensure_enqueued(addr, packet.clone())
                        .unwrap_or_else(|e| {
                            eprintln!(
                                "error: operation support delivery reconciliation failed: {e}"
                            );
                            std::process::exit(1);
                        });
                }
            }
            let support_outbox = Arc::new(Mutex::new(support_outbox));
            let mut store = store;
            for subject in supports.subjects() {
                let certificate = supports
                    .assemble_certificate(subject, faults)
                    .unwrap_or_else(|e| {
                        eprintln!("error: operation support assembly failed: {e}");
                        std::process::exit(1);
                    });
                if let Some(certificate) = certificate {
                    let keyset_guard = trusted_registry.read().unwrap_or_else(|_| {
                        eprintln!("error: Authority keyset registry is poisoned");
                        std::process::exit(1);
                    });
                    let namespace_guard = namespace.read().unwrap_or_else(|_| {
                        eprintln!("error: system namespace is poisoned");
                        std::process::exit(1);
                    });
                    let digest =
                        static_read_trust_digest(&namespace_guard, &scope, &keyset_guard, &owned)
                            .unwrap_or_else(|e| {
                                eprintln!("error: invalid static operation Authority scope: {e}");
                                std::process::exit(1);
                            });
                    let verifier = EvidenceVerifier {
                        registry: &keyset_guard,
                        scope: &scope,
                        current_epoch: 0,
                        epoch_config: &epoch_config,
                        faults,
                    };
                    store
                        .record_and_sync(certificate, &verifier, digest)
                        .unwrap_or_else(|e| {
                            eprintln!("error: operation evidence reconciliation failed: {e}");
                            std::process::exit(1);
                        });
                }
            }
            let certificate_outbox = CertificateOutbox::open(scope_dir(
                "operation_certificate_outbox",
            ))
            .unwrap_or_else(|e| {
                eprintln!("error: operation certificate outbox recovery failed: {e}");
                std::process::exit(1);
            });
            if let Some(link) = &operation_link {
                certificate_outbox
                    .ensure_link_manifest(link.manifest_digest())
                    .unwrap_or_else(|e| {
                        eprintln!(
                            "error: operation certificate link identity recovery failed: {e}"
                        );
                        std::process::exit(1);
                    });
            }
            let certificate_outbox = Arc::new(Mutex::new(certificate_outbox));
            let certificate_state = OperationCertificateState {
                inbox: Arc::clone(&certificate_inbox),
                store: Arc::new(Mutex::new(store)),
                supports: Arc::new(Mutex::new(supports)),
                support_outbox: Arc::clone(&support_outbox),
                certificate_outbox: Arc::clone(&certificate_outbox),
                neighbors: neighbors.clone(),
                owned: owned.clone(),
                registry: Arc::clone(&trusted_registry),
                namespace: Arc::clone(&namespace),
                scope: scope.clone(),
                epoch_config: epoch_config.clone(),
                faults,
                link_auth: operation_link.clone(),
            };
            let startup_reconcile = certificate_state.clone();
            tokio::task::spawn_blocking(move || {
                startup_reconcile.reconcile_evidence_delivery_blocking()
            })
            .await
            .unwrap_or_else(|e| {
                eprintln!("error: operation evidence delivery reconciliation task failed: {e}");
                std::process::exit(1);
            })
            .unwrap_or_else(|e| {
                eprintln!("error: operation evidence delivery reconciliation failed: {e}");
                std::process::exit(1);
            });
            if routed_operation_mode {
                let gc_state = certificate_state.clone();
                let gc_relay = submit_relay.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
                    loop {
                        interval.tick().await;
                        let state = gc_state.clone();
                        let relay = gc_relay.clone();
                        match tokio::task::spawn_blocking(move || {
                            state.compact_ledger_bodies_blocking(&relay)
                        })
                        .await
                        {
                            Ok(Ok(_)) => {}
                            Ok(Err(error)) => eprintln!("operation ledger GC failed: {error}"),
                            Err(error) => eprintln!("operation ledger GC task failed: {error}"),
                        }
                    }
                });
            }
            if let Some(worker) = preopened_authorities[index].take() {
                operation_authority.push((
                    worker,
                    certificate_state.clone(),
                    neighbors.clone(),
                    Arc::clone(&support_outbox),
                    routed_operation_mode,
                ));
            }
            operation_support_retry.push((support_outbox, token.clone(), operation_link.clone()));
            operation_certificate_retry.push((
                certificate_outbox,
                token.clone(),
                operation_link.clone(),
                certificate_state.clone(),
            ));
            certificate_states.push(certificate_state);
        }
        let view_certificates = if certificate_states.is_empty() {
            app = app.merge(public_operation_uncertified_router(submit_relay.clone()));
            None
        } else {
            let states =
                MultiOperationCertificateState::new(certificate_states).unwrap_or_else(|error| {
                    eprintln!("error: invalid operation certificate scopes: {error}");
                    std::process::exit(1);
                });
            app = app.merge(multi_certified_submit_router(
                submit_relay.clone(),
                states.clone(),
            ));
            app = app.merge(multi_public_operation_status_router(states.clone()));
            app = app.merge(multi_certificate_router(states.clone(), token.clone()));
            Some(states)
        };
        app = app.merge(unified_operation_view_router_multi(
            submit_relay.clone(),
            view_certificates,
        ));
        operation_retry = Some((outbox, submit_relay.clone(), token, operation_link));
    }
    if operation_retry.is_none()
        && (std::env::var("ASTEROIDB_OPERATION_CERT_SCOPE_PREFIX").is_ok()
            || std::env::var("ASTEROIDB_OPERATION_CERT_FAULTS").is_ok()
            || std::env::var("ASTEROIDB_OPERATION_CERT_SCOPES").is_ok())
    {
        eprintln!("error: operation certificates require ASTEROIDB_WRITER_KEYS");
        std::process::exit(1);
    }
    if operation_retry.is_none() && std::env::var("ASTEROIDB_OPERATION_CREATION_POLICY").is_ok() {
        eprintln!("error: operation creation policy requires ASTEROIDB_WRITER_KEYS");
        std::process::exit(1);
    }

    // Ops kill switch for digest-based anti-entropy: when set, the sync
    // loop skips the digest probe and uses the legacy full-sync fallbacks
    // only (see docs/ops-guide.md).
    let digest_sync_enabled = !std::env::var("ASTEROIDB_DIGEST_SYNC_DISABLED")
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true"
        })
        .unwrap_or(false);

    // Ops kill switch for the certified replication lane (P0-1): when
    // set, the sync loop issues no certified pulls at all, restoring the
    // pre-lane behaviour exactly (see docs/ops-guide.md).
    let certified_sync_enabled = !std::env::var("ASTEROIDB_CERTIFIED_SYNC_DISABLED")
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true"
        })
        .unwrap_or(false);

    // Minimum age a tombstone-GC mark must reach before its sweep may
    // collect (see NodeRunner::run_gc). Collection additionally requires
    // every authority frontier and every registered peer's push frontier
    // to have passed the mark time, so raising this only adds slack.
    let gc_retention = std::time::Duration::from_secs(
        std::env::var("ASTEROIDB_GC_RETENTION_SECS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(300),
    );

    // Ops kill switch for the store-digest frontier report format (M-12):
    // when set to 0/false, authority reports keep the legacy placeholder
    // digest_hash. Takes effect on restart, which is safe by construction —
    // the report clock floor guarantees the post-restart HLCs are fresh, so
    // the two formats never meet at the same frontier HLC.
    let frontier_store_digest = !std::env::var("ASTEROIDB_FRONTIER_STORE_DIGEST")
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "0" || v == "false"
        })
        .unwrap_or(false);

    let runner_config = NodeRunnerConfig {
        legacy_data_plane_enabled: !operation_mode,
        bls_config,
        node_signer,
        keyset_registry,
        internal_token: internal_token.clone(),
        current_epoch: Some(Arc::clone(&current_epoch)),
        equivocation: Some(Arc::clone(&equivocation)),
        // Same flag as AppState: without this, the self-report path would
        // keep pooling this node's own attestations while the HTTP path
        // excludes them (m-7).
        exclude_accused_authorities,
        digest_sync_enabled,
        certified_sync_enabled,
        gc_retention,
        frontier_store_digest,
        // Write-ahead floor for frontier report HLCs (M-12): lives next to
        // the other node state; without it the store-digest format never
        // activates (fail-safe).
        frontier_clock_floor_path: Some(data_dir.join("frontier_report_clock.json")),
        ..NodeRunnerConfig::default()
    };

    // NodeRunner uses the same CertifiedApi and EventualApi instances
    // for background processing, ensuring sync sees HTTP writes.
    // Always create a SyncClient so that peers added dynamically via
    // /api/internal/join are picked up by anti-entropy sync (the sync
    // loop skips when the peer list is empty, so there is no overhead).
    let engine = CompactionEngine::with_defaults();
    let sync_client = if let Some(ref token) = internal_token {
        SyncClient::with_token(Arc::clone(&shared_peers), token.clone())
    } else {
        SyncClient::new(Arc::clone(&shared_peers))
    };
    // Build membership client for fan-out join/leave and periodic ping.
    let membership_client = if let Some(ref token) = internal_token {
        MembershipClient::with_token(
            node_id.clone(),
            advertise_addr.clone(),
            Arc::clone(&shared_peers),
            token.clone(),
        )
    } else {
        MembershipClient::new(
            node_id.clone(),
            advertise_addr.clone(),
            Arc::clone(&shared_peers),
        )
    };

    let mut runner = NodeRunner::with_sync_and_cluster_nodes(
        node_id.clone(),
        Arc::clone(&certified_api),
        engine,
        runner_config,
        sync_client,
        Arc::clone(&eventual_api),
        Arc::clone(&metrics),
        Arc::clone(&shared_cluster_nodes),
    )
    .await;
    // Give the runner a real cluster inventory. Nothing populated
    // `cluster_nodes` before this, so `GET /api/topology` always reported an
    // empty cluster. This also freezes FR-003 / FR-007 until peer identity is
    // propagated on the wire — see `placement_inventory_usable`.
    runner.set_cluster_inventory_source(self_node.clone(), Arc::clone(&shared_peers));

    // Build a second membership client for the runner's periodic ping loop.
    let runner_membership_client = if let Some(ref token) = internal_token {
        MembershipClient::with_token(
            node_id.clone(),
            advertise_addr.clone(),
            Arc::clone(&shared_peers),
            token.clone(),
        )
    } else {
        MembershipClient::new(
            node_id.clone(),
            advertise_addr.clone(),
            Arc::clone(&shared_peers),
        )
    };
    runner.set_membership_client(runner_membership_client);
    runner.set_slo_tracker(slo_tracker);
    runner.set_latency_model(Arc::clone(&shared_latency_model));
    runner.set_topology_view(Arc::clone(&shared_topology_view));

    let shutdown_handle = runner.shutdown_handle();

    if let Some((outbox, relay, token, link)) = operation_retry {
        spawn_operation_retry_worker_with_auth(
            outbox,
            token.clone(),
            link.clone(),
            shutdown_handle.subscribe(),
        );
        spawn_operation_completion_worker_with_auth(
            relay,
            token,
            link,
            shutdown_handle.subscribe(),
        );
    }
    if let Some((relay, token, link)) = operation_policy_retry {
        spawn_policy_witness_retry_worker(relay, token, link, shutdown_handle.subscribe());
    }
    for (support_outbox, token, link) in operation_support_retry {
        spawn_support_retry_worker_with_auth(
            support_outbox,
            token,
            link,
            shutdown_handle.subscribe(),
        );
    }
    for (certificate_outbox, token, link, state) in operation_certificate_retry {
        spawn_certificate_retry_worker_with_auth(
            certificate_outbox,
            token,
            link,
            shutdown_handle.subscribe(),
        );
        let mut shutdown = shutdown_handle.subscribe();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                    _ = ticker.tick() => {
                        if *shutdown.borrow() { break; }
                        let state = state.clone();
                        match tokio::task::spawn_blocking(move || state.reconcile_evidence_delivery_blocking()).await {
                            Ok(Ok(())) => {}
                            other => tracing::warn!(error = ?other, "operation evidence delivery reconciliation failed"),
                        }
                    }
                }
            }
        });
    }
    for (worker, certificate_state, neighbors, support_outbox, routed) in operation_authority {
        let mut shutdown = shutdown_handle.subscribe();
        tokio::spawn(async move {
            let recovery = worker.recovery_config();
            let mut worker = Some(worker);
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut ticks = 0u64;
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                    _ = ticker.tick() => {
                        if worker.is_none() {
                            let config = recovery.clone();
                            match tokio::task::spawn_blocking(move || config.open()).await {
                                Ok(Ok(recovered)) => worker = Some(recovered),
                                Ok(Err(error)) => {
                                    eprintln!("error: operation Authority recovery failed; retrying: {error}");
                                    continue;
                                }
                                Err(error) => {
                                    eprintln!("error: operation Authority recovery task failed; retrying: {error}");
                                    continue;
                                }
                            }
                        }
                        let mut current = worker.take().expect("Authority worker was recovered");
                        let state = certificate_state.clone();
                        let gc_neighbors = neighbors.clone();
                        let gc_due = ticks.is_multiple_of(30);
                        ticks = ticks.wrapping_add(1);
                        let result = tokio::task::spawn_blocking(move || {
                            let pass = (|| -> Result<_, OperationAuthorityPassError> {
                                state.check_authority_trust_blocking().map_err(|error| match error {
                                    AuthorityTrustCheckError::Retryable(message) => OperationAuthorityPassError::RetryableShared(message),
                                    AuthorityTrustCheckError::Permanent(message) => OperationAuthorityPassError::Shared(message),
                                })?;
                                {
                                    let mut inbox = state.inbox.blocking_lock();
                                    if routed {
                                        current.reconcile_routed(&mut inbox).map_err(|e| OperationAuthorityPassError::Worker(e.to_string()))?;
                                    } else {
                                        current.reconcile(&mut inbox).map_err(|e| OperationAuthorityPassError::Worker(e.to_string()))?;
                                    }
                                }
                                let packets: Vec<_> = current.issued_supports().collect();
                                for packet in &packets {
                                    state.record_authority_support_blocking(packet.clone())
                                        .map_err(|error| match error {
                                            AuthorityTrustCheckError::Retryable(message) => OperationAuthorityPassError::RetryableShared(message),
                                            AuthorityTrustCheckError::Permanent(message) => OperationAuthorityPassError::Shared(message),
                                        })?;
                                }
                                if gc_due {
                                    let receipts: Vec<_> = packets
                                        .iter()
                                        .map(|packet| asteroidb_poc::operation::OperationReceipt::from(&packet.operation))
                                        .collect();
                                    let verified = state.verified_receipt_subjects_blocking(&receipts)
                                        .map_err(|error| match error {
                                            AuthorityTrustCheckError::Retryable(message) => OperationAuthorityPassError::Gc(message),
                                            AuthorityTrustCheckError::Permanent(message) => OperationAuthorityPassError::Shared(message),
                                        })?;
                                    let outbox = state.support_outbox.blocking_lock();
                                    let mut releasable: Vec<_> = packets
                                        .iter()
                                        .zip(receipts)
                                        .filter_map(|(packet, receipt)| {
                                            (verified.contains(&receipt.subject)
                                                && gc_neighbors
                                                    .permitted_for(&receipt.subject.namespace_id, &receipt.declaration.key)
                                                    .all(|neighbor| outbox.has_durable_receipt(&neighbor.addr, packet)))
                                            .then_some(receipt)
                                        })
                                        .collect();
                                    drop(outbox);
                                    releasable.extend(current.retired_receipts().map_err(|e| OperationAuthorityPassError::Worker(e.to_string()))?);
                                    if !releasable.is_empty() {
                                        state.pin_support_neighbor_manifest_blocking()
                                            .map_err(|error| match error {
                                                AuthorityTrustCheckError::Retryable(message) => OperationAuthorityPassError::Gc(message),
                                                AuthorityTrustCheckError::Permanent(message) => OperationAuthorityPassError::Shared(message),
                                            })?;
                                        current.compact_issued_receipted_and_sync(&releasable)
                                            .map_err(|e| OperationAuthorityPassError::Worker(e.to_string()))?;
                                    }
                                }
                                Ok(packets)
                            })();
                            (current, pass)
                        }).await;
                        let packets = match result {
                            Ok((recovered_worker, Ok(packets))) => {
                                worker = Some(recovered_worker);
                                packets
                            }
                            Ok((failed_worker, Err(OperationAuthorityPassError::Worker(error)))) => {
                                drop(failed_worker);
                                eprintln!("error: operation Authority worker failed; reopening: {error}");
                                continue;
                            }
                            Ok((failed_worker, Err(OperationAuthorityPassError::RetryableShared(error)))) => {
                                drop(failed_worker);
                                eprintln!("error: operation Authority shared state unavailable; retrying: {error}");
                                continue;
                            }
                            Ok((failed_worker, Err(OperationAuthorityPassError::Gc(error)))) => {
                                drop(failed_worker);
                                eprintln!("error: operation Authority GC check failed; retrying: {error}");
                                continue;
                            }
                            Ok((failed_worker, Err(OperationAuthorityPassError::Shared(error)))) => {
                                drop(failed_worker);
                                eprintln!("error: operation Authority shared state failed; worker stopped: {error}");
                                break;
                            }
                            Err(error) => {
                                eprintln!("error: operation Authority worker panicked: {error}");
                                break;
                            }
                        };
                        let mut outbox = support_outbox.lock().await;
                        for packet in packets {
                            for addr in neighbors.permitted_addrs_for(
                                &packet.operation.operation.subject.namespace_id,
                                &packet.operation.declaration.key,
                            ) {
                                if let Err(error) = outbox.ensure_enqueued(addr, packet.clone()) {
                                    eprintln!("error: operation support enrollment failed; retrying: {error}");
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    // Control-plane Raft driver: randomized election timer + heartbeats,
    // as an independent task (NodeRunner's select! stays clean), linked to
    // the runner's shutdown channel. No special shutdown persistence is
    // needed: every Raft state change is fsynced on the write path.
    asteroidb_poc::control_plane::raft::spawn_raft_driver(
        Arc::clone(&raft_node),
        shutdown_handle.subscribe(),
    );

    // Background persistence: WAL group-commit flushers + periodic
    // checkpoints (snapshot, then prune sealed WAL segments).
    if !operation_mode {
        asteroidb_poc::runtime::persistence::spawn_persistence_tasks(
            persistence_cfg.clone(),
            Arc::clone(&eventual_api),
            Arc::clone(&certified_api),
            eventual_wal_syncer,
            certified_wal_syncer,
        );
    }

    // Bind the TCP listener.
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind to {bind_addr}: {e}"));

    println!("HTTP server listening on {bind_addr}");
    if advertise_addr != bind_addr {
        println!("Advertise address: {advertise_addr}");
    }
    println!("Node run loop started. Press Ctrl-C to stop.");

    // Fan-out join: announce this node's presence to all known peers.
    // Spawned as a background task so that unreachable peers do not
    // block the server startup (Codex P1).
    tokio::spawn(async move {
        let fan_out_count = membership_client.fan_out_join().await;
        if fan_out_count > 0 {
            println!("Fan-out join announced to {fan_out_count} peers");
        }
    });

    // Build a membership client for the shutdown path so we can
    // announce departure before stopping.
    let shutdown_membership_client = if let Some(ref token) = internal_token {
        MembershipClient::with_token(
            node_id,
            advertise_addr.clone(),
            Arc::clone(&shared_peers),
            token.clone(),
        )
    } else {
        MembershipClient::new(node_id, advertise_addr.clone(), Arc::clone(&shared_peers))
    };

    // wait_for_signal() resolves on SIGINT (Ctrl-C) on all platforms, and
    // additionally on SIGTERM on Unix (Kubernetes/Docker graceful shutdown).
    // TODO: axum::serve should use with_graceful_shutdown() so in-flight HTTP
    // requests drain before the process exits. This requires restructuring the
    // select! to pass a oneshot channel to axum and wait for it after signalling.
    // Track as a follow-up issue.
    tokio::select! {
        result = axum::serve(listener, app) => {
            if let Err(e) = result {
                eprintln!("HTTP server error: {e}");
            }
        }
        _stats = runner.run() => {
            println!("NodeRunner exited.");
        }
        _ = wait_for_signal() => {
            println!("\nShutting down...");
            // Announce departure to all peers before stopping (P1-1).
            let leave_count = shutdown_membership_client.fan_out_leave().await;
            if leave_count > 0 {
                println!("Fan-out leave acknowledged by {leave_count} peers");
            }
            // Persist system namespace before stopping.
            {
                let ns = namespace.read().unwrap();
                if let Err(e) = ns.save(&ns_persist_path) {
                    eprintln!("warning: failed to save system namespace on shutdown: {e}");
                } else {
                    println!("System namespace saved to {}", ns_persist_path.display());
                }
            }
            // Final checkpoint: after a graceful shutdown WAL replay work
            // is minimal, but the WAL directory is NOT empty — rotation
            // always leaves a header-only active segment behind (pruning
            // only removes sealed segments), and in-flight HTTP handlers
            // can still append records after this point (no
            // with_graceful_shutdown yet, see the TODO above). WAL format
            // upgrades cannot rely on graceful shutdown alone. An older
            // experimental WAL format is rejected on restart; moving its
            // data requires an explicit migration. Checkpoint failures
            // are non-fatal because the current WAL still holds the data.
            if persistence_cfg.enabled && !operation_mode {
                if let Err(e) = asteroidb_poc::runtime::persistence::checkpoint_eventual(
                    &eventual_api,
                    &persistence_cfg,
                )
                .await
                {
                    eprintln!("warning: eventual checkpoint on shutdown failed: {e}");
                }
                if let Err(e) = asteroidb_poc::runtime::persistence::checkpoint_certified(
                    &certified_api,
                    &persistence_cfg,
                )
                .await
                {
                    eprintln!("warning: certified checkpoint on shutdown failed: {e}");
                } else {
                    println!("Store checkpoints saved to {}", data_dir.display());
                }
            }
            let _ = shutdown_handle.send(true);
        }
    }

    println!("AsteroidDB stopped.");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_mode_ignores_damaged_old_data_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("eventual.snapshot.bin"), b"damaged").unwrap();
        std::fs::write(dir.path().join("certified.snapshot.bin"), b"damaged").unwrap();
        let mut config = PersistenceConfig::from_env(dir.path().to_path_buf());
        config.enabled = true;
        let node = NodeId("node-1".into());
        let namespace = Arc::new(RwLock::new(SystemNamespace::new()));

        let (_, certified_syncer) =
            open_old_certified_for_mode(node.clone(), Arc::clone(&namespace), &config, true)
                .unwrap();
        let (_, eventual_syncer) = open_old_eventual_for_mode(node.clone(), &config, true).unwrap();
        assert!(certified_syncer.is_none());
        assert!(eventual_syncer.is_none());
        assert!(open_old_certified_for_mode(node.clone(), namespace, &config, false).is_err());
        assert!(open_old_eventual_for_mode(node, &config, false).is_err());
    }

    #[test]
    fn operation_mode_requires_a_valid_creation_policy_manifest() {
        assert!(required_operation_creation_policy(None).is_err());
        assert!(required_operation_creation_policy(Some("{}")).is_err());
        let manifest = r#"{"version":1,"ranges":[{"namespace_id":"ns","key_prefix":"","default_mode":"Disabled","override_writers":[]}]}"#;
        assert!(required_operation_creation_policy(Some(manifest)).is_ok());
    }

    #[test]
    fn policy_routes_can_be_configured_without_routed_data_delivery() {
        let destinations =
            OperationRequiredDestinations::from_json(r#"[{"key_prefix":"","destinations":["c"]}]"#)
                .unwrap();
        let edges = r#"[{"source":"a","receiver":"b","key_prefixes":[""]},
                        {"source":"b","receiver":"c","key_prefixes":[""]}]"#;
        let rows = r#"[{"source":"a","destination":"c","key_prefix":"","next_hop":"b","rank":2},
                       {"source":"b","destination":"c","key_prefix":"","next_hop":"c","rank":1}]"#;
        let routes = policy_route_configuration(Some(edges), Some(rows), Some(&destinations), None)
            .unwrap()
            .unwrap();
        assert_eq!(routes.rows().len(), 2);

        let dir = tempfile::tempdir().unwrap();
        let inbox =
            OperationInbox::open(dir.path().join("inbox"), Arc::new(PermitOperation)).unwrap();
        let mut outbox =
            OperationOutbox::open_for_node(dir.path().join("outbox"), NodeId("a".into())).unwrap();
        assert!(!outbox.is_routed());
        pin_operation_startup_manifests(
            &inbox,
            &mut outbox,
            Some([1; 32]),
            Some([2; 32]),
            None,
            [3; 32],
            None,
            None,
        )
        .unwrap();
        assert!(inbox.verify_link_manifest([2; 32]).is_ok());
        assert!(outbox.verify_link_manifest([2; 32]).is_ok());
        assert!(outbox.verify_link_manifest([4; 32]).is_err());
        assert!(policy_route_configuration(None, None, Some(&destinations), None).is_err());
        assert!(policy_route_configuration(Some(edges), None, Some(&destinations), None).is_err());
        assert!(policy_route_configuration(Some(edges), Some(rows), None, None).is_err());
    }

    struct PermitOperation;

    impl asteroidb_poc::network::operation_transport::OperationAdmission for PermitOperation {
        fn admit(
            &self,
            _: &asteroidb_poc::authority::writer_auth::SignedBoundOperation,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn route_repin_checks_other_pins_before_mutating_pristine_outbox() {
        let dir = tempfile::tempdir().unwrap();
        let inbox_dir = dir.path().join("inbox");
        let outbox_dir = dir.path().join("outbox");
        let inbox = OperationInbox::open(&inbox_dir, Arc::new(PermitOperation)).unwrap();
        let mut outbox =
            OperationOutbox::open_routed_for_node(&outbox_dir, NodeId("a".into())).unwrap();
        outbox.ensure_routed_manifest([1; 32]).unwrap();
        inbox.ensure_owned_manifest([7; 32]).unwrap();
        inbox.ensure_authority_manifest([8; 32]).unwrap();

        assert!(
            pin_operation_startup_manifests(
                &inbox,
                &mut outbox,
                Some([3; 32]),
                Some([4; 32]),
                Some([2; 32]),
                [5; 32],
                Some([6; 32]),
                None,
            )
            .is_err()
        );
        outbox.verify_routed_manifest([1; 32]).unwrap();
        assert!(!inbox_dir.join(".creation-policy-manifest").exists());
        assert!(!inbox_dir.join(".link-manifest").exists());
        assert!(!outbox_dir.join(".link-manifest").exists());

        assert!(
            pin_operation_startup_manifests(
                &inbox,
                &mut outbox,
                Some([3; 32]),
                Some([4; 32]),
                Some([1; 32]),
                [5; 32],
                Some([8; 32]),
                None,
            )
            .is_err()
        );
        assert!(
            pin_operation_startup_manifests(
                &inbox,
                &mut outbox,
                Some([3; 32]),
                Some([4; 32]),
                Some([1; 32]),
                [7; 32],
                Some([6; 32]),
                None,
            )
            .is_err()
        );
        assert!(!inbox_dir.join(".creation-policy-manifest").exists());
        assert!(!inbox_dir.join(".link-manifest").exists());
        assert!(!outbox_dir.join(".link-manifest").exists());

        pin_operation_startup_manifests(
            &inbox,
            &mut outbox,
            Some([3; 32]),
            Some([4; 32]),
            Some([2; 32]),
            [7; 32],
            Some([8; 32]),
            None,
        )
        .unwrap();
        outbox.verify_routed_manifest([2; 32]).unwrap();
        assert!(inbox_dir.join(".creation-policy-manifest").exists());
        assert!(inbox_dir.join(".link-manifest").exists());
        assert!(outbox_dir.join(".link-manifest").exists());
    }

    #[test]
    fn invalid_local_route_does_not_leave_new_startup_pins() {
        let dir = tempfile::tempdir().unwrap();
        let inbox_dir = dir.path().join("inbox");
        let outbox_dir = dir.path().join("outbox");
        let inbox = OperationInbox::open(&inbox_dir, Arc::new(PermitOperation)).unwrap();
        let mut outbox =
            OperationOutbox::open_routed_for_node(&outbox_dir, NodeId("a".into())).unwrap();
        let edges = [
            OperationRouteEdge {
                source: NodeId("a".into()),
                receiver: NodeId("b".into()),
                namespace_id: None,
                key_prefixes: vec!["x".into()],
            },
            OperationRouteEdge {
                source: NodeId("b".into()),
                receiver: NodeId("c".into()),
                namespace_id: None,
                key_prefixes: vec!["x".into()],
            },
        ];
        let routes = OperationRouteManifest::from_json(
            r#"[{"source":"a","destination":"c","key_prefix":"x","next_hop":"b","rank":2},{"source":"b","destination":"c","key_prefix":"x","next_hop":"c","rank":1}]"#,
            &edges,
        )
        .unwrap();
        let neighbors = OperationNeighborManifest::from_json(
            r#"[{"node_id":"b","addr":"127.0.0.1:8001","key_prefixes":["y"]}]"#,
            &NodeId("a".into()),
        )
        .unwrap();
        assert!(
            pin_operation_startup_manifests(
                &inbox,
                &mut outbox,
                Some([3; 32]),
                Some([4; 32]),
                Some([1; 32]),
                [5; 32],
                Some([6; 32]),
                Some((&neighbors, &routes, &[], &SystemNamespace::new())),
            )
            .is_err()
        );
        assert!(!inbox_dir.join(".creation-policy-manifest").exists());
        assert!(!inbox_dir.join(".link-manifest").exists());
        assert!(!outbox_dir.join(".link-manifest").exists());
    }

    #[test]
    fn certificate_scope_config_allows_nested_ranges_and_rejects_duplicates() {
        let valid = r#"[{"key_prefix":"orders/","faults":1},{"key_prefix":"users/","faults":0}]"#;
        let (scopes, multi) = parse_certificate_scopes(Some(valid), None, None, "n").unwrap();
        assert!(multi);
        assert_eq!(scopes.len(), 2);
        assert!(parse_certificate_scopes(Some(valid), Some("orders/"), Some("1"), "n").is_err());
        assert!(
            parse_certificate_scopes(
                Some(
                    r#"[{"key_prefix":"orders/","faults":1},{"key_prefix":"orders/x","faults":0}]"#
                ),
                None,
                None,
                "n",
            )
            .is_ok()
        );
        assert!(
            parse_certificate_scopes(
                Some(
                    r#"[{"key_prefix":"orders/","faults":1},{"key_prefix":"orders/","faults":0}]"#
                ),
                None,
                None,
                "n",
            )
            .is_err()
        );
        let (scopes, _) = parse_certificate_scopes(
            Some(r#"[{"namespace_id":"n","key_prefix":"orders/","faults":1},{"namespace_id":"m","key_prefix":"orders/","faults":0}]"#),
            None,
            None,
            "n",
        )
        .unwrap();
        assert_eq!(scopes[1].namespace_id, "m");
        assert!(parse_certificate_scopes(Some("[]"), None, None, "n").is_err());
        let (_, multi) = parse_certificate_scopes(None, Some("orders/"), Some("1"), "n").unwrap();
        assert!(!multi);
        assert!(parse_certificate_scopes(None, None, Some("1"), "n").is_err());
    }

    #[test]
    fn certificate_scope_directory_is_stable_and_distinct() {
        let base = std::path::Path::new("/tmp/operation-cert-test");
        assert_eq!(
            certificate_scope_root(base, "n", "orders/"),
            certificate_scope_root(base, "n", "orders/")
        );
        assert_ne!(
            certificate_scope_root(base, "n", "orders/"),
            certificate_scope_root(base, "n", "users/")
        );
        assert_ne!(
            certificate_scope_root(base, "n", "orders/"),
            certificate_scope_root(base, "m", "orders/")
        );
    }

    #[test]
    fn saved_certificate_scopes_cannot_be_silently_dropped() {
        let temp = tempfile::tempdir().unwrap();
        let order = ConfiguredCertificateScope {
            namespace_id: "n".into(),
            key_prefix: "orders/".into(),
            faults: 0,
        };
        let user = ConfiguredCertificateScope {
            namespace_id: "n".into(),
            key_prefix: "users/".into(),
            faults: 0,
        };
        let saved = certificate_scope_root(temp.path(), "n", &user.key_prefix);
        std::fs::create_dir_all(saved.join("operation_evidence")).unwrap();
        std::fs::write(saved.join("operation_evidence/state"), b"saved").unwrap();
        assert!(
            validate_certificate_state_layout(temp.path(), std::slice::from_ref(&order), true)
                .is_err()
        );
        assert!(
            validate_certificate_state_layout(temp.path(), &[order.clone(), user], true).is_ok()
        );
        assert!(validate_certificate_state_layout(temp.path(), &[order], false).is_err());
        assert!(validate_certificate_state_layout(temp.path(), &[], false).is_err());
    }

    #[test]
    fn marked_certificate_scope_cannot_restart_after_parent_loss() {
        let temp = tempfile::tempdir().unwrap();
        let scope = ConfiguredCertificateScope {
            namespace_id: "n".into(),
            key_prefix: "orders/".into(),
            faults: 0,
        };
        let root = certificate_scope_root(temp.path(), &scope.namespace_id, &scope.key_prefix);
        std::fs::create_dir_all(root.join("operation_evidence")).unwrap();
        std::fs::write(root.join("operation_evidence/state"), b"saved").unwrap();
        pin_certificate_scope_used(temp.path(), &root).unwrap();
        assert!(
            validate_certificate_state_layout(temp.path(), std::slice::from_ref(&scope), true)
                .is_ok()
        );
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
        assert!(
            validate_certificate_state_layout(temp.path(), std::slice::from_ref(&scope), true)
                .is_err()
        );
        assert!(validate_certificate_state_layout(temp.path(), &[], true).is_err());
    }

    #[test]
    fn missing_single_scope_state_directory_keeps_its_saved_layout() {
        for (marker, bytes) in [
            (
                ".operation_supports.operation-supports.used",
                b"ASTOS-USED-1".as_slice(),
            ),
            (
                ".operation_authority.operation-authority.used",
                b"ASTAUTH-USED-1".as_slice(),
            ),
            (".operation-evidence-deadbeef.used", b"".as_slice()),
        ] {
            let temp = tempfile::tempdir().unwrap();
            std::fs::write(temp.path().join(marker), bytes).unwrap();
            let scope = ConfiguredCertificateScope {
                namespace_id: "n".into(),
                key_prefix: "orders/".into(),
                faults: 0,
            };
            assert!(validate_certificate_state_layout(temp.path(), &[], false).is_err());
            assert!(validate_certificate_state_layout(temp.path(), &[scope], true).is_err());
        }
    }

    #[test]
    fn operation_scopes_select_independent_authority_keysets() {
        let mut global = KeysetRegistry::new();
        let keys = ["a", "b", "c"]
            .into_iter()
            .enumerate()
            .map(|(index, id)| {
                (
                    NodeId(id.into()),
                    ed25519_dalek::SigningKey::from_bytes(&[(index + 1) as u8; 32]).verifying_key(),
                )
            })
            .collect();
        global.register_keyset(KeysetVersion(1), 0, keys).unwrap();
        let first =
            operation_scope_keyset(&global, &[NodeId("a".into()), NodeId("b".into())]).unwrap();
        let second = operation_scope_keyset(&global, &[NodeId("c".into())]).unwrap();
        assert_eq!(first.get_keys(&KeysetVersion(1)).unwrap().len(), 2);
        assert_eq!(second.get_keys(&KeysetVersion(1)).unwrap().len(), 1);
        assert!(operation_scope_keyset(&global, &[NodeId("missing".into())]).is_err());
        assert!(
            operation_scope_keyset(&global, &[NodeId("a".into()), NodeId("a".into())]).is_err()
        );
    }

    #[test]
    fn invalid_certificate_scope_is_rejected_before_startup_pins() {
        let dir = tempfile::tempdir().unwrap();
        let inbox_dir = dir.path().join("inbox");
        let outbox_dir = dir.path().join("outbox");
        let inbox = OperationInbox::open(&inbox_dir, Arc::new(PermitOperation)).unwrap();
        let mut outbox =
            OperationOutbox::open_routed_for_node(&outbox_dir, NodeId("a".into())).unwrap();
        let mut namespace = SystemNamespace::new();
        namespace.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "orders/".into(),
            },
            authority_nodes: vec![NodeId("a".into()), NodeId("b".into())],
            auto_generated: false,
        });
        namespace
            .set_placement_policy(
                asteroidb_poc::placement::PlacementPolicy::new(
                    asteroidb_poc::types::PolicyVersion(1),
                    KeyRange {
                        prefix: "orders/".into(),
                    },
                    2,
                )
                .with_certified(true),
            )
            .unwrap();
        let scopes = vec![ConfiguredCertificateScope {
            namespace_id: "n".into(),
            key_prefix: "orders/".into(),
            faults: 0,
        }];
        let owned = OperationOwnedPrefixes::from_json(r#"["orders/"]"#).unwrap();
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![(
                    NodeId("a".into()),
                    ed25519_dalek::SigningKey::from_bytes(&[1; 32]).verifying_key(),
                )],
            )
            .unwrap();
        let preflight = preflight_certificate_scopes(&scopes, &namespace, &registry, &owned);
        assert!(preflight.is_err());
        assert!(!inbox_dir.join(".authority-manifest").exists());
        assert!(!inbox_dir.join(".creation-policy-manifest").exists());
        assert!(!outbox_dir.join(".routing").exists());

        registry
            .register_keyset(
                KeysetVersion(2),
                0,
                vec![
                    (
                        NodeId("a".into()),
                        ed25519_dalek::SigningKey::from_bytes(&[1; 32]).verifying_key(),
                    ),
                    (
                        NodeId("b".into()),
                        ed25519_dalek::SigningKey::from_bytes(&[2; 32]).verifying_key(),
                    ),
                ],
            )
            .unwrap();
        assert_eq!(
            preflight_certificate_scopes(&scopes, &namespace, &registry, &owned)
                .unwrap()
                .len(),
            1
        );
        let too_many_faults = vec![ConfiguredCertificateScope {
            faults: 1,
            ..scopes[0].clone()
        }];
        assert!(
            preflight_certificate_scopes(&too_many_faults, &namespace, &registry, &owned).is_err()
        );
        let mut later_epoch_registry = KeysetRegistry::new();
        later_epoch_registry
            .register_keyset(
                KeysetVersion(1),
                1,
                registry.get_keys(&KeysetVersion(2)).unwrap().to_vec(),
            )
            .unwrap();
        assert!(
            preflight_certificate_scopes(&scopes, &namespace, &later_epoch_registry, &owned)
                .is_err()
        );
        // Transport reachability is independent of certificate trust. A
        // valid scope can serve a locally durable Pending operation even when
        // no destination or route is currently configured.
        assert!(preflight_certificate_scopes(&scopes, &namespace, &registry, &owned).is_ok());
        pin_operation_startup_manifests(
            &inbox,
            &mut outbox,
            None,
            None,
            None,
            owned.digest(),
            Some([6; 32]),
            None,
        )
        .unwrap();
    }

    // ---------------------------------------------------------------
    // Log filter defaults (P2-1)
    // ---------------------------------------------------------------

    /// Without `RUST_LOG`, the shipped Docker images must still emit INFO and
    /// WARN. `EnvFilter::from_default_env()` defaults to ERROR-only, which
    /// silences every operational warning (full-sync 413, bad signature,
    /// checkpoint failure, GC gate block, ping failure) in the shipped
    /// compose stacks, where no `RUST_LOG` is set.
    #[test]
    fn log_filter_defaults_to_info_when_rust_log_is_unset() {
        use tracing_subscriber::layer::Filter;
        assert_eq!(
            Filter::<tracing_subscriber::Registry>::max_level_hint(&log_env_filter(None)),
            Some(tracing::level_filters::LevelFilter::INFO),
            "RUST_LOG unset must default to INFO, not ERROR"
        );
    }

    /// An empty `RUST_LOG` (`RUST_LOG=` in a compose file) is equivalent to
    /// unset and must not silence the process either.
    #[test]
    fn log_filter_defaults_to_info_when_rust_log_is_empty() {
        use tracing_subscriber::layer::Filter;
        assert_eq!(
            Filter::<tracing_subscriber::Registry>::max_level_hint(&log_env_filter(Some(""))),
            Some(tracing::level_filters::LevelFilter::INFO),
        );
    }

    /// An explicit `RUST_LOG` still wins in both directions: the default is
    /// only a fallback, never an override.
    #[test]
    fn log_filter_honours_an_explicit_rust_log() {
        use tracing_subscriber::layer::Filter;
        for (directive, expected) in [
            ("error", tracing::level_filters::LevelFilter::ERROR),
            ("warn", tracing::level_filters::LevelFilter::WARN),
            (
                "asteroidb_poc=debug",
                tracing::level_filters::LevelFilter::DEBUG,
            ),
            ("trace", tracing::level_filters::LevelFilter::TRACE),
        ] {
            assert_eq!(
                Filter::<tracing_subscriber::Registry>::max_level_hint(&log_env_filter(Some(
                    directive
                ))),
                Some(expected),
                "RUST_LOG={directive} must be honoured verbatim"
            );
        }
    }

    /// Deterministic Ed25519 verifying key and its hex encoding.
    fn ed_key_hex(byte: u8) -> (ed25519_dalek::VerifyingKey, String) {
        let mut seed = [0u8; 32];
        seed[0] = byte;
        let vk = ed25519_dalek::SigningKey::from_bytes(&seed).verifying_key();
        (vk, hex::encode(vk.as_bytes()))
    }

    /// A structurally valid BLS public key hex string (96 chars).
    ///
    /// With native-crypto this must be a real group element, so it is derived
    /// from an actual keypair; the stub build accepts any string.
    #[cfg(feature = "native-crypto")]
    fn bls_key_hex(byte: u8) -> String {
        let mut seed = [0u8; 32];
        seed[0] = byte;
        asteroidb_poc::authority::bls::BlsKeypair::generate(&seed)
            .public_key
            .to_hex()
    }
    #[cfg(not(feature = "native-crypto"))]
    fn bls_key_hex(byte: u8) -> String {
        format!("{byte:02x}").repeat(48)
    }

    /// A matching BLS public key + proof-of-possession hex pair (96 + 192
    /// chars). Real key material with native-crypto; shape-only for the stub.
    #[cfg(feature = "native-crypto")]
    fn bls_entry_hex(byte: u8) -> (String, String) {
        let mut seed = [0u8; 32];
        seed[0] = byte;
        let kp = asteroidb_poc::authority::bls::BlsKeypair::generate(&seed);
        (kp.public_key.to_hex(), kp.proof_of_possession().to_hex())
    }
    #[cfg(not(feature = "native-crypto"))]
    fn bls_entry_hex(byte: u8) -> (String, String) {
        (
            format!("{byte:02x}").repeat(48),
            format!("{byte:02x}").repeat(96),
        )
    }

    // ---------------------------------------------------------------
    // parse_control_plane_nodes (m-5)
    // ---------------------------------------------------------------

    fn cp_voters_of(parse: ControlPlaneNodesParse) -> std::collections::BTreeSet<NodeId> {
        match parse {
            ControlPlaneNodesParse::Voters(v) => v,
            other => panic!(
                "expected Voters, got {}",
                match other {
                    ControlPlaneNodesParse::Unset => "Unset",
                    ControlPlaneNodesParse::AllEntriesEmpty => "AllEntriesEmpty",
                    ControlPlaneNodesParse::Voters(_) => unreachable!(),
                }
            ),
        }
    }

    #[test]
    fn control_plane_nodes_trailing_comma_creates_no_phantom_voter() {
        // A trailing comma must NOT materialize NodeId("") — a phantom
        // voter changes the majority denominator (3 voters instead of 2).
        let voters = cp_voters_of(parse_control_plane_nodes(Some("n1,n2,")));
        assert_eq!(voters.len(), 2, "phantom empty voter must be skipped");
        assert!(voters.contains(&NodeId("n1".into())));
        assert!(voters.contains(&NodeId("n2".into())));
    }

    #[test]
    fn control_plane_nodes_trims_whitespace_and_skips_empty_entries() {
        let voters = cp_voters_of(parse_control_plane_nodes(Some("n1, n2 , ")));
        assert_eq!(voters.len(), 2);
        assert!(voters.contains(&NodeId("n1".into())));
        assert!(voters.contains(&NodeId("n2".into())));
    }

    #[test]
    fn control_plane_nodes_all_empty_entries_is_a_config_error() {
        // Set-but-all-empty (","): definite config damage — the caller
        // fail-stops instead of silently self-electing with majority=1.
        for raw in [",", " , ", ",,"] {
            assert!(
                matches!(
                    parse_control_plane_nodes(Some(raw)),
                    ControlPlaneNodesParse::AllEntriesEmpty
                ),
                "{raw:?} must parse as AllEntriesEmpty"
            );
        }
    }

    #[test]
    fn control_plane_nodes_unset_or_blank_means_standalone_default() {
        // Existing behavior pinned: unset / empty / whitespace-only fall
        // back to the [self] standalone default at the call site.
        for raw in [None, Some(""), Some(" ")] {
            assert!(
                matches!(
                    parse_control_plane_nodes(raw),
                    ControlPlaneNodesParse::Unset
                ),
                "{raw:?} must parse as Unset"
            );
        }
    }

    // ---------------------------------------------------------------
    // parse_authority_nodes
    // ---------------------------------------------------------------

    #[test]
    fn authority_nodes_defaults_when_unset_or_blank() {
        for raw in [None, Some(""), Some("  ")] {
            assert_eq!(
                parse_authority_nodes(raw),
                vec![
                    NodeId("auth-1".into()),
                    NodeId("auth-2".into()),
                    NodeId("auth-3".into()),
                ],
                "{raw:?} must fall back to the default authority set"
            );
        }
    }

    #[test]
    fn authority_nodes_trims_and_preserves_order() {
        assert_eq!(
            parse_authority_nodes(Some(" n1 , n2 ,n3")),
            vec![
                NodeId("n1".into()),
                NodeId("n2".into()),
                NodeId("n3".into())
            ]
        );
    }

    /// A phantom `NodeId("")` inflates the majority denominator with a node
    /// that can never report a frontier, wedging every certified write on the
    /// seeded catch-all range.
    #[test]
    fn authority_nodes_skips_empty_entries() {
        assert_eq!(
            parse_authority_nodes(Some("n1,,n2,")),
            vec![NodeId("n1".into()), NodeId("n2".into())]
        );
    }

    /// Same wedge, different cause: ack frontiers are keyed by authority ID,
    /// so `["n1","n1"]` needs 2 frontiers from a set that can supply 1.
    #[test]
    fn authority_nodes_drops_duplicates() {
        assert_eq!(
            parse_authority_nodes(Some("n1,n2,n1")),
            vec![NodeId("n1".into()), NodeId("n2".into())]
        );
    }

    #[test]
    fn authority_nodes_all_empty_entries_fall_back_to_the_default() {
        for raw in [",", " , ", ",,"] {
            assert_eq!(
                parse_authority_nodes(Some(raw)),
                vec![
                    NodeId("auth-1".into()),
                    NodeId("auth-2".into()),
                    NodeId("auth-3".into()),
                ],
                "{raw:?} must not produce phantom NodeId(\"\") authorities"
            );
        }
    }

    #[test]
    fn parse_authority_keys_ed25519_only_entry() {
        let (vk, ed_hex) = ed_key_hex(1);
        let parsed = parse_authority_keys(&format!("auth-2={ed_hex}"), false).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].0, NodeId("auth-2".into()));
        assert_eq!(parsed[0].1, vk);
        assert!(parsed[0].2.is_none(), "BLS part is optional");
    }

    #[test]
    fn parse_authority_keys_full_triple_accepted() {
        let (vk, ed_hex) = ed_key_hex(2);
        let (bls_hex, pop_hex) = bls_entry_hex(2);
        let parsed =
            parse_authority_keys(&format!("auth-3={ed_hex}/{bls_hex}/{pop_hex}"), false).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].0, NodeId("auth-3".into()));
        assert_eq!(parsed[0].1, vk);
        let (pk, pop) = parsed[0].2.as_ref().expect("BLS key + PoP must be parsed");
        assert_eq!(pk.to_hex(), bls_hex);
        assert_eq!(pop.to_hex(), pop_hex);
    }

    #[test]
    fn parse_authority_keys_bls_without_pop_degrades_to_ed25519() {
        // Legacy two-segment entries carry no proof of possession: the BLS
        // part is dropped but the Ed25519 key is kept (lenient mode).
        let (vk, ed_hex) = ed_key_hex(3);
        let bls_hex = bls_key_hex(3);
        let parsed = parse_authority_keys(&format!("auth-3={ed_hex}/{bls_hex}"), false).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].1, vk);
        assert!(
            parsed[0].2.is_none(),
            "a BLS key without a PoP must be dropped"
        );
    }

    #[test]
    fn parse_authority_keys_multiple_entries_with_whitespace() {
        let (vk1, ed1) = ed_key_hex(4);
        let (vk2, ed2) = ed_key_hex(5);
        let parsed =
            parse_authority_keys(&format!(" auth-2 = {ed1} , auth-3={ed2},, "), false).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, NodeId("auth-2".into()));
        assert_eq!(parsed[0].1, vk1);
        assert_eq!(parsed[1].0, NodeId("auth-3".into()));
        assert_eq!(parsed[1].1, vk2);
    }

    #[test]
    fn parse_authority_keys_skips_entry_without_equals() {
        let (_, ed_hex) = ed_key_hex(6);
        let parsed = parse_authority_keys(&format!("garbage,auth-2={ed_hex}"), false).unwrap();
        assert_eq!(parsed.len(), 1, "malformed entry must be skipped");
        assert_eq!(parsed[0].0, NodeId("auth-2".into()));
    }

    #[test]
    fn parse_authority_keys_skips_invalid_ed25519_hex() {
        let (_, ed_hex) = ed_key_hex(7);
        let parsed =
            parse_authority_keys(&format!("auth-2=nothex,auth-3=abcd,auth-4={ed_hex}"), false)
                .unwrap();
        assert_eq!(
            parsed.len(),
            1,
            "invalid/short Ed25519 keys must be skipped"
        );
        assert_eq!(parsed[0].0, NodeId("auth-4".into()));
    }

    // The BLS stub's from_hex is a passthrough, so full group-element
    // rejection only exists with native-crypto (the stub still enforces the
    // hex shape; see the stub-specific test below).
    #[cfg(feature = "native-crypto")]
    #[test]
    fn parse_authority_keys_skips_invalid_bls_key() {
        let (_, ed_hex) = ed_key_hex(8);
        let (_, pop_hex) = bls_entry_hex(8);
        let parsed =
            parse_authority_keys(&format!("auth-2={ed_hex}/nothex/{pop_hex}"), false).unwrap();
        assert!(
            parsed.is_empty(),
            "an entry with an invalid BLS part must be skipped entirely"
        );
    }

    #[cfg(feature = "native-crypto")]
    #[test]
    fn parse_authority_keys_invalid_pop_skips_entry() {
        // A PoP generated by a different keypair fails verification.
        let (_, ed_hex) = ed_key_hex(9);
        let (bls_hex, _) = bls_entry_hex(9);
        let (_, wrong_pop_hex) = bls_entry_hex(10);
        let parsed =
            parse_authority_keys(&format!("auth-2={ed_hex}/{bls_hex}/{wrong_pop_hex}"), false)
                .unwrap();
        assert!(
            parsed.is_empty(),
            "an entry whose PoP fails verification must be skipped entirely"
        );
    }

    #[test]
    fn parse_authority_keys_strict_rejects_missing_pop() {
        let (_, ed_hex) = ed_key_hex(11);
        let bls_hex = bls_key_hex(11);
        let result = parse_authority_keys(&format!("auth-2={ed_hex}/{bls_hex}"), true);
        let err = result.expect_err("strict mode must reject a BLS key without a PoP");
        assert!(err.contains("auth-2"), "error must name the entry: {err}");
    }

    #[cfg(feature = "native-crypto")]
    #[test]
    fn parse_authority_keys_strict_rejects_invalid_pop() {
        let (_, ed_hex) = ed_key_hex(12);
        let (bls_hex, _) = bls_entry_hex(12);
        let (_, wrong_pop_hex) = bls_entry_hex(13);
        let result =
            parse_authority_keys(&format!("auth-2={ed_hex}/{bls_hex}/{wrong_pop_hex}"), true);
        assert!(
            result.is_err(),
            "strict mode must reject a PoP that fails verification"
        );
    }

    #[test]
    fn parse_authority_keys_strict_rejects_entry_without_equals() {
        let (_, ed_hex) = ed_key_hex(15);
        let result = parse_authority_keys(&format!("garbage,auth-2={ed_hex}"), true);
        let err = result.expect_err("strict mode must reject an entry without '='");
        assert!(err.contains("garbage"), "error must name the entry: {err}");
    }

    #[test]
    fn parse_authority_keys_strict_rejects_invalid_ed25519_hex() {
        let (_, ed_hex) = ed_key_hex(16);
        let result = parse_authority_keys(&format!("auth-2=abcg,auth-3={ed_hex}"), true);
        let err = result.expect_err("strict mode must reject an invalid Ed25519 key");
        assert!(err.contains("auth-2"), "error must name the entry: {err}");
    }

    // Stub builds cannot verify a PoP cryptographically but still enforce
    // the hex shape (96-char key, 192-char PoP) so the env format stays
    // consistent across build flavours.
    #[cfg(not(feature = "native-crypto"))]
    #[test]
    fn parse_authority_keys_stub_rejects_malformed_hex_lengths() {
        let (_, ed_hex) = ed_key_hex(14);
        let (bls_hex, pop_hex) = bls_entry_hex(14);

        let parsed =
            parse_authority_keys(&format!("auth-2={ed_hex}/abcd/{pop_hex}"), false).unwrap();
        assert!(parsed.is_empty(), "short BLS hex must skip the entry");

        let parsed =
            parse_authority_keys(&format!("auth-2={ed_hex}/{bls_hex}/abcd"), false).unwrap();
        assert!(parsed.is_empty(), "short PoP hex must skip the entry");
    }

    #[test]
    fn parse_authority_keys_empty_input() {
        assert!(parse_authority_keys("", false).unwrap().is_empty());
        assert!(parse_authority_keys(" , ,", false).unwrap().is_empty());
        assert!(parse_authority_keys("", true).unwrap().is_empty());
    }
}
