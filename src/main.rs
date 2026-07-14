use std::sync::{Arc, RwLock};

use tokio::sync::Mutex;

#[cfg(feature = "native-crypto")]
use asteroidb_poc::authority::bls::{BlsProofOfPossession, BlsPublicKey};
#[cfg(not(feature = "native-crypto"))]
use asteroidb_poc::authority::bls_stub::{BlsProofOfPossession, BlsPublicKey};
use asteroidb_poc::authority::certificate::{EpochManager, KeysetRegistry, KeysetVersion};
use asteroidb_poc::authority::equivocation::EquivocationDetector;
use asteroidb_poc::authority::frontier_sig::NodeSigner;
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::network::membership::MembershipClient;
use asteroidb_poc::network::sync::SyncClient;
use asteroidb_poc::network::{NodeConfig, PeerRegistry};
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::runtime::{BlsConfig, NodeRunner, NodeRunnerConfig};
use asteroidb_poc::types::{KeyRange, NodeId};

/// A parsed `ASTEROIDB_AUTHORITY_KEYS` entry: node ID, Ed25519 verifying key,
/// and (optionally) a BLS public key with its verified proof of possession.
type AuthorityKeyEntry = (
    NodeId,
    ed25519_dalek::VerifyingKey,
    Option<(BlsPublicKey, BlsProofOfPossession)>,
);

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
fn authority_nodes() -> Vec<NodeId> {
    match std::env::var("ASTEROIDB_AUTHORITY_NODES") {
        Ok(val) if !val.trim().is_empty() => val
            .split(',')
            .map(|s| NodeId(s.trim().to_string()))
            .collect(),
        _ => vec![
            NodeId("auth-1".into()),
            NodeId("auth-2".into()),
            NodeId("auth-3".into()),
        ],
    }
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

#[tokio::main]
async fn main() {
    // Initialize structured logging. Users control verbosity via RUST_LOG env var
    // (e.g. RUST_LOG=info or RUST_LOG=asteroidb_poc=debug).
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Load configuration: either from a config file or from individual env vars.
    let (node_id, bind_addr, advertise_addr, config_peer_registry) =
        match std::env::var("ASTEROIDB_CONFIG") {
            Ok(config_path) => match NodeConfig::load(&config_path) {
                Ok(config) => {
                    let node_id = config.node.id;
                    let bind_addr = config.bind_addr.to_string();
                    // Prefer ASTEROIDB_ADVERTISE_ADDR env var, then config field, then bind_addr.
                    let advertise_addr = std::env::var("ASTEROIDB_ADVERTISE_ADDR")
                        .ok()
                        .or(config.advertise_addr)
                        .unwrap_or_else(|| bind_addr.clone());
                    let peer_registry = config.peers;
                    (node_id, bind_addr, advertise_addr, Some(peer_registry))
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
                let node_id = NodeId(node_id_str);
                // Prefer ASTEROIDB_ADVERTISE_ADDR env var, then fall back to bind_addr.
                let advertise_addr =
                    std::env::var("ASTEROIDB_ADVERTISE_ADDR").unwrap_or_else(|_| bind_addr.clone());
                (node_id, bind_addr, advertise_addr, None)
            }
        };

    println!("AsteroidDB starting... (node_id={})", node_id.0);

    let auth_nodes = authority_nodes();

    // Determine persistence directory (used for peer registry, store snapshots,
    // and system namespace persistence).
    let data_dir = std::path::PathBuf::from(
        std::env::var("ASTEROIDB_DATA_DIR").unwrap_or_else(|_| "./data".into()),
    );

    let ns_persist_path = data_dir.join("system_namespace.json");
    let mut ns = match SystemNamespace::load(&ns_persist_path) {
        Ok(Some(loaded)) => {
            println!("Loaded system namespace from {}", ns_persist_path.display(),);
            loaded
        }
        Ok(None) => SystemNamespace::new(),
        Err(e) => {
            eprintln!(
                "warning: failed to load system namespace from {}: {e}; starting fresh",
                ns_persist_path.display(),
            );
            SystemNamespace::new()
        }
    };
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: String::new(),
        },
        authority_nodes: auth_nodes.clone(),
        auto_generated: false,
    });

    let namespace = Arc::new(RwLock::new(ns));

    // Build shared runtime metrics.
    let metrics = Arc::new(RuntimeMetrics::default());

    // Crash recovery: load each store's snapshot and replay its WAL, then
    // open fresh WAL writers. This must complete before the listener binds
    // and before any peer sync starts, so clients and peers only ever see
    // the recovered state. Recovery failures (damaged snapshot, mid-log
    // WAL corruption) are fail-stop with a runbook pointer — silently
    // starting empty would overwrite an intact replica's durable state.
    let persistence_cfg = asteroidb_poc::runtime::PersistenceConfig::from_env(data_dir.clone());

    // Share a single CertifiedApi between HTTP handlers and NodeRunner
    // so that certification status updates are visible to both.
    let (certified_recovered, certified_wal_syncer) =
        match asteroidb_poc::runtime::persistence::recover_certified(
            node_id.clone(),
            Arc::clone(&namespace),
            &persistence_cfg,
        ) {
            Ok(recovered) => recovered,
            Err(e) => {
                eprintln!(
                    "error: certified store recovery failed: {e}\n\
                     See docs/ops-guide.md (Crash recovery runbook) before retrying."
                );
                std::process::exit(1);
            }
        };
    let certified_api = Arc::new(Mutex::new(certified_recovered));
    let peer_persist_path = PeerRegistry::persist_path(&data_dir);

    // Share a single EventualApi between HTTP handlers and NodeRunner
    // so that HTTP writes are visible to the anti-entropy sync loop.
    let (eventual_recovered, eventual_wal_syncer) =
        match asteroidb_poc::runtime::persistence::recover_eventual(
            node_id.clone(),
            &persistence_cfg,
        ) {
            Ok(recovered) => recovered,
            Err(e) => {
                eprintln!(
                    "error: eventual store recovery failed: {e}\n\
                     See docs/ops-guide.md (Crash recovery runbook) before retrying."
                );
                std::process::exit(1);
            }
        };
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

    // Build control-plane consensus with the same authority nodes (FR-009).
    let consensus = Arc::new(Mutex::new(ControlPlaneConsensus::new(auth_nodes)));

    // Optional shared token for authenticating internal API requests.
    // Treat an empty string the same as unset — Docker Compose substitutes
    // `${ASTEROIDB_INTERNAL_TOKEN}` as "" when the host variable is not
    // defined, which would otherwise activate the auth middleware with a
    // degenerate empty token, breaking inter-node communication in CI.
    let internal_token = std::env::var("ASTEROIDB_INTERNAL_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());

    // SLO tracker shared between HTTP handlers and NodeRunner.
    let slo_tracker = Arc::new(asteroidb_poc::ops::slo::SloTracker::new());

    // Shared latency model and topology view for placement policies and the
    // /api/topology endpoint. The same Arc instances are shared between
    // AppState (read by HTTP handlers) and NodeRunner (updated by sync/ping).
    let shared_latency_model = Arc::new(std::sync::RwLock::new(
        asteroidb_poc::placement::latency::LatencyModel::new(),
    ));
    let shared_cluster_nodes: Arc<std::sync::RwLock<Vec<asteroidb_poc::node::Node>>> =
        Arc::new(std::sync::RwLock::new(Vec::new()));
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
    if require_signed_frontiers && keyset_registry.is_none() {
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
    let equivocation = Arc::new(EquivocationDetector::new(Some(
        data_dir.join("equivocation_evidence.json"),
    )));
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
        epoch_config,
        current_epoch: Arc::clone(&current_epoch),
        require_signed_frontiers,
        equivocation: Arc::clone(&equivocation),
        exclude_accused_authorities,
        eventual_wal: eventual_wal_syncer.clone(),
        certified_wal: certified_wal_syncer.clone(),
    });

    let app = router(state);

    let runner_config = NodeRunnerConfig {
        bls_config,
        node_signer,
        keyset_registry,
        internal_token: internal_token.clone(),
        current_epoch: Some(Arc::clone(&current_epoch)),
        equivocation: Some(Arc::clone(&equivocation)),
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

    // Background persistence: WAL group-commit flushers + periodic
    // checkpoints (snapshot, then prune sealed WAL segments).
    asteroidb_poc::runtime::persistence::spawn_persistence_tasks(
        persistence_cfg.clone(),
        Arc::clone(&eventual_api),
        Arc::clone(&certified_api),
        eventual_wal_syncer,
        certified_wal_syncer,
    );

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
            // upgrades therefore require the new binary to read the old
            // format (versioned decode arm — see the WAL_FORMAT_VERSION
            // maintainer warning); a graceful shutdown alone is not a
            // format-upgrade path. Failures are non-fatal — the WAL
            // already holds everything a restart needs.
            if persistence_cfg.enabled {
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
