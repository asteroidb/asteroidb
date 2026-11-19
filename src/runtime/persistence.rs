//! Runtime persistence wiring: startup recovery, periodic checkpoints, and
//! WAL flusher tasks.
//!
//! ## Recovery (startup, before the listener binds)
//!
//! 1. Load the bincode snapshot (`<data>/eventual.snapshot.bin` /
//!    `<data>/certified.snapshot.bin`); a missing file means an empty
//!    store, any other error is fail-stop (a damaged snapshot must never
//!    be silently replaced by an empty store).
//! 2. Replay every retained WAL segment in ascending order. All records
//!    are idempotent redo records, so replaying entries the snapshot
//!    already covers is a no-op ("over-replay safety") — no LSN watermark
//!    is needed in the snapshot.
//! 3. Seed the HLC clock from the recovered maximum (done inside
//!    `EventualApi::recovered` / `CertifiedApi::recovered`).
//! 4. Open a NEW WAL segment for appending (sealed segments are never
//!    reused).
//!
//! A torn tail in the final segment is expected crash damage (only
//! un-synced, un-acked writes are lost) and recovery continues; mid-log
//! corruption is fail-stop unless `ASTEROIDB_WAL_RECOVER_TRUNCATE=1`
//! explicitly opts into truncating at the first invalid record.
//!
//! Whenever recovery continues past a non-clean read (torn tail, or
//! corruption under the truncate flag) the log is **physically repaired**
//! to the replayed prefix (`wal::truncate_to_valid_prefix`) before the new
//! writer opens: the writer always starts a higher segment, so an invalid
//! frame left in place would become *mid-log* corruption on the next boot
//! and hide (then lose) every acked write appended after this recovery.
//!
//! ## Checkpoint ordering discipline
//!
//! `rotate → clone → save → delete`: the WAL is rotated and the store
//! cloned in ONE critical section (so the clone covers every record in
//! sealed segments), the snapshot is written off the lock, and only after
//! the snapshot is durably renamed are segments `<= sealed` deleted. A
//! crash at ANY point leaves "snapshot + retained segments ⊇ acked state"
//! true — the worst case is harmless over-replay.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::api::certified::CertifiedApi;
use crate::api::eventual::EventualApi;
use crate::control_plane::system_namespace::SystemNamespace;
use crate::store::Store;
use crate::store::wal::{
    self, CertifiedScopePin, SyncPolicy, WalConfig, WalReadOutcome, WalSyncer, WalWriter,
};
use crate::types::{NodeId, PolicyVersion};

/// Snapshot file name for the eventual store.
const EVENTUAL_SNAPSHOT: &str = "eventual.snapshot.bin";
/// Snapshot file name for the certified store.
const CERTIFIED_SNAPSHOT: &str = "certified.snapshot.bin";
/// WAL subdirectory for the eventual store.
const EVENTUAL_WAL_DIR: &str = "wal/eventual";
/// WAL subdirectory for the certified store.
const CERTIFIED_WAL_DIR: &str = "wal/certified";
/// Sidecar file holding the write-time (origin) policy version of each
/// still-uncertified certified-store key as of the last checkpoint.
///
/// The certified snapshot (a bincode `Store`) has no place for a per-key
/// policy version, and once a checkpoint prunes the WAL the origin-carrying
/// `WalRecord::CertifiedUpsert` records are gone. This sidecar preserves the
/// origins so recovery can re-track a checkpointed-but-uncertified write
/// under the version it was written with and re-derive its FR-009 fence
/// (fix/fence-persistence-across-restart). It is a belt beside the WAL: it
/// never changes the `Store` snapshot format, so an old data directory
/// without it recovers exactly as before (empty origins = current-version
/// fallback).
const CERTIFIED_ORIGINS: &str = "certified.origins.bin";
/// Versioned, checksummed scope pins for certified writes. The marker makes
/// a missing sidecar fail-stop after the first checkpoint that relies on it.
const CERTIFIED_SCOPE_PINS: &str = "certified.scope-pins.bin";
const CERTIFIED_SCOPE_PINS_REQUIRED: &str = "certified.scope-pins.required";
const CERTIFIED_ORIGINS_PENDING: &str = "certified.origins.pending.bin";
const CERTIFIED_SCOPE_PINS_PENDING: &str = "certified.scope-pins.pending.bin";
const SCOPE_PINS_MAGIC: [u8; 8] = *b"ADBSCOPE";
const SCOPE_PINS_VERSION: u32 = 1;
const SCOPE_PINS_MARKER: &[u8] = b"asteroidb-certified-scope-pins-v1\n";

/// Per-store checkpoint serialization locks, shared by every clone of a
/// [`PersistenceConfig`].
///
/// The periodic checkpoint ticker (never stopped) and the shutdown-path
/// final checkpoint call [`checkpoint_eventual`] / [`checkpoint_certified`]
/// concurrently. The API mutex only covers the rotate+clone step; the
/// snapshot save and segment deletion run off the lock, so without this
/// lock two checkpoints of the SAME store can interleave such that an
/// older snapshot is renamed over a newer one AFTER the newer checkpoint
/// already deleted the WAL segments covering the difference — silently
/// losing acked writes at the next recovery.
#[derive(Debug, Clone, Default)]
pub struct CheckpointLocks {
    eventual: Arc<tokio::sync::Mutex<()>>,
    certified: Arc<tokio::sync::Mutex<()>>,
}

/// Persistence configuration, parsed from the environment.
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// Master switch (`ASTEROIDB_PERSISTENCE`, default on). Off restores
    /// the historical purely-in-memory behaviour.
    pub enabled: bool,
    /// Data directory (`ASTEROIDB_DATA_DIR`).
    pub data_dir: PathBuf,
    /// WAL fsync policy (`ASTEROIDB_WAL_SYNC`, default `always`).
    pub sync: SyncPolicy,
    /// Checkpoint period (`ASTEROIDB_SNAPSHOT_INTERVAL_SECS`, default 300;
    /// `None` = periodic checkpoints disabled).
    pub snapshot_interval: Option<Duration>,
    /// WAL segment rotation threshold (`ASTEROIDB_WAL_SEGMENT_BYTES`).
    pub segment_max_bytes: u64,
    /// Truncate-at-first-corruption escape hatch
    /// (`ASTEROIDB_WAL_RECOVER_TRUNCATE`, default off = fail-stop).
    pub recover_truncate: bool,
    /// Per-store checkpoint serialization (see [`CheckpointLocks`]).
    /// Shared across clones, so the periodic ticker and the shutdown-path
    /// final checkpoint can never checkpoint the same store concurrently.
    pub checkpoint_locks: CheckpointLocks,
}

fn env_flag(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(v) => {
            let v = v.trim().to_ascii_lowercase();
            if default {
                !(v == "0" || v == "false" || v == "off")
            } else {
                v == "1" || v == "true" || v == "on"
            }
        }
        Err(_) => default,
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

impl PersistenceConfig {
    /// Parse persistence settings from the environment.
    pub fn from_env(data_dir: PathBuf) -> Self {
        let sync = match std::env::var("ASTEROIDB_WAL_SYNC")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "interval" => SyncPolicy::Interval(Duration::from_millis(
                env_u64("ASTEROIDB_WAL_SYNC_INTERVAL_MS", 100).max(1),
            )),
            "off" => SyncPolicy::Off,
            "always" | "" => SyncPolicy::Always,
            other => {
                tracing::warn!(
                    value = other,
                    "unknown ASTEROIDB_WAL_SYNC value; defaulting to 'always'"
                );
                SyncPolicy::Always
            }
        };
        let snapshot_secs = env_u64("ASTEROIDB_SNAPSHOT_INTERVAL_SECS", 300);
        Self {
            enabled: env_flag("ASTEROIDB_PERSISTENCE", true),
            data_dir,
            sync,
            snapshot_interval: (snapshot_secs > 0).then(|| Duration::from_secs(snapshot_secs)),
            segment_max_bytes: env_u64(
                "ASTEROIDB_WAL_SEGMENT_BYTES",
                WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
            ),
            recover_truncate: env_flag("ASTEROIDB_WAL_RECOVER_TRUNCATE", false),
            checkpoint_locks: CheckpointLocks::default(),
        }
    }

    fn wal_config(&self, subdir: &str) -> WalConfig {
        WalConfig {
            dir: self.data_dir.join(subdir),
            sync: self.sync,
            segment_max_bytes: self.segment_max_bytes,
        }
    }
}

/// Recover a store from its snapshot + WAL and open a fresh WAL writer.
///
/// Returns the recovered store, the writer, and the WAL read outcome
/// (input to the recovery-gap fence decision in [`recover_eventual`]).
/// Fail-stop errors: damaged snapshot, or mid-log corruption without the
/// truncate escape hatch.
type RecoveredStore = (
    Store,
    WalWriter,
    WalReadOutcome,
    HashMap<String, PolicyVersion>,
    HashMap<String, Option<CertifiedScopePin>>,
);

fn recover_store(
    label: &str,
    snapshot_path: &Path,
    wal_cfg: WalConfig,
    recover_truncate: bool,
) -> io::Result<RecoveredStore> {
    let started = std::time::Instant::now();
    // Sweep tmp files left by saves that failed (or crashed) before their
    // in-line cleanup (m-3). Startup-only: no concurrent save can be in
    // flight here, so a matching tmp is always stale. Best-effort — a
    // failed sweep only leaves litter behind, never blocks recovery.
    if let Err(e) = crate::store::backend::FileBackend::new(snapshot_path).remove_stale_tmp_files()
    {
        tracing::warn!(
            store = label,
            snapshot = %snapshot_path.display(),
            error = %e,
            "failed to sweep stale snapshot tmp files; continuing"
        );
    }
    let mut store = Store::load_snapshot_bincode_or_default(snapshot_path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "failed to load {label} snapshot {}: {e}. A damaged snapshot is never \
                 replaced silently — restore THIS FILE from a backup, or move aside only \
                 the damaged store's snapshot+WAL. Only the eventual store can be rebuilt \
                 from peers via anti-entropy; the certified store, raft/ vote state, and \
                 equivocation evidence have NO rebuild path, so never discard the whole \
                 data directory (see docs/ops-guide.md)",
                snapshot_path.display()
            ),
        )
    })?;

    let read = wal::read_all_segments(&wal_cfg.dir)?;
    match read.outcome {
        WalReadOutcome::Clean => {}
        WalReadOutcome::TornTail => {
            tracing::warn!(
                store = label,
                records = read.records.len(),
                "WAL ends in a torn tail (crash mid-append); replaying up to the last \
                 valid record — only un-acked writes are lost"
            );
        }
        WalReadOutcome::Corruption => {
            if recover_truncate {
                tracing::warn!(
                    store = label,
                    records = read.records.len(),
                    "WAL is corrupted mid-log; ASTEROIDB_WAL_RECOVER_TRUNCATE=1 set — \
                     truncating at the first invalid record (acked writes after it are LOST)"
                );
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{label} WAL in {} is corrupted mid-log: acked data may be damaged. \
                         Either restore this store's snapshot+WAL from a backup (for the \
                         eventual store, moving aside ONLY its snapshot+WAL and re-filling \
                         from peers via anti-entropy also works; the certified store has no \
                         such rebuild path, and raft/ vote state and equivocation evidence \
                         must never be discarded) or set ASTEROIDB_WAL_RECOVER_TRUNCATE=1 \
                         to explicitly truncate at the first invalid record \
                         (see docs/ops-guide.md)",
                        wal_cfg.dir.display()
                    ),
                ));
            }
        }
    }
    // Physically repair the log to the replayed prefix BEFORE opening the
    // next segment. WalWriter::open always creates a new, higher-numbered
    // segment, so a torn/invalid frame left in place would sit in a
    // non-final segment on the next boot and read as fail-stop mid-log
    // corruption — with every acked write appended after this recovery
    // unreachable behind it (and silently discarded under the truncate
    // flag, then deleted for good by the next checkpoint).
    if read.outcome != WalReadOutcome::Clean {
        wal::truncate_to_valid_prefix(&wal_cfg.dir, &read).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "failed to repair {label} WAL in {} after a non-clean read: {e}",
                    wal_cfg.dir.display()
                ),
            )
        })?;
    }

    let replayed = read.records.len();
    // Harvest write-time policy versions from certified-store records. A
    // later record for the same key wins (writes are appended in order), so
    // the map ends up with each key's most recent origin. The eventual store
    // never emits `CertifiedUpsert`, so its map stays empty (and is ignored).
    let mut origins: HashMap<String, PolicyVersion> = HashMap::new();
    let mut scope_updates: HashMap<String, Option<CertifiedScopePin>> = HashMap::new();
    for record in read.records {
        match &record {
            wal::WalRecord::CertifiedUpsert {
                key,
                policy_version,
                ..
            } => {
                origins.insert(key.clone(), *policy_version);
                scope_updates.insert(key.clone(), None);
            }
            wal::WalRecord::CertifiedUpsertScoped {
                key, hlc, scope, ..
            } => {
                validate_scope_pin(key, scope)?;
                if scope.write_hlc != *hlc {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "certified WAL scope pin HLC does not match upsert HLC",
                    ));
                }
                origins.insert(key.clone(), scope.policy_version);
                scope_updates.insert(key.clone(), Some(scope.clone()));
            }
            _ => {}
        }
        wal::replay_record(&mut store, record);
    }

    if snapshot_path.exists() && !wal::has_incarnation_high_water(&wal_cfg.dir) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} snapshot exists without WAL incarnation high-water"),
        ));
    }
    let wal_dir = wal_cfg.dir.clone();
    let writer = WalWriter::open(wal_cfg)?;
    // Replayed frames may have come only from the previous process's page
    // cache. The new writer's own durable segment does not fsync older
    // segment bodies. Fence them before exposing the recovered state: a
    // subsequent digest with no new WAL position may otherwise claim that
    // an unflushed tombstone is durable and authorize a peer's GC sweep.
    wal::sync_replayed_segments(&wal_dir)?;
    tracing::info!(
        store = label,
        snapshot = %snapshot_path.display(),
        wal_segments = read.segments,
        wal_records = replayed,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "recovery complete"
    );
    Ok((store, writer, read.outcome, origins, scope_updates))
}

/// Recover the eventual store and build its API + WAL syncer.
///
/// Recovery-gap fence (session guarantees): when the WAL could not
/// guarantee that every ACKED write survived — sync policy
/// `interval`/`off`, persistence disabled, or a truncating corruption
/// recovery — the local origin's applied frontier is fenced over the
/// possibly-lost range (`EventualApi::install_recovery_fence`). Without
/// the fence, the first post-restart local write max-merges
/// `applied_origins[self]` past the hole and a session token for a lost
/// acked write would return a FALSE success; with it, such tokens answer
/// 412 until anti-entropy adoption proves the range re-covered. A clean
/// or torn-tail read under `SyncPolicy::Always` loses only un-acked
/// writes, so no fence is needed there.
///
/// **Fence durability**: `install_recovery_fence` only mutates the
/// in-memory store, the WAL has no fence record, and the first periodic
/// checkpoint runs a full `snapshot_interval` after startup (or never,
/// when checkpoints are disabled) — so a SECOND crash before that
/// checkpoint would silently drop the fence while the WAL replay of any
/// post-recovery durable write re-advances `applied_origins[self]` past
/// the old hole, reopening the exact false-success path the fence
/// closes. Therefore a snapshot (fence included) is forced RIGHT HERE,
/// before any traffic is served; failure to write it is fail-stop. The
/// snapshot is written without rotating or pruning the WAL: retained
/// segments are merely over-replayed on the next boot (harmless, same
/// invariant as a checkpoint crash). With persistence disabled entirely
/// there is nothing to persist — but then EVERY restart re-fences from
/// zero, so no second-crash hole exists either.
pub fn recover_eventual(
    node_id: NodeId,
    cfg: &PersistenceConfig,
) -> io::Result<(EventualApi, Option<Arc<WalSyncer>>)> {
    if !cfg.enabled {
        // No durability at all: every acked write of a previous
        // incarnation is gone. Fence from zero so old self-origin tokens
        // cannot pass path A off this incarnation's fresh writes.
        let mut api = EventualApi::new(node_id);
        api.install_recovery_fence().map_err(io::Error::other)?;
        return Ok((api, None));
    }
    // The eventual store never emits `CertifiedUpsert`, so its origins map is
    // always empty; certification-scope origins are a certified-store concept.
    let (store, writer, outcome, _origins, _scope_updates) = recover_store(
        "eventual",
        &cfg.data_dir.join(EVENTUAL_SNAPSHOT),
        cfg.wal_config(EVENTUAL_WAL_DIR),
        cfg.recover_truncate,
    )?;
    let syncer = Arc::new(writer.syncer());
    let mut api = EventualApi::recovered(node_id, store, Some(writer));
    let fence_needed = !matches!(cfg.sync, SyncPolicy::Always)
        || (outcome == WalReadOutcome::Corruption && cfg.recover_truncate);
    if fence_needed {
        api.install_recovery_fence().map_err(io::Error::other)?;
        // Make the fence durable BEFORE serving traffic (see the doc
        // comment above). The atomic snapshot write (tmp + rename) means
        // a crash during it leaves the previous snapshot in place — safe,
        // because the vulnerable window only opens once post-recovery
        // writes are durably logged, which cannot happen before this
        // function returns.
        let snapshot_path = cfg.data_dir.join(EVENTUAL_SNAPSHOT);
        api.store()
            .save_snapshot_bincode(&snapshot_path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "failed to persist the recovery-gap fence snapshot {}: {e}. Serving \
                         traffic without it would let a second crash silently drop the fence \
                         and turn lost acked writes into false session successes",
                        snapshot_path.display()
                    ),
                )
            })?;
    }
    Ok((api, Some(syncer)))
}

/// Recover the certified store and build its API + WAL syncer.
pub fn recover_certified(
    node_id: NodeId,
    namespace: Arc<std::sync::RwLock<SystemNamespace>>,
    cfg: &PersistenceConfig,
) -> io::Result<(CertifiedApi, Option<Arc<WalSyncer>>)> {
    if !cfg.enabled {
        return Ok((CertifiedApi::new(node_id, namespace), None));
    }
    // No recovery-gap fence here: session tokens are an eventual-API-only
    // contract (api-reference.md), so the certified store needs no fence.
    let (store, writer, _outcome, wal_origins, wal_scope_updates) = recover_store(
        "certified",
        &cfg.data_dir.join(CERTIFIED_SNAPSHOT),
        cfg.wal_config(CERTIFIED_WAL_DIR),
        cfg.recover_truncate,
    )?;
    let syncer = Arc::new(writer.syncer());

    // Seed write-time policy versions from the checkpoint sidecar, then let
    // retained-WAL origins (writes since the last checkpoint) override them:
    // a post-checkpoint write is newer than the sidecar's snapshot-time view.
    // These origins let `CertifiedApi::recovered` re-track each write under
    // the version it was written with and re-derive the FR-009 fence a
    // version bump installed only in memory (fix/fence-persistence-across-restart).
    let origins_path = cfg.data_dir.join(CERTIFIED_ORIGINS);
    // Sweep tmp files left by a sidecar save that crashed before its rename
    // (symmetric with the snapshot sweep in `recover_store`). Startup-only, so
    // any matching tmp is stale; best-effort — a failed sweep only leaves
    // litter, never blocks recovery.
    if let Err(e) = crate::store::backend::FileBackend::new(&origins_path).remove_stale_tmp_files()
    {
        tracing::warn!(
            sidecar = %origins_path.display(),
            error = %e,
            "failed to sweep stale certified origins tmp files; continuing"
        );
    }
    let (mut origins, _) = load_origins_sidecar(&origins_path);
    origins.extend(wal_origins);
    let mut scope_pins = load_scope_pins_sidecar(&cfg.data_dir)?;
    for (key, update) in wal_scope_updates {
        match update {
            Some(pin) => {
                scope_pins.insert(key, pin);
            }
            None => {
                scope_pins.remove(&key);
            }
        }
    }
    // A sidecar may have been saved just before a crash that left the old
    // snapshot in place. Replay has already reconstructed the final store;
    // only pins naming that exact write can establish its origin. This also
    // lets a valid pin recover a missing origins sidecar.
    scope_pins.retain(|key, pin| store.timestamp_for(key) == Some(&pin.write_hlc));
    for (key, pin) in &scope_pins {
        origins.insert(key.clone(), pin.policy_version);
    }
    if store
        .keys()
        .into_iter()
        .any(|key| !scope_pins.contains_key(key))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "certified store contains a key without a write-time scope pin",
        ));
    }

    Ok((
        CertifiedApi::recovered_with_scope_pins(
            node_id,
            namespace,
            store,
            Some(writer),
            origins,
            scope_pins,
        ),
        Some(syncer),
    ))
}

/// Load the certified origins sidecar, or an empty map when it is absent
/// (a fresh or pre-upgrade data directory) or unreadable.
///
/// Legacy directories may lack this sidecar and fall back to the current
/// policy version. Once a scope-pin checkpoint has been marked, recovery
/// may use matching pins and retained WAL origins to reconstruct missing
/// entries; it fails closed if any checkpointed key has neither. NotFound is
/// normal for data directories written before the origins feature.
fn load_origins_sidecar(path: &Path) -> (HashMap<String, PolicyVersion>, bool) {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return (HashMap::new(), false),
        Err(e) => {
            tracing::warn!(
                sidecar = %path.display(),
                error = %e,
                "failed to read certified origins sidecar; recovered writes fall back \
                 to the current policy version"
            );
            return (HashMap::new(), false);
        }
    };
    match bincode::serde::decode_from_slice::<Vec<(String, PolicyVersion)>, _>(
        &bytes,
        bincode::config::standard(),
    ) {
        Ok((entries, used)) if used == bytes.len() => (entries.into_iter().collect(), true),
        Ok(_) => {
            tracing::warn!(sidecar = %path.display(), "certified origins sidecar has trailing bytes");
            (HashMap::new(), false)
        }
        Err(e) => {
            tracing::warn!(
                sidecar = %path.display(),
                error = %e,
                "certified origins sidecar is corrupt; recovered writes fall back to \
                 the current policy version"
            );
            (HashMap::new(), false)
        }
    }
}

/// Atomically write the certified origins sidecar via [`FileBackend::save`]
/// (tmp + fsync + rename + parent-dir fsync), the exact same crash-safe
/// discipline as the snapshot — including the tmp naming that
/// [`FileBackend::remove_stale_tmp_files`] sweeps at startup, so a crash
/// between the tmp write and the rename leaves no orphan behind (symmetric
/// with the snapshot's stale-tmp cleanup in `recover_certified`).
fn save_origins_sidecar(path: &Path, origins: &HashMap<String, PolicyVersion>) -> io::Result<()> {
    use crate::store::backend::StorageBackend;

    let entries: Vec<(String, PolicyVersion)> =
        origins.iter().map(|(k, v)| (k.clone(), *v)).collect();
    let bytes = bincode::serde::encode_to_vec(&entries, bincode::config::standard())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    crate::store::backend::FileBackend::new(path).save(&bytes)
}

fn validate_scope_pin(key: &str, pin: &CertifiedScopePin) -> io::Result<()> {
    if !key.starts_with(&pin.key_range.prefix)
        || pin.authority_nodes.is_empty()
        || pin
            .authority_nodes
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid certified scope pin for key {key}"),
        ));
    }
    Ok(())
}

/// A modern checkpoint promises a durable scope sidecar. Legacy directories
/// have neither the sidecar nor the marker and keep their old recovery path.
fn load_scope_pins_sidecar(data_dir: &Path) -> io::Result<HashMap<String, CertifiedScopePin>> {
    let marker_path = data_dir.join(CERTIFIED_SCOPE_PINS_REQUIRED);
    let marked = match std::fs::read(&marker_path) {
        Ok(bytes) if bytes == SCOPE_PINS_MARKER => true,
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "certified scope pin marker has invalid contents",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(error),
    };
    let path = data_dir.join(CERTIFIED_SCOPE_PINS);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound && !marked => {
            return Ok(HashMap::new());
        }
        Err(error) => {
            return Err(io::Error::new(
                error.kind(),
                format!(
                    "required certified scope pin sidecar {}: {error}",
                    path.display()
                ),
            ));
        }
    };
    if bytes.len() < 16 || bytes[..8] != SCOPE_PINS_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "certified scope pin sidecar has invalid magic or length",
        ));
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let expected_crc = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    let payload = &bytes[16..];
    if version != SCOPE_PINS_VERSION || crc32fast::hash(payload) != expected_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "certified scope pin sidecar version or checksum mismatch",
        ));
    }
    let (entries, used): (Vec<(String, CertifiedScopePin)>, usize) =
        bincode::serde::decode_from_slice(payload, bincode::config::standard())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if used != payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "certified scope pin sidecar has trailing bytes",
        ));
    }
    let mut pins = HashMap::with_capacity(entries.len());
    for (key, pin) in entries {
        validate_scope_pin(&key, &pin)?;
        if pins.insert(key, pin).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "certified scope pin sidecar has duplicate keys",
            ));
        }
    }
    Ok(pins)
}

fn save_scope_pins_sidecar(
    path: &Path,
    pins: &HashMap<String, CertifiedScopePin>,
) -> io::Result<()> {
    use crate::store::backend::StorageBackend;

    let mut entries: Vec<(String, CertifiedScopePin)> = pins
        .iter()
        .map(|(key, pin)| (key.clone(), pin.clone()))
        .collect();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (key, pin) in &entries {
        validate_scope_pin(key, pin)?;
    }
    let payload = bincode::serde::encode_to_vec(&entries, bincode::config::standard())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut bytes = Vec::with_capacity(16 + payload.len());
    bytes.extend_from_slice(&SCOPE_PINS_MAGIC);
    bytes.extend_from_slice(&SCOPE_PINS_VERSION.to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    bytes.extend_from_slice(&payload);
    crate::store::backend::FileBackend::new(path).save(&bytes)
}

fn promote_sidecar(pending: &Path, active: &Path) -> io::Result<()> {
    std::fs::rename(pending, active)?;
    crate::store::backend::fsync_dir(active.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "sidecar has no parent directory",
        )
    })?)
}

/// Snapshot the write-time policy version of every key in the certified
/// store, for the origins sidecar.
///
/// Reads `CertifiedApi::origins` — the API's first-class per-key pin map —
/// rather than deriving the set from `pending_writes`.
///
/// It USED to derive it, and that was a durability hole. `pending_writes`
/// is a bounded, EXPIRING tracking cache: `NodeRunner::run_cleanup` (5s by
/// default) drops entries past `max_age_ms` (60s), while the checkpoint
/// ticker runs on `snapshot_interval` (300s) and the checkpoint itself
/// prunes the WAL segments holding the same origins. So ~60s after a write
/// both carriers of its origin were erased in one step and the key
/// silently fell back to `unwrap_or(current_version)` on the next restart
/// — the FR-009 fence bypass that fix/fence-persistence-across-restart
/// closed, reopened by a timer. (The four `fenced_*_across_restart`
/// regression tests never caught it: they use `snapshot_interval: None`
/// with a manual checkpoint and never run a cleanup tick.)
///
/// EVERY key is captured, including keys whose writes were certified and
/// whose tracking entry is long gone. The WAL-only recovery path has no
/// status to filter on — it harvests the origin of every `CertifiedUpsert`
/// record regardless — so the sidecar must do the same or the two paths
/// diverge: a write certified under v_old, then fenced when the policy
/// bumps to v_new, would recover as v_old (Timeout) from the WAL but as
/// v_new (re-certified off the newer frontier) from a pruned-WAL
/// checkpoint. The certification state itself stays deliberately volatile:
/// on restart every write regresses to `Pending`, and a write whose origin
/// trails the current version stays pinned to that origin and re-fenced so
/// it cannot re-certify off a newer frontier.
fn certified_origins(api: &CertifiedApi) -> HashMap<String, PolicyVersion> {
    api.origins().clone()
}

/// Write a snapshot off the API lock and, on success, drop sealed WAL
/// segments (shared tail of both checkpoint functions).
async fn finish_checkpoint(
    label: &'static str,
    store: Store,
    snapshot_path: PathBuf,
    wal_dir: PathBuf,
    sealed: Option<u64>,
) -> io::Result<()> {
    let path = snapshot_path.clone();
    tokio::task::spawn_blocking(move || store.save_snapshot_bincode(&path))
        .await
        .map_err(io::Error::other)??;

    // Only AFTER the snapshot is durably renamed may sealed segments go.
    // Deletion is best-effort: a crash (or failure) here just leaves
    // harmless over-replay work for the next recovery.
    if let Some(sealed_seq) = sealed
        && let Err(e) = wal::remove_segments_up_to(&wal_dir, sealed_seq)
    {
        tracing::warn!(
            store = label,
            sealed_seq,
            error = %e,
            "failed to remove sealed WAL segments; they will be over-replayed harmlessly"
        );
    }
    tracing::debug!(store = label, snapshot = %snapshot_path.display(), "checkpoint complete");
    Ok(())
}

/// Checkpoint the eventual store: rotate + clone under the lock, snapshot
/// off the lock, then delete sealed segments.
///
/// On failure NO segment is deleted (the WAL keeps growing until a later
/// checkpoint succeeds — an ops monitoring item).
pub async fn checkpoint_eventual(
    api: &Arc<Mutex<EventualApi>>,
    cfg: &PersistenceConfig,
) -> io::Result<()> {
    // One checkpoint of this store at a time: the periodic ticker and the
    // shutdown-path final checkpoint must never interleave their
    // rotate→clone→save→delete sequences (see [`CheckpointLocks`]).
    let _serial = cfg.checkpoint_locks.eventual.lock().await;
    let (sealed, store) = {
        let mut api = api.lock().await;
        let sealed = api.wal_rotate()?;
        (sealed, api.store().clone())
    };
    finish_checkpoint(
        "eventual",
        store,
        cfg.data_dir.join(EVENTUAL_SNAPSHOT),
        cfg.data_dir.join(EVENTUAL_WAL_DIR),
        sealed,
    )
    .await
}

/// Checkpoint the certified store (same discipline as
/// [`checkpoint_eventual`]).
pub async fn checkpoint_certified(
    api: &Arc<Mutex<CertifiedApi>>,
    cfg: &PersistenceConfig,
) -> io::Result<()> {
    // Serialized for the same reason as `checkpoint_eventual`.
    let _serial = cfg.checkpoint_locks.certified.lock().await;
    let (sealed, store, origins, scope_pins) = {
        let mut api = api.lock().await;
        let sealed = api.wal_rotate()?;
        // Capture the origins under the SAME lock as the store clone so the
        // sidecar and the snapshot describe the same set of writes.
        let origins = certified_origins(&api);
        (
            sealed,
            api.store().clone(),
            origins,
            api.scope_pins().clone(),
        )
    };

    // Stage both sidecars without overwriting the active generation. If a
    // crash occurs before the snapshot rename, the old snapshot still needs
    // the old active pin for a same-key write whose earlier WAL was pruned.
    let pending_origins = cfg.data_dir.join(CERTIFIED_ORIGINS_PENDING);
    let pending_scopes = cfg.data_dir.join(CERTIFIED_SCOPE_PINS_PENDING);
    save_origins_sidecar(&pending_origins, &origins)?;
    save_scope_pins_sidecar(&pending_scopes, &scope_pins)?;

    let snapshot_path = cfg.data_dir.join(CERTIFIED_SNAPSHOT);
    let path = snapshot_path.clone();
    tokio::task::spawn_blocking(move || store.save_snapshot_bincode(&path))
        .await
        .map_err(io::Error::other)??;

    // After the snapshot is durable, promote both sidecars. A crash between
    // promotions leaves the sealed WAL intact, so replay restores any newer
    // same-key write that the older sidecar does not yet describe.
    promote_sidecar(&pending_origins, &cfg.data_dir.join(CERTIFIED_ORIGINS))?;
    promote_sidecar(&pending_scopes, &cfg.data_dir.join(CERTIFIED_SCOPE_PINS))?;
    use crate::store::backend::StorageBackend;
    crate::store::backend::FileBackend::new(cfg.data_dir.join(CERTIFIED_SCOPE_PINS_REQUIRED))
        .save(SCOPE_PINS_MARKER)?;

    // Only now can sealed WAL segments be removed. Deletion failure leaves
    // harmless over-replay work for the next boot.
    if let Some(sealed_seq) = sealed
        && let Err(error) =
            wal::remove_segments_up_to(&cfg.data_dir.join(CERTIFIED_WAL_DIR), sealed_seq)
    {
        tracing::warn!(sealed_seq, error = %error, "failed to prune certified WAL after checkpoint");
    }
    tracing::debug!(snapshot = %snapshot_path.display(), "certified checkpoint complete");
    Ok(())
}

/// Spawn the background persistence tasks: one WAL flusher per store and
/// (when enabled) the periodic checkpoint ticker. Kept separate from
/// `NodeRunner`'s tick loop so its select! stays untouched.
pub fn spawn_persistence_tasks(
    cfg: PersistenceConfig,
    eventual: Arc<Mutex<EventualApi>>,
    certified: Arc<Mutex<CertifiedApi>>,
    eventual_syncer: Option<Arc<WalSyncer>>,
    certified_syncer: Option<Arc<WalSyncer>>,
) {
    if !cfg.enabled {
        return;
    }
    for syncer in [eventual_syncer, certified_syncer].into_iter().flatten() {
        tokio::spawn(syncer.run_flusher());
    }
    if let Some(interval) = cfg.snapshot_interval {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick fires immediately; skip it so the initial
            // checkpoint happens one full interval after startup.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if let Err(e) = checkpoint_eventual(&eventual, &cfg).await {
                    tracing::warn!(error = %e, "eventual checkpoint failed; WAL segments retained");
                }
                if let Err(e) = checkpoint_certified(&certified, &cfg).await {
                    tracing::warn!(error = %e, "certified checkpoint failed; WAL segments retained");
                }
            }
        });
    }
}

#[cfg(test)]
mod scope_pin_tests {
    use super::*;
    use crate::api::certified::OnTimeout;
    use crate::control_plane::system_namespace::AuthorityDefinition;
    use crate::crdt::pn_counter::PnCounter;
    use crate::placement::PlacementPolicy;
    use crate::store::kv::CrdtValue;
    use crate::types::KeyRange;
    use std::sync::RwLock;

    fn namespace(members: &[&str]) -> Arc<RwLock<SystemNamespace>> {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "user/".into(),
            },
            authority_nodes: members.iter().map(|id| NodeId((*id).into())).collect(),
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(
            PolicyVersion(1),
            KeyRange {
                prefix: "user/".into(),
            },
            members.len(),
        ))
        .unwrap();
        Arc::new(RwLock::new(ns))
    }

    fn config(dir: &Path) -> PersistenceConfig {
        PersistenceConfig {
            enabled: true,
            data_dir: dir.to_path_buf(),
            sync: SyncPolicy::Off,
            snapshot_interval: None,
            segment_max_bytes: WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
            recover_truncate: false,
            checkpoint_locks: CheckpointLocks::default(),
        }
    }

    fn counter(value: u64) -> CrdtValue {
        let mut counter = PnCounter::new();
        for _ in 0..value {
            counter.increment(&NodeId("writer".into()));
        }
        CrdtValue::Counter(counter)
    }

    #[tokio::test]
    async fn checkpoint_restores_original_roster_after_live_change() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let ns = namespace(&["a", "b", "c"]);
        let (mut api, _) = recover_certified(NodeId("writer".into()), ns.clone(), &cfg).unwrap();
        api.certified_write("user/x".into(), counter(1), OnTimeout::Pending)
            .unwrap();
        let original = api.pending_writes()[0].authority_nodes.clone();
        let held = Arc::new(Mutex::new(api));
        checkpoint_certified(&held, &cfg).await.unwrap();
        drop(held);

        ns.write()
            .unwrap()
            .set_authority_definition(AuthorityDefinition {
                key_range: KeyRange {
                    prefix: "user/".into(),
                },
                authority_nodes: vec![NodeId("a".into()), NodeId("d".into()), NodeId("e".into())],
                auto_generated: false,
            });
        ns.write()
            .unwrap()
            .set_placement_policy(PlacementPolicy::new(
                PolicyVersion(2),
                KeyRange {
                    prefix: "user/".into(),
                },
                3,
            ))
            .unwrap();
        // The modern pin carries the origin version as well as the roster.
        // Recovery must not fall back to current v2 if the old origins
        // sidecar is missing after the checkpoint WAL was pruned.
        std::fs::remove_file(dir.path().join(CERTIFIED_ORIGINS)).unwrap();
        let (recovered, _) = recover_certified(NodeId("writer".into()), ns, &cfg).unwrap();
        assert_eq!(recovered.pending_writes()[0].authority_nodes, original);
        assert_eq!(
            recovered.pending_writes()[0].policy_version,
            PolicyVersion(1)
        );
        assert_eq!(recovered.scope_pins()["user/x"].authority_nodes, original);
    }

    #[tokio::test]
    async fn marked_checkpoint_fails_if_scope_sidecar_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let ns = namespace(&["a", "b", "c"]);
        let (mut api, _) = recover_certified(NodeId("writer".into()), ns.clone(), &cfg).unwrap();
        api.certified_write("user/x".into(), counter(1), OnTimeout::Pending)
            .unwrap();
        let held = Arc::new(Mutex::new(api));
        checkpoint_certified(&held, &cfg).await.unwrap();
        drop(held);
        std::fs::remove_file(dir.path().join(CERTIFIED_SCOPE_PINS)).unwrap();
        assert!(recover_certified(NodeId("writer".into()), ns, &cfg).is_err());
    }

    #[tokio::test]
    async fn staged_same_key_sidecars_do_not_replace_old_snapshot_pin() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let ns = namespace(&["a", "b", "c"]);
        let (mut api, _) = recover_certified(NodeId("writer".into()), ns.clone(), &cfg).unwrap();
        api.certified_write("user/x".into(), counter(1), OnTimeout::Pending)
            .unwrap();
        let old_pin = api.scope_pins()["user/x"].clone();
        let held = Arc::new(Mutex::new(api));
        checkpoint_certified(&held, &cfg).await.unwrap();

        ns.write()
            .unwrap()
            .set_authority_definition(AuthorityDefinition {
                key_range: KeyRange {
                    prefix: "user/".into(),
                },
                authority_nodes: vec![NodeId("a".into()), NodeId("d".into()), NodeId("e".into())],
                auto_generated: false,
            });
        {
            let mut api = held.lock().await;
            api.certified_write("user/x".into(), counter(2), OnTimeout::Pending)
                .unwrap();
            save_origins_sidecar(&dir.path().join(CERTIFIED_ORIGINS_PENDING), api.origins())
                .unwrap();
            save_scope_pins_sidecar(
                &dir.path().join(CERTIFIED_SCOPE_PINS_PENDING),
                api.scope_pins(),
            )
            .unwrap();
        }
        drop(held);
        // Simulate loss of the un-synced second write before snapshot rename.
        for (_, path) in wal::list_segments(&dir.path().join(CERTIFIED_WAL_DIR)).unwrap() {
            std::fs::remove_file(path).unwrap();
        }
        let (recovered, _) = recover_certified(NodeId("writer".into()), ns, &cfg).unwrap();
        assert_eq!(recovered.scope_pins()["user/x"], old_pin);
        assert_eq!(
            recovered.pending_writes()[0].authority_nodes,
            old_pin.authority_nodes
        );
    }

    #[tokio::test]
    async fn new_snapshot_with_old_sidecars_recovers_new_pin_from_retained_wal() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let ns = namespace(&["a", "b", "c"]);
        let (mut api, _) = recover_certified(NodeId("writer".into()), ns.clone(), &cfg).unwrap();
        api.certified_write("user/x".into(), counter(1), OnTimeout::Pending)
            .unwrap();
        let held = Arc::new(Mutex::new(api));
        checkpoint_certified(&held, &cfg).await.unwrap();

        ns.write()
            .unwrap()
            .set_authority_definition(AuthorityDefinition {
                key_range: KeyRange {
                    prefix: "user/".into(),
                },
                authority_nodes: vec![NodeId("a".into()), NodeId("d".into()), NodeId("e".into())],
                auto_generated: false,
            });
        let new_pin;
        {
            let mut api = held.lock().await;
            api.certified_write("user/x".into(), counter(2), OnTimeout::Pending)
                .unwrap();
            new_pin = api.scope_pins()["user/x"].clone();
            // Stop a checkpoint after snapshot save but before sidecar
            // promotion. The sealed WAL must remain available for replay.
            api.wal_rotate().unwrap();
            api.store()
                .save_snapshot_bincode(&dir.path().join(CERTIFIED_SNAPSHOT))
                .unwrap();
        }
        drop(held);
        let (recovered, _) = recover_certified(NodeId("writer".into()), ns, &cfg).unwrap();
        assert_eq!(recovered.scope_pins()["user/x"], new_pin);
        assert_eq!(
            recovered.pending_writes()[0].authority_nodes,
            new_pin.authority_nodes
        );
    }

    #[test]
    fn wal_only_restart_keeps_local_scope_pin() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let ns = namespace(&["a", "b", "c"]);
        let (mut api, _) = recover_certified(NodeId("writer".into()), ns.clone(), &cfg).unwrap();
        api.certified_write("user/x".into(), counter(1), OnTimeout::Pending)
            .unwrap();
        let pin = api.scope_pins()["user/x"].clone();
        drop(api);
        ns.write()
            .unwrap()
            .set_authority_definition(AuthorityDefinition {
                key_range: KeyRange {
                    prefix: "user/".into(),
                },
                authority_nodes: vec![NodeId("d".into()), NodeId("e".into()), NodeId("f".into())],
                auto_generated: false,
            });
        let (recovered, _) = recover_certified(NodeId("writer".into()), ns, &cfg).unwrap();
        assert_eq!(recovered.scope_pins()["user/x"], pin);
        assert_eq!(
            recovered.pending_writes()[0].authority_nodes,
            pin.authority_nodes
        );
    }

    #[test]
    fn certified_wal_without_scope_pin_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let ns = namespace(&["a", "b", "c"]);
        let mut writer = WalWriter::open(cfg.wal_config(CERTIFIED_WAL_DIR)).unwrap();
        writer
            .append(&wal::WalRecord::CertifiedUpsert {
                key: "user/x".into(),
                value: counter(1),
                hlc: crate::hlc::HlcTimestamp {
                    physical: 100,
                    logical: 0,
                    node_id: "writer".into(),
                },
                policy_version: PolicyVersion(1),
            })
            .unwrap();
        drop(writer);
        assert!(recover_certified(NodeId("writer".into()), ns, &cfg).is_err());
    }

    #[test]
    fn certified_snapshot_without_scope_pin_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let mut store = Store::new();
        store.merge_value("user/x".into(), &counter(1)).unwrap();
        store.record_change(
            "user/x",
            crate::hlc::HlcTimestamp {
                physical: 100,
                logical: 0,
                node_id: "writer".into(),
            },
        );
        store
            .save_snapshot_bincode(&dir.path().join(CERTIFIED_SNAPSHOT))
            .unwrap();

        assert!(
            recover_certified(NodeId("writer".into()), namespace(&["a", "b", "c"]), &cfg).is_err()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::kv::CrdtValue;

    fn cfg(dir: &Path) -> PersistenceConfig {
        PersistenceConfig {
            enabled: true,
            data_dir: dir.to_path_buf(),
            sync: SyncPolicy::Off,
            snapshot_interval: None,
            segment_max_bytes: WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
            recover_truncate: false,
            checkpoint_locks: CheckpointLocks::default(),
        }
    }

    #[test]
    fn lost_wal_dot_cannot_be_reused_after_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let config = cfg(dir.path());
        let identity = NodeId("origin".into());
        let (mut first, _) = recover_eventual(identity.clone(), &config).unwrap();
        first
            .eventual_map_set("map", "old".into(), "first".into())
            .unwrap();
        first.eventual_set_add("set", "old".into()).unwrap();
        let peer_map = first.get_eventual("map").unwrap().clone();
        let peer_set = first.get_eventual("set").unwrap().clone();
        drop(first);

        // An unsynced WAL tail is lost on the origin while another node
        // already holds the two dots. The durable incarnation reservation
        // remains even when the final segment itself disappears.
        for (_, path) in wal::list_segments(&dir.path().join(EVENTUAL_WAL_DIR)).unwrap() {
            std::fs::remove_file(path).unwrap();
        }
        let (mut restarted, _) = recover_eventual(identity, &config).unwrap();
        restarted
            .eventual_map_set("map", "new".into(), "second".into())
            .unwrap();
        restarted.eventual_set_add("set", "new".into()).unwrap();

        let mut merged_map = restarted.get_eventual("map").unwrap().clone();
        merged_map.validate_dot_assignments(&peer_map).unwrap();
        merged_map.merge_delta(&peer_map).unwrap();
        let CrdtValue::Map(map) = merged_map else {
            panic!("expected map");
        };
        assert_eq!(map.get(&"old".to_string()), Some(&"first".to_string()));
        assert_eq!(map.get(&"new".to_string()), Some(&"second".to_string()));

        let mut merged_set = restarted.get_eventual("set").unwrap().clone();
        merged_set.validate_dot_assignments(&peer_set).unwrap();
        merged_set.merge_delta(&peer_set).unwrap();
        let CrdtValue::Set(set) = merged_set else {
            panic!("expected set");
        };
        assert!(set.contains(&"old".to_string()));
        assert!(set.contains(&"new".to_string()));
    }

    #[tokio::test]
    async fn checkpoint_prunes_segments_and_recovery_composes() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg(dir.path());

        let (api, _syncer) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
        let api = Arc::new(Mutex::new(api));
        {
            let mut guard = api.lock().await;
            guard.eventual_counter_inc("pre").unwrap();
        }
        checkpoint_eventual(&api, &cfg).await.unwrap();

        // The sealed pre-checkpoint segment must be gone; only the active
        // one remains.
        let segments = wal::list_segments(&dir.path().join(EVENTUAL_WAL_DIR)).unwrap();
        assert_eq!(segments.len(), 1, "sealed segments must be pruned");

        {
            let mut guard = api.lock().await;
            guard.eventual_counter_inc("post").unwrap();
        }
        drop(api); // crash

        let (api, _syncer) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
        assert!(api.get_eventual("pre").is_some(), "from the snapshot");
        assert!(api.get_eventual("post").is_some(), "from the WAL");
        match api.get_eventual("pre") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn disabled_persistence_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg(dir.path());
        cfg.enabled = false;
        let (mut api, syncer) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
        assert!(syncer.is_none());
        api.eventual_counter_inc("k").unwrap();
        assert!(
            !dir.path().join(EVENTUAL_WAL_DIR).exists(),
            "persistence off must leave the data dir untouched"
        );
    }

    #[test]
    fn corrupt_snapshot_is_fail_stop_not_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg(dir.path());
        std::fs::write(dir.path().join(EVENTUAL_SNAPSHOT), b"garbage").unwrap();
        let err = recover_eventual(NodeId("node-a".into()), &cfg)
            .err()
            .expect("a damaged snapshot must fail recovery, not start empty");
        assert!(err.to_string().contains("snapshot"));
    }

    #[test]
    fn mid_log_corruption_fail_stops_without_truncate_flag() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_default = cfg(dir.path());
        // Produce a WAL with two records, then flip a byte in the first.
        {
            let (mut api, _s) = recover_eventual(NodeId("node-a".into()), &cfg_default).unwrap();
            api.eventual_counter_inc("a").unwrap();
            api.eventual_counter_inc("b").unwrap();
        }
        let wal_dir = dir.path().join(EVENTUAL_WAL_DIR);
        let (_, seg) = wal::list_segments(&wal_dir).unwrap().pop().unwrap();
        let mut data = std::fs::read(&seg).unwrap();
        data[16 + 8 + 1] ^= 0xFF; // first record payload
        std::fs::write(&seg, &data).unwrap();

        let err = recover_eventual(NodeId("node-a".into()), &cfg_default)
            .err()
            .expect("mid-log corruption must fail-stop by default");
        assert!(err.to_string().contains("corrupted"));

        // Explicit escape hatch: truncate at the first invalid record.
        let mut cfg_truncate = cfg_default.clone();
        cfg_truncate.recover_truncate = true;
        let (api, _s) = recover_eventual(NodeId("node-a".into()), &cfg_truncate).unwrap();
        assert!(
            api.get_eventual("a").is_none() && api.get_eventual("b").is_none(),
            "truncation keeps only records before the corruption"
        );
    }

    /// A torn-tail recovery must repair the segment ON DISK: the next boot
    /// sees it as a non-final segment (the recovered process opened a new
    /// one), where the same torn bytes would read as fail-stop mid-log
    /// corruption — and acked writes appended after the first recovery
    /// would be unreachable behind it.
    #[test]
    fn torn_tail_recovery_survives_a_second_crash() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg(dir.path());
        {
            let (mut api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
            api.eventual_counter_inc("a").unwrap();
        }
        // Crash mid-append: a garbage partial frame at the segment tail.
        let wal_dir = dir.path().join(EVENTUAL_WAL_DIR);
        let (_, seg) = wal::list_segments(&wal_dir).unwrap().pop().unwrap();
        let mut data = std::fs::read(&seg).unwrap();
        data.extend_from_slice(&[0xDE, 0xAD, 0xBE]);
        std::fs::write(&seg, &data).unwrap();

        // First recovery (torn tail) + an acked write into the NEW segment.
        {
            let (mut api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
            assert!(api.get_eventual("a").is_some(), "pre-tear record survives");
            api.eventual_counter_inc("b").unwrap();
        } // second crash before any checkpoint

        // Second recovery with DEFAULT config (no truncate flag): must not
        // fail-stop, and both acked writes must be present.
        let (api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
        assert!(api.get_eventual("a").is_some(), "pre-tear acked write");
        assert!(
            api.get_eventual("b").is_some(),
            "acked write from after the torn-tail recovery must survive a second crash"
        );
    }

    /// A truncating recovery must physically remove the corruption from
    /// disk: otherwise writes acked after it land in later segments that
    /// every subsequent boot stops short of (fail-stop without the flag,
    /// silent loss with it).
    #[test]
    fn truncate_recovery_prunes_corruption_for_subsequent_boots() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_default = cfg(dir.path());
        {
            let (mut api, _s) = recover_eventual(NodeId("node-a".into()), &cfg_default).unwrap();
            api.eventual_counter_inc("a").unwrap();
            api.eventual_counter_inc("b").unwrap();
        }
        // Mid-log corruption: damage the first record's payload.
        let wal_dir = dir.path().join(EVENTUAL_WAL_DIR);
        let (_, seg) = wal::list_segments(&wal_dir).unwrap().pop().unwrap();
        let mut data = std::fs::read(&seg).unwrap();
        data[16 + 8 + 1] ^= 0xFF;
        std::fs::write(&seg, &data).unwrap();

        // Operator opts into truncation; the node then acks a new write.
        let mut cfg_truncate = cfg_default.clone();
        cfg_truncate.recover_truncate = true;
        {
            let (mut api, _s) = recover_eventual(NodeId("node-a".into()), &cfg_truncate).unwrap();
            api.eventual_counter_inc("post").unwrap();
        } // crash before any checkpoint

        // Restart WITHOUT the flag (as the ops runbook instructs): the
        // already-handled corruption must not resurface, and the acked
        // post-recovery write must be visible.
        let (api, _s) = recover_eventual(NodeId("node-a".into()), &cfg_default).unwrap();
        assert!(
            api.get_eventual("post").is_some(),
            "acked post-recovery write must survive the second crash"
        );
        assert!(
            api.get_eventual("a").is_none() && api.get_eventual("b").is_none(),
            "records at/after the corruption stay dropped"
        );
    }

    // ---------------------------------------------------------------
    // Recovery gap fence (M-3): lost acked writes must answer 412,
    // never a false success — until anti-entropy adoption re-covers them.
    // ---------------------------------------------------------------

    /// WAL sync=off: an ACKED suffix can be lost in a crash. After
    /// recovery, the first local write leapfrogs `applied_origins[self]`
    /// past the hole — without the fence, the lost write's session token
    /// would wrongly pass evidence path A. With it, the token answers
    /// 412 until a peer's applied frontier is adopted (proving the range
    /// re-covered), after which it answers 200 again.
    #[test]
    fn lost_acked_suffix_fences_session_tokens_until_adopted() {
        use crate::session::SessionToken;

        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg(dir.path()); // SyncPolicy::Off

        // Incarnation 1: one durable write, then one acked write whose
        // WAL bytes never reach disk (simulated by truncating the file
        // back to the pre-write length).
        let wal_dir = dir.path().join(EVENTUAL_WAL_DIR);
        let (lost_ts, token, seg_path, keep_len) = {
            let (mut api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
            api.eventual_counter_inc("k").unwrap();
            let (_, seg) = wal::list_segments(&wal_dir).unwrap().pop().unwrap();
            let keep_len = std::fs::metadata(&seg).unwrap().len();
            let lost_ts = api.eventual_counter_inc("k").unwrap(); // acked, then lost
            (
                lost_ts.clone(),
                SessionToken::from_hlc(&lost_ts),
                seg,
                keep_len,
            )
        };
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&seg_path)
            .unwrap();
        f.set_len(keep_len).unwrap();
        drop(f);

        // Incarnation 2: recovery + a fresh local write that advances the
        // applied frontier PAST the lost HLC (the leapfrog of the bug).
        let (mut api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
        api.eventual_counter_inc("other").unwrap();
        assert!(
            api.store().applied_origin("node-a").unwrap() > &lost_ts,
            "precondition: the frontier has leapfrogged the lost write"
        );
        assert!(
            !api.session_check("k", &token),
            "a lost acked write's token must answer 412, not a false success"
        );

        // A peer that still holds the write proves re-coverage through
        // frontier adoption — the token turns 200 again.
        let mut applied = std::collections::HashMap::new();
        applied.insert("node-a".to_string(), lost_ts);
        api.adopt_session_claims(&applied, &std::collections::HashMap::new(), Vec::new())
            .unwrap();
        assert!(
            api.session_check("k", &token),
            "adoption must heal the fence and re-satisfy the token"
        );
    }

    /// M-3 re-review regression: the fence must survive a SECOND crash
    /// before the first periodic checkpoint. `install_recovery_fence`
    /// only mutates the in-memory store and the WAL has no fence record,
    /// so without the forced snapshot in `recover_eventual` this
    /// three-incarnation sequence reopened the false-success path:
    /// crash 1 loses an acked suffix; incarnation 2 installs the fence
    /// and durably logs a fresh write ABOVE the fence ceiling; crash 2;
    /// incarnation 3's WAL replay then advances `applied_origins[self]`
    /// past the old ceiling while its own (new) fence spans a range above
    /// it — the original hole would be unfenced and the lost write's
    /// token would answer 200 off a store missing the data.
    #[test]
    fn recovery_fence_survives_a_second_crash_before_any_checkpoint() {
        use crate::session::SessionToken;

        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg(dir.path()); // SyncPolicy::Off, no periodic checkpoints

        // Incarnation 1: one durable write, then an acked write whose WAL
        // bytes never reach disk (simulated by truncating the segment).
        let wal_dir = dir.path().join(EVENTUAL_WAL_DIR);
        let (lost_ts, token, seg_path, keep_len) = {
            let (mut api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
            api.eventual_counter_inc("k").unwrap();
            let (_, seg) = wal::list_segments(&wal_dir).unwrap().pop().unwrap();
            let keep_len = std::fs::metadata(&seg).unwrap().len();
            let lost_ts = api.eventual_counter_inc("k").unwrap(); // acked, then lost
            (
                lost_ts.clone(),
                SessionToken::from_hlc(&lost_ts),
                seg,
                keep_len,
            )
        };
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&seg_path)
            .unwrap();
        f.set_len(keep_len).unwrap();
        drop(f);

        // Incarnation 2: the fence is installed (and force-snapshotted);
        // a fresh local write above the fence ceiling is durably logged.
        {
            let (mut api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
            assert!(
                !api.store().recovery_gaps().is_empty(),
                "precondition: incarnation 2 installed a fence"
            );
            assert!(
                !api.session_check("k", &token),
                "incarnation 2 fences the lost write"
            );
            api.eventual_counter_inc("other").unwrap();
        } // crash 2 — before any checkpoint ever ran

        // Incarnation 3: WAL replay advances applied_origins[self] past
        // the OLD fence ceiling. The persisted fence must still cover the
        // original hole.
        let (mut api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
        assert!(
            api.store().applied_origin("node-a").unwrap() > &lost_ts,
            "precondition: replay leapfrogged the lost write again"
        );
        assert!(
            !api.session_check("k", &token),
            "the lost write's token must STILL answer 412 after a second crash \
             (the fence must be durable, not in-memory only)"
        );

        // Adoption still heals the persisted fence (tokens turn 200).
        let mut applied = std::collections::HashMap::new();
        applied.insert("node-a".to_string(), lost_ts);
        api.adopt_session_claims(&applied, &std::collections::HashMap::new(), Vec::new())
            .unwrap();
        assert!(
            api.session_check("k", &token),
            "adoption must heal the persisted fence"
        );
    }

    /// PERSISTENCE=off: every acked write of a previous incarnation is
    /// gone on restart. The fresh incarnation's writes must not let a
    /// prior incarnation's token pass path A.
    #[test]
    fn persistence_off_fences_prior_incarnation_tokens() {
        use crate::session::SessionToken;

        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg(dir.path());
        cfg.enabled = false;

        // Incarnation 1: a single acked write, then a crash losing it.
        let token = {
            let (mut api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
            let ts = api.eventual_counter_inc("k").unwrap();
            SessionToken::from_hlc(&ts)
        };

        // A real restart never completes within the same wall-clock
        // millisecond as the previous incarnation's writes (which the
        // fence-installation clock bump stamped ~1 ms ahead); model that
        // so the new incarnation's fence ceiling covers the lost write.
        std::thread::sleep(std::time::Duration::from_millis(3));

        // Incarnation 2: fresh in-memory store; a new local write
        // advances applied_origins[self] past the lost token.
        let (mut api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
        api.eventual_counter_inc("k").unwrap();
        assert!(
            !api.session_check("k", &token),
            "a lost incarnation's token must answer 412, not a false success"
        );
    }

    /// SyncPolicy::Always with a clean (or torn-tail) WAL loses only
    /// un-acked writes: no fence is installed and pre-crash tokens keep
    /// answering 200 immediately after recovery.
    #[test]
    fn sync_always_recovery_installs_no_fence() {
        use crate::session::SessionToken;

        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg(dir.path());
        cfg.sync = SyncPolicy::Always;

        let token = {
            let (mut api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
            let ts = api.eventual_counter_inc("k").unwrap();
            SessionToken::from_hlc(&ts)
        };
        let (api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
        assert!(api.store().recovery_gaps().is_empty(), "no fence expected");
        assert!(
            api.session_check("k", &token),
            "a durably-acked token must stay satisfied across recovery"
        );
    }

    /// Checkpoints of the same store must serialize (periodic ticker vs
    /// shutdown path): interleaved rotate→clone→save→delete sequences can
    /// rename an older snapshot over a newer one after the newer
    /// checkpoint already pruned WAL segments.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_checkpoints_do_not_lose_acked_writes() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg(dir.path());
        let (api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
        let api = Arc::new(Mutex::new(api));

        for round in 0..8u32 {
            {
                let mut guard = api.lock().await;
                guard.eventual_counter_inc(&format!("k{round}")).unwrap();
            }
            let a = tokio::spawn({
                let api = Arc::clone(&api);
                let cfg = cfg.clone();
                async move { checkpoint_eventual(&api, &cfg).await }
            });
            let b = tokio::spawn({
                let api = Arc::clone(&api);
                let cfg = cfg.clone();
                async move { checkpoint_eventual(&api, &cfg).await }
            });
            a.await.unwrap().unwrap();
            b.await.unwrap().unwrap();
        }
        drop(api); // crash

        let (api, _s) = recover_eventual(NodeId("node-a".into()), &cfg).unwrap();
        for round in 0..8u32 {
            assert!(
                api.get_eventual(&format!("k{round}")).is_some(),
                "acked write k{round} lost across concurrent checkpoints"
            );
        }
    }
}
