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

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::api::certified::CertifiedApi;
use crate::api::eventual::EventualApi;
use crate::control_plane::system_namespace::SystemNamespace;
use crate::store::Store;
use crate::store::wal::{self, SyncPolicy, WalConfig, WalReadOutcome, WalSyncer, WalWriter};
use crate::types::NodeId;

/// Snapshot file name for the eventual store.
const EVENTUAL_SNAPSHOT: &str = "eventual.snapshot.bin";
/// Snapshot file name for the certified store.
const CERTIFIED_SNAPSHOT: &str = "certified.snapshot.bin";
/// WAL subdirectory for the eventual store.
const EVENTUAL_WAL_DIR: &str = "wal/eventual";
/// WAL subdirectory for the certified store.
const CERTIFIED_WAL_DIR: &str = "wal/certified";

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
            "interval" => SyncPolicy::Interval(Duration::from_millis(env_u64(
                "ASTEROIDB_WAL_SYNC_INTERVAL_MS",
                100,
            ))),
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
fn recover_store(
    label: &str,
    snapshot_path: &Path,
    wal_cfg: WalConfig,
    recover_truncate: bool,
) -> io::Result<(Store, WalWriter, WalReadOutcome)> {
    let started = std::time::Instant::now();
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
    for record in read.records {
        wal::replay_record(&mut store, record);
    }

    let writer = WalWriter::open(wal_cfg)?;
    tracing::info!(
        store = label,
        snapshot = %snapshot_path.display(),
        wal_segments = read.segments,
        wal_records = replayed,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "recovery complete"
    );
    Ok((store, writer, read.outcome))
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
    let (store, writer, outcome) = recover_store(
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
    let (store, writer, _outcome) = recover_store(
        "certified",
        &cfg.data_dir.join(CERTIFIED_SNAPSHOT),
        cfg.wal_config(CERTIFIED_WAL_DIR),
        cfg.recover_truncate,
    )?;
    let syncer = Arc::new(writer.syncer());
    Ok((
        CertifiedApi::recovered(node_id, namespace, store, Some(writer)),
        Some(syncer),
    ))
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
    let (sealed, store) = {
        let mut api = api.lock().await;
        let sealed = api.wal_rotate()?;
        (sealed, api.store().clone())
    };
    finish_checkpoint(
        "certified",
        store,
        cfg.data_dir.join(CERTIFIED_SNAPSHOT),
        cfg.data_dir.join(CERTIFIED_WAL_DIR),
        sealed,
    )
    .await
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
