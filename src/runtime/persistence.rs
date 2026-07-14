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
/// Returns the recovered store and the writer. Fail-stop errors: damaged
/// snapshot, or mid-log corruption without the truncate escape hatch.
fn recover_store(
    label: &str,
    snapshot_path: &Path,
    wal_cfg: WalConfig,
    recover_truncate: bool,
) -> io::Result<(Store, WalWriter)> {
    let started = std::time::Instant::now();
    let mut store = Store::load_snapshot_bincode_or_default(snapshot_path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "failed to load {label} snapshot {}: {e}. A damaged snapshot is never \
                 replaced silently — move the data directory aside and restart to rebuild \
                 from peers via anti-entropy (see docs/ops-guide.md)",
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
                         Either rebuild this node from peers (move the data directory aside \
                         and restart) or set ASTEROIDB_WAL_RECOVER_TRUNCATE=1 to explicitly \
                         truncate at the first invalid record (see docs/ops-guide.md)",
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
    Ok((store, writer))
}

/// Recover the eventual store and build its API + WAL syncer.
pub fn recover_eventual(
    node_id: NodeId,
    cfg: &PersistenceConfig,
) -> io::Result<(EventualApi, Option<Arc<WalSyncer>>)> {
    if !cfg.enabled {
        return Ok((EventualApi::new(node_id), None));
    }
    let (store, writer) = recover_store(
        "eventual",
        &cfg.data_dir.join(EVENTUAL_SNAPSHOT),
        cfg.wal_config(EVENTUAL_WAL_DIR),
        cfg.recover_truncate,
    )?;
    let syncer = Arc::new(writer.syncer());
    Ok((
        EventualApi::recovered(node_id, store, Some(writer)),
        Some(syncer),
    ))
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
    let (store, writer) = recover_store(
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
