//! Durable Raft state (fail-stop, never best-effort).
//!
//! Two files under `<data_dir>/raft/`:
//!
//! - `hard_state.json` — `currentTerm` / `votedFor`. Written (and fsynced,
//!   via `ops::write_atomic`) before any vote is granted or any term bump is
//!   acted upon. Losing it would allow double voting in one term.
//! - `log.json` — the snapshot (meta + [`ControlPlaneState`]) AND the log
//!   tail, co-persisted atomically in a single file. Compaction therefore
//!   cannot be torn: there is no window where the log prefix is dropped but
//!   the covering snapshot is not yet durable.
//!
//! The control-plane log is small by construction (low-frequency policy /
//! authority mutations, compacted above `raft_log_max` entries), so each
//! save rewrites the whole file — correctness over throughput.
//!
//! Format is JSON, matching the `system_namespace.json` convention: human
//! debuggable, and immune to bincode's field-order pitfalls.
//!
//! Unlike `persist_namespace` (best-effort), every error here is surfaced
//! to the caller, which abandons the in-flight response (no vote / no ack /
//! proposal fails with 503). A corrupt file at load time is a hard startup
//! error — booting with damaged Raft state risks split-brain (see
//! docs/ops-guide.md for the recovery runbook). The same applies to
//! *inconsistent* state: a log without a hard state, or a hard state whose
//! `currentTerm` is below the maximum log term, is refused at load time.

use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

use crate::ops::write_atomic;

use super::core::{HardState, SnapshotMeta};
use super::types::{ControlPlaneState, LogEntry};

/// Everything a node restores at startup.
#[derive(Debug, Clone, PartialEq)]
pub struct PersistedRaft {
    pub hard: HardState,
    pub snapshot_meta: SnapshotMeta,
    pub snapshot_state: ControlPlaneState,
    pub entries: Vec<LogEntry>,
}

/// Durable storage for Raft state. All methods are synchronous: callers
/// invoke them inside the node lock so that no response can overtake its
/// own persistence (control-plane writes are rare; correctness first).
pub trait RaftStorage: Send + Sync {
    fn save_hard_state(&self, hard: &HardState) -> Result<(), String>;
    fn save_log(
        &self,
        meta: &SnapshotMeta,
        state: &ControlPlaneState,
        entries: &[LogEntry],
    ) -> Result<(), String>;
    fn load(&self) -> Result<Option<PersistedRaft>, String>;
}

// ---------------------------------------------------------------
// File-backed storage
// ---------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotSection {
    last_included_index: u64,
    last_included_term: u64,
    state: ControlPlaneState,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogFile {
    snapshot: SnapshotSection,
    entries: Vec<LogEntry>,
}

/// JSON files under `dir` (typically `<data_dir>/raft/`), written with
/// `write_atomic` (temp + fsync + rename + parent-dir fsync).
pub struct FileRaftStorage {
    dir: PathBuf,
}

impl FileRaftStorage {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn hard_path(&self) -> PathBuf {
        self.dir.join("hard_state.json")
    }

    fn log_path(&self) -> PathBuf {
        self.dir.join("log.json")
    }
}

impl RaftStorage for FileRaftStorage {
    fn save_hard_state(&self, hard: &HardState) -> Result<(), String> {
        let json = serde_json::to_vec_pretty(hard).map_err(|e| e.to_string())?;
        write_atomic(&self.hard_path(), &json)
    }

    fn save_log(
        &self,
        meta: &SnapshotMeta,
        state: &ControlPlaneState,
        entries: &[LogEntry],
    ) -> Result<(), String> {
        if entries.len() > 512 {
            tracing::warn!(
                entries = entries.len(),
                "control-plane raft log tail is unusually large; each save rewrites the file"
            );
        }
        let file = LogFile {
            snapshot: SnapshotSection {
                last_included_index: meta.last_included_index,
                last_included_term: meta.last_included_term,
                state: state.clone(),
            },
            entries: entries.to_vec(),
        };
        let json = serde_json::to_vec_pretty(&file).map_err(|e| e.to_string())?;
        write_atomic(&self.log_path(), &json)
    }

    fn load(&self) -> Result<Option<PersistedRaft>, String> {
        let hard_path = self.hard_path();
        let log_path = self.log_path();
        if !hard_path.exists() && !log_path.exists() {
            return Ok(None); // first boot
        }
        // A log without a hard state is never a legal state produced by this
        // code: every entry carries a term >= 1, and observing a term >= 1
        // persists the hard state BEFORE any log write. Defaulting to
        // (term 0, no vote) here would forget a possibly-granted vote and
        // enable double voting in an already-voted term — fail-stop instead.
        if !hard_path.exists() {
            return Err(format!(
                "raft log {} exists but hard state {} is missing; booting without \
                 currentTerm/votedFor could double-vote in an already-voted term \
                 (see docs/ops-guide.md, Control-plane Raft recovery runbook)",
                log_path.display(),
                hard_path.display(),
            ));
        }
        // Parse failures are FATAL (fail-stop): restarting with damaged
        // Raft state could double-vote or lose committed entries.
        let hard: HardState = {
            let data = std::fs::read_to_string(&hard_path)
                .map_err(|e| format!("read {}: {e}", hard_path.display()))?;
            serde_json::from_str(&data)
                .map_err(|e| format!("corrupt raft hard state {}: {e}", hard_path.display()))?
        };
        let (snapshot_meta, snapshot_state, entries) = if log_path.exists() {
            let data = std::fs::read_to_string(&log_path)
                .map_err(|e| format!("read {}: {e}", log_path.display()))?;
            let file: LogFile = serde_json::from_str(&data)
                .map_err(|e| format!("corrupt raft log {}: {e}", log_path.display()))?;
            (
                SnapshotMeta {
                    last_included_index: file.snapshot.last_included_index,
                    last_included_term: file.snapshot.last_included_term,
                },
                file.snapshot.state,
                file.entries,
            )
        } else {
            (
                SnapshotMeta::default(),
                ControlPlaneState::default(),
                Vec::new(),
            )
        };
        // currentTerm >= max(log term) invariant: hard state is persisted
        // before any entry of a newer term is appended, so a hard state
        // older than the log means the files are from different points in
        // time (partial restore). Booting anyway could double-vote.
        let max_log_term = entries
            .last()
            .map(|e| e.term)
            .unwrap_or(0)
            .max(snapshot_meta.last_included_term);
        if hard.current_term < max_log_term {
            return Err(format!(
                "inconsistent raft state: current_term {} in {} is below the \
                 maximum log term {} in {}; the files are from different points \
                 in time (see docs/ops-guide.md, Control-plane Raft recovery runbook)",
                hard.current_term,
                hard_path.display(),
                max_log_term,
                log_path.display(),
            ));
        }
        Ok(Some(PersistedRaft {
            hard,
            snapshot_meta,
            snapshot_state,
            entries,
        }))
    }
}

// ---------------------------------------------------------------
// In-memory storage (tests) with failure injection
// ---------------------------------------------------------------

#[derive(Default)]
struct MemState {
    hard: Option<HardState>,
    log: Option<(SnapshotMeta, ControlPlaneState, Vec<LogEntry>)>,
}

/// In-memory `RaftStorage` for tests. `set_fail(true)` makes every save
/// return an error, for verifying the "no response before persistence"
/// invariant.
#[derive(Default)]
pub struct MemRaftStorage {
    state: Mutex<MemState>,
    fail: AtomicBool,
}

impl MemRaftStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inject persistence failures.
    pub fn set_fail(&self, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
    }

    /// The persisted hard state, if any (test assertions).
    pub fn persisted_hard(&self) -> Option<HardState> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .hard
            .clone()
    }

    /// The persisted log entries, if any (test assertions).
    pub fn persisted_entries(&self) -> Option<Vec<LogEntry>> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .log
            .as_ref()
            .map(|(_, _, e)| e.clone())
    }
}

impl RaftStorage for MemRaftStorage {
    fn save_hard_state(&self, hard: &HardState) -> Result<(), String> {
        if self.fail.load(Ordering::SeqCst) {
            return Err("injected hard-state persistence failure".into());
        }
        self.state.lock().unwrap_or_else(|e| e.into_inner()).hard = Some(hard.clone());
        Ok(())
    }

    fn save_log(
        &self,
        meta: &SnapshotMeta,
        state: &ControlPlaneState,
        entries: &[LogEntry],
    ) -> Result<(), String> {
        if self.fail.load(Ordering::SeqCst) {
            return Err("injected log persistence failure".into());
        }
        self.state.lock().unwrap_or_else(|e| e.into_inner()).log =
            Some((*meta, state.clone(), entries.to_vec()));
        Ok(())
    }

    fn load(&self) -> Result<Option<PersistedRaft>, String> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if guard.hard.is_none() && guard.log.is_none() {
            return Ok(None);
        }
        let (snapshot_meta, snapshot_state, entries) = guard.log.clone().unwrap_or((
            SnapshotMeta::default(),
            ControlPlaneState::default(),
            Vec::new(),
        ));
        Ok(Some(PersistedRaft {
            hard: guard.hard.clone().unwrap_or_default(),
            snapshot_meta,
            snapshot_state,
            entries,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::raft::types::ControlPlaneCommand;
    use crate::types::NodeId;

    fn sample_entries() -> Vec<LogEntry> {
        vec![
            LogEntry {
                index: 6,
                term: 2,
                command: ControlPlaneCommand::Noop,
            },
            LogEntry {
                index: 7,
                term: 3,
                command: ControlPlaneCommand::RemovePolicy {
                    prefix: "user/".into(),
                },
            },
        ]
    }

    #[test]
    fn file_storage_save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftStorage::new(dir.path().join("raft"));

        assert!(storage.load().unwrap().is_none(), "first boot loads None");

        let hard = HardState {
            current_term: 7,
            voted_for: Some(NodeId("n2".into())),
        };
        storage.save_hard_state(&hard).unwrap();

        let meta = SnapshotMeta {
            last_included_index: 5,
            last_included_term: 2,
        };
        let state = ControlPlaneState {
            bootstrapped: true,
            version_counter: 5,
            ..Default::default()
        };
        storage.save_log(&meta, &state, &sample_entries()).unwrap();

        // "Restart": a new storage instance over the same directory.
        let restarted = FileRaftStorage::new(dir.path().join("raft"));
        let loaded = restarted.load().unwrap().unwrap();
        assert_eq!(loaded.hard, hard);
        assert_eq!(loaded.snapshot_meta, meta);
        assert_eq!(loaded.snapshot_state, state);
        assert_eq!(loaded.entries, sample_entries());
    }

    #[test]
    fn file_storage_corrupt_hard_state_is_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let raft_dir = dir.path().join("raft");
        std::fs::create_dir_all(&raft_dir).unwrap();
        std::fs::write(raft_dir.join("hard_state.json"), "not json {{{").unwrap();
        let storage = FileRaftStorage::new(raft_dir);
        let err = storage
            .load()
            .expect_err("corrupt hard state must be fatal");
        assert!(err.contains("corrupt raft hard state"), "{err}");
    }

    #[test]
    fn file_storage_corrupt_log_is_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let raft_dir = dir.path().join("raft");
        let storage = FileRaftStorage::new(raft_dir.clone());
        storage.save_hard_state(&HardState::default()).unwrap();
        std::fs::write(raft_dir.join("log.json"), "{\"broken\": true").unwrap();
        let err = storage.load().expect_err("corrupt log must be fatal");
        assert!(err.contains("corrupt raft log"), "{err}");
    }

    #[test]
    fn file_storage_log_without_hard_state_is_fatal() {
        // A surviving log.json with a missing hard_state.json (partial disk
        // restore / accidental deletion) must NOT silently boot with
        // HardState::default() — that forgets votedFor and enables double
        // voting in an already-voted term.
        let dir = tempfile::tempdir().unwrap();
        let raft_dir = dir.path().join("raft");
        let storage = FileRaftStorage::new(raft_dir.clone());
        storage
            .save_hard_state(&HardState {
                current_term: 7,
                voted_for: Some(NodeId("n1".into())),
            })
            .unwrap();
        storage
            .save_log(
                &SnapshotMeta::default(),
                &ControlPlaneState::default(),
                &sample_entries(),
            )
            .unwrap();
        std::fs::remove_file(raft_dir.join("hard_state.json")).unwrap();

        let err = storage
            .load()
            .expect_err("log without hard state must be fatal");
        assert!(err.contains("hard state"), "{err}");
    }

    #[test]
    fn file_storage_hard_state_term_below_log_term_is_fatal() {
        // hard_state.json restored from an older backup than log.json:
        // currentTerm < max(log term) violates the persist-before-append
        // invariant and must be fail-stop.
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftStorage::new(dir.path().join("raft"));
        storage
            .save_hard_state(&HardState {
                current_term: 2, // sample_entries reach term 3
                voted_for: None,
            })
            .unwrap();
        storage
            .save_log(
                &SnapshotMeta::default(),
                &ControlPlaneState::default(),
                &sample_entries(),
            )
            .unwrap();
        let err = storage
            .load()
            .expect_err("hard state older than the log must be fatal");
        assert!(err.contains("inconsistent raft state"), "{err}");

        // A snapshot term newer than the hard state is equally fatal.
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftStorage::new(dir.path().join("raft"));
        storage
            .save_hard_state(&HardState {
                current_term: 1,
                voted_for: None,
            })
            .unwrap();
        storage
            .save_log(
                &SnapshotMeta {
                    last_included_index: 5,
                    last_included_term: 4,
                },
                &ControlPlaneState::default(),
                &[],
            )
            .unwrap();
        let err = storage
            .load()
            .expect_err("hard state older than the snapshot must be fatal");
        assert!(err.contains("inconsistent raft state"), "{err}");
    }

    #[test]
    fn file_storage_hard_state_only_uses_empty_log() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileRaftStorage::new(dir.path().join("raft"));
        storage
            .save_hard_state(&HardState {
                current_term: 1,
                voted_for: None,
            })
            .unwrap();
        let loaded = storage.load().unwrap().unwrap();
        assert_eq!(loaded.hard.current_term, 1);
        assert!(loaded.entries.is_empty());
        assert_eq!(loaded.snapshot_meta, SnapshotMeta::default());
    }

    #[test]
    fn mem_storage_failure_injection() {
        let storage = MemRaftStorage::new();
        storage.set_fail(true);
        assert!(storage.save_hard_state(&HardState::default()).is_err());
        assert!(
            storage
                .save_log(&SnapshotMeta::default(), &ControlPlaneState::default(), &[])
                .is_err()
        );
        storage.set_fail(false);
        assert!(storage.save_hard_state(&HardState::default()).is_ok());
        assert!(storage.load().unwrap().is_some());
    }
}
