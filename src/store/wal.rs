//! Write-ahead log (WAL) for crash recovery.
//!
//! ## Design: state-based, redo-only
//!
//! Records carry the **post-mutation CRDT state** of a key (plus the HLC
//! that stamped the change), never the operation itself. Replay is a plain
//! CRDT merge + max-monotone metadata update, so it is idempotent and
//! commutative: replaying any prefix twice — or replaying records already
//! contained in a snapshot ("over-replay") — is a no-op. This is what makes
//! an undo phase unnecessary and lets snapshots omit an LSN watermark
//! entirely (recovery is always "load snapshot, then replay every retained
//! segment in order"). Recording operations instead would break this:
//! replaying `counter_inc` twice double-counts.
//!
//! ## On-disk format
//!
//! Segment files are named `wal-<seq:016x>.log` (`seq` strictly increasing)
//! and live under a per-store directory (`<data>/wal/eventual/`,
//! `<data>/wal/certified/`). Each segment starts with a 16-byte header:
//!
//! ```text
//! offset  size  content
//! 0       8     magic  = b"ADBWAL\x00\x01"
//! 8       4     wal_format_version: u32 LE = 1
//! 12      4     reserved: u32 LE = 0
//! ```
//!
//! followed by length-prefixed, CRC-protected record frames:
//!
//! ```text
//! [len: u32 LE][crc32: u32 LE][payload: len bytes]
//! ```
//!
//! where `payload` is the bincode encoding of a [`WalRecord`]. A new
//! segment is always started on open and on rotation — sealed segments are
//! never appended to again, so a torn tail can only exist in the segment
//! with the highest sequence number.
//!
//! ## Corruption policy (torn tail vs mid-log corruption)
//!
//! * **Torn tail** (harmless): an incomplete or zero-filled frame at the
//!   very end of the *last* segment. This is the expected shape of a crash
//!   mid-append; only un-synced (hence un-acked) writes are lost. Recovery
//!   logs a warning and stops there.
//! * **Mid-log corruption** (fail-stop): any invalid frame that is followed
//!   by more data, or any invalid frame in a non-final segment. This means
//!   acked data may be damaged; silently truncating would lose it, so the
//!   caller must fail-stop (see `ASTEROIDB_WAL_RECOVER_TRUNCATE` for the
//!   explicit operator escape hatch).

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::hlc::HlcTimestamp;
use crate::store::backend::fsync_dir;
use crate::store::kv::{CrdtValue, Store};

/// Segment file magic (8 bytes).
pub const WAL_MAGIC: [u8; 8] = *b"ADBWAL\x00\x01";

/// Current WAL segment format version.
///
/// MAINTAINER WARNING: the record payload is bincode — positional and
/// non-self-describing. Any change to [`WalRecord`] variants or fields
/// requires bumping this version and adding a versioned decode arm in
/// [`read_all_segments`] (the snapshot format's `StoreV2Layout` is the
/// pattern to follow).
pub const WAL_FORMAT_VERSION: u32 = 1;

/// Size of the segment header in bytes.
const SEGMENT_HEADER_LEN: usize = 16;

/// Size of a record frame header (`len` + `crc`) in bytes.
const FRAME_HEADER_LEN: usize = 8;

/// Upper bound for a single record payload. Anything larger is rejected on
/// append and treated as corruption on read.
pub const MAX_RECORD_LEN: u32 = 64 * 1024 * 1024;

/// A single logical WAL record.
///
/// All variants replay through idempotent CRDT merges and max-monotone
/// metadata updates ([`replay_record`]), so duplicated or over-replayed
/// records are harmless.
///
/// MAINTAINER WARNING: bincode is positional and non-self-describing —
/// changing variants or fields requires a [`WAL_FORMAT_VERSION`] bump plus
/// a versioned decode arm for the old layout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecord {
    /// A locally-originated mutation or a push-path merge (`merge_remote`),
    /// both re-stamped with a local HLC. Replays with an applied claim.
    UpsertApplied {
        key: String,
        value: CrdtValue,
        hlc: HlcTimestamp,
    },
    /// A delta-pull merge (`merge_remote_with_hlc`) that preserves the
    /// origin HLC and makes NO applied claim (see the session-guarantee
    /// soundness argument on `EventualApi::merge_remote_with_hlc`).
    UpsertVisible {
        key: String,
        value: CrdtValue,
        hlc: HlcTimestamp,
    },
    /// Keys poisoned by a failed remote merge (type mismatch). Losing the
    /// poison while keeping the frontier would produce false session
    /// successes after restart, so the poison is logged on its own.
    MergeFailed { keys: Vec<String> },
    /// Adoption of a sender's session metadata (delta / full sync) as ONE
    /// atomic record: persisting the applied frontier without the matching
    /// poison set would be unsound, so they can never be separated by a
    /// torn tail.
    SessionClaims {
        applied: HashMap<String, HlcTimestamp>,
        visible: HashMap<String, HlcTimestamp>,
        failed: Vec<String>,
    },
}

/// When to fdatasync the WAL relative to acknowledging writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Group-commit fdatasync before every write acknowledgement
    /// (default). Acked writes survive process/OS crash and power loss.
    Always,
    /// Background fdatasync on a fixed interval. A process crash alone
    /// loses nothing (page cache survives); an OS crash / power loss can
    /// lose up to one interval of acked writes locally.
    Interval(Duration),
    /// No explicit fsync. Process crash loses nothing; OS crash durability
    /// depends on kernel writeback. Development / benchmarking only.
    Off,
}

/// Configuration for a [`WalWriter`].
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory holding the segment files for one store.
    pub dir: PathBuf,
    /// Fsync policy (see [`SyncPolicy`]).
    pub sync: SyncPolicy,
    /// Rotate to a new segment once the active one exceeds this size.
    pub segment_max_bytes: u64,
}

impl WalConfig {
    /// Default segment rotation threshold (64 MiB).
    pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;

    /// Create a config with the default segment size.
    pub fn new(dir: impl Into<PathBuf>, sync: SyncPolicy) -> Self {
        Self {
            dir: dir.into(),
            sync,
            segment_max_bytes: Self::DEFAULT_SEGMENT_MAX_BYTES,
        }
    }
}

/// Monotone position of an appended record (1-based append counter).
///
/// `WalPos(n)` is durable once the syncer's durable watermark reaches `n`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WalPos(pub u64);

/// Mutable file state, guarded by `WalShared::state`.
struct FdState {
    file: File,
    seg_seq: u64,
    seg_bytes: u64,
    seg_records: u64,
    /// Set when a `write_all` failed part-way (e.g. ENOSPC): the file may
    /// end in a partial frame. The next append (or rotation) truncates
    /// back to `seg_bytes` — the last known-good record boundary — before
    /// touching the file again. Without this, a later successful append
    /// would land AFTER the garbage and turn a harmless torn write into
    /// fail-stop mid-log corruption at the next recovery.
    tainted: bool,
}

impl FdState {
    /// Truncate away a possibly-partial frame left by a failed append.
    fn repair_if_tainted(&mut self) -> io::Result<()> {
        if self.tainted {
            self.file.set_len(self.seg_bytes)?;
            self.tainted = false;
        }
        Ok(())
    }
}

/// State shared between the writer (append path, under the API mutex) and
/// the syncer (group-commit fdatasync, outside the API mutex).
struct WalShared {
    state: Mutex<FdState>,
    /// Count of records appended (written to the file) so far.
    appended: AtomicU64,
    /// Highest append count known to be durable (fdatasynced or on a
    /// sealed-and-synced segment).
    durable: AtomicU64,
    /// Wakes the group-commit flusher after an append.
    #[cfg(feature = "native-runtime")]
    wake: tokio::sync::Notify,
    /// Broadcasts durable-watermark advances to `wait_durable` callers.
    #[cfg(feature = "native-runtime")]
    durable_tx: tokio::sync::watch::Sender<u64>,
}

impl WalShared {
    /// Advance the durable watermark to at least `to` and notify waiters.
    fn advance_durable(&self, to: u64) {
        self.durable.fetch_max(to, Ordering::AcqRel);
        #[cfg(feature = "native-runtime")]
        self.durable_tx.send_if_modified(|v| {
            if to > *v {
                *v = to;
                true
            } else {
                false
            }
        });
    }
}

/// Appender for a single WAL directory.
///
/// Owned by the API object whose mutations it logs; the surrounding
/// `Arc<Mutex<Api>>` is the append serialization point (no extra lock is
/// taken on the hot path beyond the short internal fd mutex).
pub struct WalWriter {
    shared: Arc<WalShared>,
    cfg: WalConfig,
}

impl WalWriter {
    /// Open a WAL directory for appending.
    ///
    /// Always starts a NEW segment (max existing sequence + 1); sealed
    /// segments are never appended to, so recovery never needs to repair
    /// or truncate an old file before reuse.
    pub fn open(cfg: WalConfig) -> io::Result<Self> {
        fs::create_dir_all(&cfg.dir)?;
        let next_seq = list_segments(&cfg.dir)?
            .last()
            .map(|(seq, _)| seq + 1)
            .unwrap_or(1);
        let file = create_segment(&cfg.dir, next_seq)?;
        Ok(Self {
            shared: Arc::new(WalShared {
                state: Mutex::new(FdState {
                    file,
                    seg_seq: next_seq,
                    seg_bytes: SEGMENT_HEADER_LEN as u64,
                    seg_records: 0,
                    tainted: false,
                }),
                appended: AtomicU64::new(0),
                durable: AtomicU64::new(0),
                #[cfg(feature = "native-runtime")]
                wake: tokio::sync::Notify::new(),
                #[cfg(feature = "native-runtime")]
                durable_tx: tokio::sync::watch::channel(0).0,
            }),
            cfg,
        })
    }

    /// The configured sync policy.
    pub fn sync_policy(&self) -> SyncPolicy {
        self.cfg.sync
    }

    /// Append a record frame (`write` syscall; durability is the syncer's
    /// job). Returns the record's monotone position for `wait_durable`.
    ///
    /// Auto-rotates when the active segment exceeds the configured size.
    pub fn append(&mut self, record: &WalRecord) -> io::Result<WalPos> {
        let payload = bincode::serde::encode_to_vec(record, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if payload.len() as u64 > MAX_RECORD_LEN as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("WAL record too large: {} bytes", payload.len()),
            ));
        }
        let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        frame.extend_from_slice(&payload);

        let pos = {
            let mut state = self.shared.state.lock().unwrap();
            state.repair_if_tainted()?;
            if state.seg_records > 0
                && state.seg_bytes + frame.len() as u64 > self.cfg.segment_max_bytes
            {
                self.rotate_locked(&mut state)?;
            }
            if let Err(e) = state.file.write_all(&frame) {
                // The frame may be partially on disk; repair before the
                // next append (see `FdState::tainted`).
                state.tainted = true;
                return Err(e);
            }
            state.seg_bytes += frame.len() as u64;
            state.seg_records += 1;
            self.shared.appended.fetch_add(1, Ordering::AcqRel) + 1
        };
        #[cfg(feature = "native-runtime")]
        self.shared.wake.notify_one();
        Ok(WalPos(pos))
    }

    /// Seal the active segment (fsync it) and start a new one.
    ///
    /// Returns the sealed segment's sequence number: after this call every
    /// record appended so far lives in a segment with `seq <= sealed` and
    /// is durable, so a snapshot of the current in-memory state permits
    /// deleting segments up to `sealed` once the snapshot itself is safely
    /// on disk.
    pub fn rotate(&mut self) -> io::Result<u64> {
        let mut state = self.shared.state.lock().unwrap();
        self.rotate_locked(&mut state)
    }

    fn rotate_locked(&self, state: &mut FdState) -> io::Result<u64> {
        // Seal: everything appended so far is on this (or an earlier,
        // already-sealed) segment. sync_all also covers file-length
        // metadata, which fdatasync alone might not. A sealed segment must
        // never end in a partial frame (non-final torn tails read as
        // corruption), so repair first.
        state.repair_if_tainted()?;
        state.file.sync_all()?;
        let sealed = state.seg_seq;
        let next = sealed + 1;
        let file = create_segment(&self.cfg.dir, next)?;
        state.file = file;
        state.seg_seq = next;
        state.seg_bytes = SEGMENT_HEADER_LEN as u64;
        state.seg_records = 0;
        state.tainted = false;
        // The sealed segment is synced, so every append so far is durable.
        // Advancing here keeps `wait_durable` correct across rotation.
        self.shared
            .advance_durable(self.shared.appended.load(Ordering::Acquire));
        Ok(sealed)
    }

    /// Test hook: simulate a failed append that left `garbage` bytes of a
    /// partial frame on disk (what a mid-`write_all` ENOSPC/crash leaves).
    #[cfg(test)]
    fn simulate_torn_append(&mut self, garbage: &[u8]) {
        let mut state = self.shared.state.lock().unwrap();
        state.file.write_all(garbage).unwrap();
        state.tainted = true;
    }

    /// Build the group-commit syncer companion for this writer.
    #[cfg(feature = "native-runtime")]
    pub fn syncer(&self) -> WalSyncer {
        WalSyncer {
            shared: Arc::clone(&self.shared),
            policy: self.cfg.sync,
        }
    }
}

/// Group-commit fdatasync companion of a [`WalWriter`].
///
/// Runs the fdatasync OUTSIDE the API mutex (`spawn_blocking`), so an
/// `Always` policy never serializes every HTTP handler behind a disk
/// flush: while one fdatasync is in flight, further appends accumulate and
/// are covered by the next flush (group commit).
#[cfg(feature = "native-runtime")]
pub struct WalSyncer {
    shared: Arc<WalShared>,
    policy: SyncPolicy,
}

#[cfg(feature = "native-runtime")]
impl WalSyncer {
    /// The policy this syncer runs under.
    pub fn policy(&self) -> SyncPolicy {
        self.policy
    }

    /// Wait until the record at `pos` is durable.
    ///
    /// Errors only if the flusher task is gone (which, given the fail-stop
    /// policy on fsync errors, should be unreachable in practice).
    pub async fn wait_durable(&self, pos: WalPos) -> io::Result<()> {
        if self.shared.durable.load(Ordering::Acquire) >= pos.0 {
            return Ok(());
        }
        let mut rx = self.shared.durable_tx.subscribe();
        loop {
            if *rx.borrow() >= pos.0 {
                return Ok(());
            }
            rx.changed()
                .await
                .map_err(|_| io::Error::other("WAL syncer stopped"))?;
        }
    }

    /// Run the flusher loop. Spawn with `tokio::spawn`.
    ///
    /// * `Always` — woken by every append; one fdatasync covers all appends
    ///   accumulated while the previous one was in flight (group commit).
    /// * `Interval(d)` — ticks every `d`, syncing only when appends are
    ///   pending.
    /// * `Off` — returns immediately (no explicit fsync ever).
    ///
    /// An fdatasync failure aborts the process: after a failed fsync the
    /// page cache state is undefined (fsyncgate), so retrying would report
    /// durability that does not exist.
    pub async fn run_flusher(self: Arc<Self>) {
        match self.policy {
            SyncPolicy::Off => {}
            SyncPolicy::Always => loop {
                self.shared.wake.notified().await;
                self.sync_once().await;
            },
            SyncPolicy::Interval(d) => {
                let mut ticker = tokio::time::interval(d);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    self.sync_once().await;
                }
            }
        }
    }

    /// One group-commit flush: snapshot the append watermark, fdatasync
    /// the active file on the blocking pool, then advance `durable`.
    async fn sync_once(&self) {
        let (fd, target) = {
            let state = self.shared.state.lock().unwrap();
            let target = self.shared.appended.load(Ordering::Acquire);
            let fd = match state.file.try_clone() {
                Ok(fd) => fd,
                Err(e) => wal_sync_fail(&e),
            };
            (fd, target)
        };
        if target <= self.shared.durable.load(Ordering::Acquire) {
            return;
        }
        // The cloned fd shares the open file description, so this syncs
        // every append made to the active segment up to (at least) the
        // snapshot point. Appends on segments sealed since the snapshot
        // were already synced by rotation.
        match tokio::task::spawn_blocking(move || fd.sync_data()).await {
            Ok(Ok(())) => self.shared.advance_durable(target),
            Ok(Err(e)) => wal_sync_fail(&e),
            Err(e) => wal_sync_fail(&io::Error::other(e)),
        }
    }
}

/// Human-readable fail-stop justification for an fsync failure.
///
/// Split from [`wal_sync_fail`] so the message (the part that matters for
/// operators) is unit-testable; the abort itself is not.
pub(crate) fn wal_sync_failure_message(err: &io::Error) -> String {
    format!(
        "WAL fdatasync failed ({err}); aborting: after a failed fsync the page cache state is \
         undefined, so retrying would acknowledge durability that does not exist. Check disk \
         health/space, then restart — recovery replays the WAL up to the last valid record."
    )
}

/// Fail-stop on fsync failure (fsyncgate).
#[cfg(feature = "native-runtime")]
fn wal_sync_fail(err: &io::Error) -> ! {
    tracing::error!("{}", wal_sync_failure_message(err));
    std::process::abort();
}

// ---------------------------------------------------------------------------
// Segment files
// ---------------------------------------------------------------------------

/// Segment file name for a sequence number.
fn segment_file_name(seq: u64) -> String {
    format!("wal-{seq:016x}.log")
}

/// Create segment `seq` in `dir`: header, file fsync, then directory fsync
/// (so the new file's existence is itself durable).
fn create_segment(dir: &Path, seq: u64) -> io::Result<File> {
    let path = dir.join(segment_file_name(seq));
    let mut file = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(&path)?;
    let mut header = [0u8; SEGMENT_HEADER_LEN];
    header[..8].copy_from_slice(&WAL_MAGIC);
    header[8..12].copy_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    // bytes 12..16: reserved, zero.
    file.write_all(&header)?;
    file.sync_all()?;
    fsync_dir(dir)?;
    Ok(file)
}

/// List `(seq, path)` for every segment in `dir`, sorted ascending by seq.
///
/// Returns an empty list when the directory does not exist.
pub fn list_segments(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut segments = Vec::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(seq_hex) = name
            .strip_prefix("wal-")
            .and_then(|rest| rest.strip_suffix(".log"))
        else {
            continue;
        };
        if let Ok(seq) = u64::from_str_radix(seq_hex, 16) {
            segments.push((seq, entry.path()));
        }
    }
    segments.sort_unstable_by_key(|(seq, _)| *seq);
    Ok(segments)
}

/// Delete every segment with `seq <= sealed_seq`, then fsync the directory.
///
/// The active segment always has a strictly greater sequence number than
/// any value returned by [`WalWriter::rotate`], so it can never be deleted
/// through this function. Deletion is the LAST step of a checkpoint: a
/// crash before it merely leaves harmless over-replay work behind.
pub fn remove_segments_up_to(dir: &Path, sealed_seq: u64) -> io::Result<()> {
    let mut removed = false;
    for (seq, path) in list_segments(dir)? {
        if seq <= sealed_seq {
            fs::remove_file(&path)?;
            removed = true;
        }
    }
    if removed {
        fsync_dir(dir)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Reading / recovery
// ---------------------------------------------------------------------------

/// Terminal condition of a WAL read (see the module-level corruption policy).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalReadOutcome {
    /// Every retained record parsed cleanly.
    Clean,
    /// The final segment ends in an incomplete/zeroed frame — the expected
    /// shape of a crash mid-append. Harmless: only un-synced (un-acked)
    /// writes are lost.
    TornTail,
    /// An invalid frame that is NOT a torn tail: acked data may be damaged.
    /// The caller must fail-stop unless the operator explicitly opted into
    /// truncation.
    Corruption,
}

/// Result of reading a WAL directory.
#[derive(Debug)]
pub struct WalReadResult {
    /// Records parsed, in append order, up to the first invalid frame.
    pub records: Vec<WalRecord>,
    /// Why parsing stopped.
    pub outcome: WalReadOutcome,
    /// Number of segment files visited.
    pub segments: usize,
    /// Where parsing stopped, for non-[`Clean`](WalReadOutcome::Clean)
    /// outcomes: the segment holding the first invalid frame and the byte
    /// length of its valid prefix. `None` when everything parsed cleanly.
    pub stop: Option<WalStopPoint>,
}

/// The point at which a WAL read stopped on an invalid frame
/// (input to [`truncate_to_valid_prefix`]).
#[derive(Debug, Clone)]
pub struct WalStopPoint {
    /// Sequence number of the segment holding the invalid frame.
    pub seq: u64,
    /// Path of that segment file.
    pub path: PathBuf,
    /// Length in bytes of the segment's valid prefix (header plus every
    /// frame before the invalid one). Zero when the header itself is
    /// invalid or incomplete.
    pub valid_len: u64,
}

/// Read and validate every segment in `dir` in ascending sequence order.
///
/// Parsing stops at the first invalid frame; `outcome` distinguishes a
/// harmless torn tail from mid-log corruption. Records preceding the stop
/// point are always returned (they are what a truncating recovery keeps).
pub fn read_all_segments(dir: &Path) -> io::Result<WalReadResult> {
    let segments = list_segments(dir)?;
    let mut records = Vec::new();
    let total = segments.len();
    for (idx, (seq, path)) in segments.iter().enumerate() {
        let data = fs::read(path)?;
        let is_last = idx + 1 == total;
        let (outcome, valid_len) = parse_segment(&data, is_last, &mut records);
        if outcome != WalReadOutcome::Clean {
            return Ok(WalReadResult {
                records,
                outcome,
                segments: total,
                stop: Some(WalStopPoint {
                    seq: *seq,
                    path: path.clone(),
                    valid_len,
                }),
            });
        }
    }
    Ok(WalReadResult {
        records,
        outcome: WalReadOutcome::Clean,
        segments: total,
        stop: None,
    })
}

/// Physically truncate the log to the valid prefix a read stopped at: cut
/// the offending segment back to its last valid frame boundary (removing
/// the file entirely when even its header is invalid) and delete every
/// later segment, then fsync the lot.
///
/// Recovery MUST call this before opening a new writer whenever it decides
/// to continue past a non-`Clean` read (a [`WalReadOutcome::TornTail`], or
/// a [`WalReadOutcome::Corruption`] under the explicit operator truncate
/// escape hatch). [`WalWriter::open`] always starts a NEW, higher-numbered
/// segment, so an invalid frame left in place would sit in a *non-final*
/// segment on the next boot and read as fail-stop mid-log corruption —
/// making every acked record appended after this recovery unreachable
/// behind it (and silently lost under the truncate flag).
///
/// Deleting the segments after the stop point matches the read semantics:
/// their records were never parsed, so they are exactly what the caller
/// already decided to drop by continuing past the invalid frame.
pub fn truncate_to_valid_prefix(dir: &Path, read: &WalReadResult) -> io::Result<()> {
    let Some(stop) = &read.stop else {
        return Ok(());
    };
    for (seq, path) in list_segments(dir)? {
        if seq > stop.seq {
            fs::remove_file(&path)?;
        }
    }
    if stop.valid_len < SEGMENT_HEADER_LEN as u64 {
        // Not even a valid header survives: drop the whole file (a
        // truncated-to-zero or header-only-invalid segment would still
        // read as torn/corrupt).
        fs::remove_file(&stop.path)?;
    } else {
        let file = OpenOptions::new().write(true).open(&stop.path)?;
        file.set_len(stop.valid_len)?;
        file.sync_all()?;
    }
    fsync_dir(dir)?;
    Ok(())
}

/// Parse one segment, appending parsed records to `out`.
///
/// Returns the outcome plus the byte length of the segment's valid prefix
/// (header + every frame parsed before the stop point; zero when the
/// header itself is invalid or incomplete) — the truncation boundary for
/// [`truncate_to_valid_prefix`].
fn parse_segment(data: &[u8], is_last: bool, out: &mut Vec<WalRecord>) -> (WalReadOutcome, u64) {
    // Header validation. A crash during segment creation can leave a short
    // (or empty) newest segment — that is a torn tail, not corruption.
    if data.len() < SEGMENT_HEADER_LEN {
        let outcome = if is_last {
            WalReadOutcome::TornTail
        } else {
            WalReadOutcome::Corruption
        };
        return (outcome, 0);
    }
    if data[..8] != WAL_MAGIC {
        return (WalReadOutcome::Corruption, 0);
    }
    let version = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
    if version != WAL_FORMAT_VERSION {
        // Unknown version: refuse to guess at the layout.
        return (WalReadOutcome::Corruption, 0);
    }

    // A torn frame at the end of the LAST segment is a torn tail;
    // anywhere else the same shape is corruption (sealed segments are
    // never appended to, so they cannot legitimately end mid-frame).
    let torn = if is_last {
        WalReadOutcome::TornTail
    } else {
        WalReadOutcome::Corruption
    };

    let mut off = SEGMENT_HEADER_LEN;
    loop {
        let rem = data.len() - off;
        if rem == 0 {
            return (WalReadOutcome::Clean, off as u64);
        }
        if rem < FRAME_HEADER_LEN {
            return (torn, off as u64);
        }
        let len = u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
        let crc = u32::from_le_bytes([data[off + 4], data[off + 5], data[off + 6], data[off + 7]]);
        // A zero length can only come from a zero-filled (preallocated /
        // torn) tail or corruption — records are never empty. Checked
        // before the CRC because crc32(empty) == 0 would "validate".
        if len == 0 || len > MAX_RECORD_LEN {
            if is_last && data[off..].iter().all(|&b| b == 0) {
                return (WalReadOutcome::TornTail, off as u64);
            }
            return (WalReadOutcome::Corruption, off as u64);
        }
        let len = len as usize;
        if len > rem - FRAME_HEADER_LEN {
            // Frame extends past EOF: the classic partially-appended
            // record. Torn tail only at the end of the last segment.
            return (torn, off as u64);
        }
        let payload = &data[off + FRAME_HEADER_LEN..off + FRAME_HEADER_LEN + len];
        if crc32fast::hash(payload) != crc {
            // A CRC mismatch on the FINAL frame of the last segment is a
            // torn write (partial payload flush); a mismatch with valid
            // data after it means acked records were damaged in place.
            if is_last && off + FRAME_HEADER_LEN + len == data.len() {
                return (WalReadOutcome::TornTail, off as u64);
            }
            return (WalReadOutcome::Corruption, off as u64);
        }
        match bincode::serde::decode_from_slice::<WalRecord, _>(
            payload,
            bincode::config::standard(),
        ) {
            Ok((record, _)) => out.push(record),
            // Valid CRC but undecodable payload: a format-level problem,
            // never a torn write. Fail-stop material.
            Err(_) => return (WalReadOutcome::Corruption, off as u64),
        }
        off += FRAME_HEADER_LEN + len;
    }
}

/// Apply one WAL record to a store (redo).
///
/// Every arm is idempotent / max-monotone, so replaying a record that the
/// snapshot (or an earlier replay) already covers is a no-op. A type
/// mismatch during replay poisons the key exactly like the live merge path
/// would, then continues — consistent with the `MergeFailed` record the
/// live path wrote alongside the failure.
pub fn replay_record(store: &mut Store, record: WalRecord) {
    match record {
        WalRecord::UpsertApplied { key, value, hlc } => {
            if let Err(e) = store.merge_value(key.clone(), &value) {
                tracing::warn!(key = %key, error = %e, "WAL replay merge failed; poisoning key");
                store.note_merge_failed(&key);
            }
            store.record_change_max(&key, hlc.clone());
            store.note_applied(&hlc);
        }
        WalRecord::UpsertVisible { key, value, hlc } => {
            if let Err(e) = store.merge_value(key.clone(), &value) {
                tracing::warn!(key = %key, error = %e, "WAL replay merge failed; poisoning key");
                store.note_merge_failed(&key);
            }
            store.record_change_max(&key, hlc.clone());
            store.note_visible(&hlc);
        }
        WalRecord::MergeFailed { keys } => {
            for key in &keys {
                store.note_merge_failed(key);
            }
        }
        WalRecord::SessionClaims {
            applied,
            visible,
            failed,
        } => {
            store.merge_applied_origins(&applied);
            store.merge_visible_origins(&visible);
            store.merge_failed_extend(failed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::pn_counter::PnCounter;
    use crate::types::NodeId;

    fn ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    fn counter(n: u64, node: &str) -> CrdtValue {
        let mut c = PnCounter::new();
        for _ in 0..n {
            c.increment(&NodeId(node.into()));
        }
        CrdtValue::Counter(c)
    }

    fn upsert(key: &str, n: u64, physical: u64) -> WalRecord {
        WalRecord::UpsertApplied {
            key: key.into(),
            value: counter(n, "node-a"),
            hlc: ts(physical, 0, "node-a"),
        }
    }

    fn cfg(dir: &Path) -> WalConfig {
        WalConfig::new(dir, SyncPolicy::Off)
    }

    fn encode(record: &WalRecord) -> Vec<u8> {
        bincode::serde::encode_to_vec(record, bincode::config::standard()).unwrap()
    }

    // ---------------------------------------------------------------
    // Round trip / framing
    // ---------------------------------------------------------------

    #[test]
    fn append_and_read_round_trip_all_variants() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::open(cfg(dir.path())).unwrap();

        let mut applied = HashMap::new();
        applied.insert("node-b".to_string(), ts(50, 1, "node-b"));
        let mut visible = HashMap::new();
        visible.insert("node-c".to_string(), ts(60, 2, "node-c"));

        let records = vec![
            upsert("k1", 3, 100),
            WalRecord::UpsertVisible {
                key: "k2".into(),
                value: counter(1, "node-b"),
                hlc: ts(200, 5, "node-b"),
            },
            WalRecord::MergeFailed {
                keys: vec!["bad".into()],
            },
            WalRecord::SessionClaims {
                applied,
                visible,
                failed: vec!["bad2".into()],
            },
        ];
        let mut last_pos = WalPos(0);
        for r in &records {
            let pos = wal.append(r).unwrap();
            assert!(pos > last_pos, "positions must be strictly increasing");
            last_pos = pos;
        }
        drop(wal);

        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Clean);
        assert_eq!(read.records.len(), records.len());
        for (got, want) in read.records.iter().zip(&records) {
            assert_eq!(encode(got), encode(want), "record must round-trip exactly");
        }
    }

    #[test]
    fn open_always_starts_a_new_segment() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut wal = WalWriter::open(cfg(dir.path())).unwrap();
            wal.append(&upsert("k", 1, 1)).unwrap();
        }
        {
            let mut wal = WalWriter::open(cfg(dir.path())).unwrap();
            wal.append(&upsert("k", 1, 2)).unwrap();
        }
        let segments = list_segments(dir.path()).unwrap();
        assert_eq!(
            segments.len(),
            2,
            "each open must claim a fresh segment (never append to a sealed one)"
        );
        assert!(segments[0].0 < segments[1].0);
        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Clean);
        assert_eq!(read.records.len(), 2);
    }

    #[test]
    fn segment_rotation_by_size_and_multi_segment_replay_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = cfg(dir.path());
        config.segment_max_bytes = 256; // force frequent rotation
        let mut wal = WalWriter::open(config).unwrap();
        for i in 0..20u64 {
            wal.append(&upsert(&format!("k{i}"), 1, 100 + i)).unwrap();
        }
        drop(wal);

        let segments = list_segments(dir.path()).unwrap();
        assert!(segments.len() > 1, "size threshold must trigger rotation");

        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Clean);
        assert_eq!(read.records.len(), 20);
        // Ascending append order across segments.
        for (i, rec) in read.records.iter().enumerate() {
            match rec {
                WalRecord::UpsertApplied { key, .. } => assert_eq!(key, &format!("k{i}")),
                other => panic!("unexpected record {other:?}"),
            }
        }
    }

    #[test]
    fn rotate_then_remove_segments_keeps_active_and_later() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::open(cfg(dir.path())).unwrap();
        wal.append(&upsert("old", 1, 1)).unwrap();
        let sealed = wal.rotate().unwrap();
        wal.append(&upsert("new", 1, 2)).unwrap();

        remove_segments_up_to(dir.path(), sealed).unwrap();

        let segments = list_segments(dir.path()).unwrap();
        assert_eq!(segments.len(), 1, "only the active segment must remain");
        assert!(segments[0].0 > sealed);
        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.records.len(), 1);
        match &read.records[0] {
            WalRecord::UpsertApplied { key, .. } => assert_eq!(key, "new"),
            other => panic!("unexpected record {other:?}"),
        }
    }

    // ---------------------------------------------------------------
    // Torn tail vs corruption
    // ---------------------------------------------------------------

    /// Write `n` records into a fresh single-segment WAL and return the
    /// segment path.
    fn write_segment(dir: &Path, n: u64) -> PathBuf {
        let mut wal = WalWriter::open(cfg(dir)).unwrap();
        for i in 0..n {
            wal.append(&upsert(&format!("k{i}"), i + 1, 100 + i))
                .unwrap();
        }
        drop(wal);
        list_segments(dir).unwrap().pop().unwrap().1
    }

    #[test]
    fn truncation_at_any_byte_yields_a_record_prefix() {
        // Ferrite: ext4 does not even guarantee prefix-append, so a crash
        // can leave the file cut at ANY byte. Whatever survives must parse
        // as a strict prefix of what was written — never garbage records.
        let dir = tempfile::tempdir().unwrap();
        let path = write_segment(dir.path(), 5);
        let full = fs::read(&path).unwrap();

        for cut in 0..full.len() {
            fs::write(&path, &full[..cut]).unwrap();
            let read = read_all_segments(dir.path()).unwrap();
            assert!(
                read.records.len() <= 5,
                "cut at {cut}: more records than written"
            );
            for (i, rec) in read.records.iter().enumerate() {
                match rec {
                    WalRecord::UpsertApplied { key, .. } => assert_eq!(
                        key,
                        &format!("k{i}"),
                        "cut at {cut}: records must be a prefix"
                    ),
                    other => panic!("cut at {cut}: unexpected record {other:?}"),
                }
            }
            assert_ne!(
                read.outcome,
                WalReadOutcome::Corruption,
                "cut at {cut}: a pure truncation is a torn tail, never corruption"
            );
        }
    }

    #[test]
    fn zero_filled_tail_is_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_segment(dir.path(), 3);
        let mut data = fs::read(&path).unwrap();
        data.extend_from_slice(&[0u8; 64]);
        fs::write(&path, &data).unwrap();

        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::TornTail);
        assert_eq!(read.records.len(), 3, "all complete records must survive");
    }

    #[test]
    fn crc_flip_on_final_record_is_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_segment(dir.path(), 3);
        let mut data = fs::read(&path).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF; // damage the last payload byte
        fs::write(&path, &data).unwrap();

        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::TornTail);
        assert_eq!(read.records.len(), 2);
    }

    #[test]
    fn mid_log_bit_flip_is_corruption_not_silent_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_segment(dir.path(), 3);
        let mut data = fs::read(&path).unwrap();
        // Damage a byte inside the FIRST record's payload (valid records follow).
        data[SEGMENT_HEADER_LEN + FRAME_HEADER_LEN + 2] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(
            read.outcome,
            WalReadOutcome::Corruption,
            "an invalid frame followed by valid data means acked records were damaged"
        );
        assert!(read.records.is_empty());
    }

    #[test]
    fn torn_tail_in_non_final_segment_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::open(cfg(dir.path())).unwrap();
        wal.append(&upsert("a", 1, 1)).unwrap();
        wal.rotate().unwrap();
        wal.append(&upsert("b", 1, 2)).unwrap();
        drop(wal);

        // Truncate the FIRST (sealed) segment mid-record.
        let segments = list_segments(dir.path()).unwrap();
        let first = &segments[0].1;
        let data = fs::read(first).unwrap();
        fs::write(first, &data[..data.len() - 3]).unwrap();

        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(
            read.outcome,
            WalReadOutcome::Corruption,
            "a sealed segment can never legitimately have a torn tail"
        );
    }

    #[test]
    fn oversized_length_field_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_segment(dir.path(), 1);
        let mut data = fs::read(&path).unwrap();
        // Append a frame header claiming a huge record, plus garbage.
        data.extend_from_slice(&(MAX_RECORD_LEN + 1).to_le_bytes());
        data.extend_from_slice(&[0xAB; 12]);
        fs::write(&path, &data).unwrap();

        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Corruption);
        assert_eq!(read.records.len(), 1, "the valid record before it survives");
    }

    #[test]
    fn bad_magic_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_segment(dir.path(), 1);
        let mut data = fs::read(&path).unwrap();
        data[0] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Corruption);
    }

    #[test]
    fn unknown_format_version_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_segment(dir.path(), 1);
        let mut data = fs::read(&path).unwrap();
        data[8..12].copy_from_slice(&99u32.to_le_bytes());
        fs::write(&path, &data).unwrap();

        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Corruption);
    }

    #[test]
    fn truncate_to_valid_prefix_cuts_a_torn_tail_at_the_record_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_segment(dir.path(), 3);
        let mut data = fs::read(&path).unwrap();
        data.extend_from_slice(&[0xAB; 5]); // partial frame from a torn append
        fs::write(&path, &data).unwrap();

        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::TornTail);
        truncate_to_valid_prefix(dir.path(), &read).unwrap();

        // The repaired segment ends exactly at the last valid frame: it now
        // parses Clean even as a NON-final segment (the next boot's writer
        // will have opened a higher one).
        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Clean);
        assert_eq!(read.records.len(), 3, "all complete records survive");
    }

    #[test]
    fn truncate_to_valid_prefix_removes_segments_after_the_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::open(cfg(dir.path())).unwrap();
        wal.append(&upsert("a", 1, 1)).unwrap();
        wal.rotate().unwrap();
        wal.append(&upsert("b", 1, 2)).unwrap();
        drop(wal);

        // Corrupt the FIRST segment's record (mid-log corruption; a later
        // segment with valid data follows).
        let segments = list_segments(dir.path()).unwrap();
        let first = segments[0].1.clone();
        let mut data = fs::read(&first).unwrap();
        data[SEGMENT_HEADER_LEN + FRAME_HEADER_LEN + 2] ^= 0xFF;
        fs::write(&first, &data).unwrap();

        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Corruption);
        truncate_to_valid_prefix(dir.path(), &read).unwrap();

        // Only the truncated first segment remains (header-only) and the
        // directory reads Clean — nothing left to trip the next boot.
        assert_eq!(list_segments(dir.path()).unwrap().len(), 1);
        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Clean);
        assert!(read.records.is_empty());
    }

    #[test]
    fn truncate_to_valid_prefix_deletes_a_segment_with_an_invalid_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_segment(dir.path(), 1);
        let mut data = fs::read(&path).unwrap();
        data[0] ^= 0xFF; // bad magic: no valid prefix at all
        fs::write(&path, &data).unwrap();

        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Corruption);
        truncate_to_valid_prefix(dir.path(), &read).unwrap();

        assert!(
            list_segments(dir.path()).unwrap().is_empty(),
            "a segment without a valid header must be removed entirely"
        );
        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Clean);
    }

    #[test]
    fn empty_directory_reads_clean() {
        let dir = tempfile::tempdir().unwrap();
        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Clean);
        assert!(read.records.is_empty());
        // A missing directory is also fine (first boot).
        let read = read_all_segments(&dir.path().join("nope")).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Clean);
    }

    // ---------------------------------------------------------------
    // Replay idempotence
    // ---------------------------------------------------------------

    fn store_json(store: &Store) -> serde_json::Value {
        serde_json::to_value(store).unwrap()
    }

    #[test]
    fn replay_is_idempotent_under_full_duplication() {
        let records = vec![
            upsert("cnt", 1, 100),
            upsert("cnt", 2, 101), // post-state after second inc: value 2
            upsert("cnt", 3, 102),
            WalRecord::UpsertVisible {
                key: "remote".into(),
                value: counter(5, "node-b"),
                hlc: ts(90, 0, "node-b"),
            },
            WalRecord::MergeFailed {
                keys: vec!["poisoned".into()],
            },
        ];

        let mut once = Store::new();
        for r in &records {
            replay_record(&mut once, r.clone());
        }
        let mut twice = Store::new();
        for r in records.iter().chain(records.iter()) {
            replay_record(&mut twice, r.clone());
        }
        assert_eq!(
            store_json(&once),
            store_json(&twice),
            "replay(L) must equal replay(L ++ L) — the redo-only guarantee"
        );
        match once.get("cnt") {
            Some(CrdtValue::Counter(c)) => {
                assert_eq!(c.value(), 3, "post-state records must not double-count")
            }
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn over_replay_onto_snapshot_is_a_noop() {
        // Simulate: snapshot covers a prefix; recovery replays ALL records.
        let records: Vec<WalRecord> = (1..=6u64).map(|i| upsert("cnt", i, 100 + i)).collect();

        // "Snapshot" = state after the first 3 records.
        let mut snapshot = Store::new();
        for r in &records[..3] {
            replay_record(&mut snapshot, r.clone());
        }
        // Recovery: snapshot + full replay.
        let mut recovered = snapshot.clone();
        for r in &records {
            replay_record(&mut recovered, r.clone());
        }
        // Baseline: full replay from empty.
        let mut baseline = Store::new();
        for r in &records {
            replay_record(&mut baseline, r.clone());
        }
        assert_eq!(store_json(&recovered), store_json(&baseline));
    }

    #[test]
    fn replay_claims_applied_vs_visible_correctly() {
        let mut store = Store::new();
        replay_record(
            &mut store,
            WalRecord::UpsertApplied {
                key: "a".into(),
                value: counter(1, "node-a"),
                hlc: ts(100, 0, "node-a"),
            },
        );
        replay_record(
            &mut store,
            WalRecord::UpsertVisible {
                key: "b".into(),
                value: counter(1, "node-b"),
                hlc: ts(200, 0, "node-b"),
            },
        );
        assert_eq!(store.applied_origin("node-a"), Some(&ts(100, 0, "node-a")));
        assert!(
            store.applied_origin("node-b").is_none(),
            "UpsertVisible must not fabricate an applied claim"
        );
        assert_eq!(
            store.visible_origins().get("node-b"),
            Some(&ts(200, 0, "node-b"))
        );
        // visible ⊇ applied invariant.
        assert_eq!(
            store.visible_origins().get("node-a"),
            Some(&ts(100, 0, "node-a"))
        );
    }

    #[test]
    fn replay_type_mismatch_poisons_and_continues() {
        let mut store = Store::new();
        replay_record(&mut store, upsert("k", 1, 100));
        // Conflicting type for the same key (can only arise from snapshot /
        // log interleavings) must poison, not abort the replay.
        replay_record(
            &mut store,
            WalRecord::UpsertApplied {
                key: "k".into(),
                value: CrdtValue::Register(crate::crdt::lww_register::LwwRegister::new()),
                hlc: ts(200, 0, "node-b"),
            },
        );
        replay_record(&mut store, upsert("k2", 1, 300));
        assert!(store.merge_failed_contains("k"));
        assert!(
            store.contains_key("k2"),
            "replay must continue past the poison"
        );
    }

    #[test]
    fn replay_session_claims_restores_all_three_maps() {
        let mut applied = HashMap::new();
        applied.insert("node-b".to_string(), ts(70, 0, "node-b"));
        let mut visible = HashMap::new();
        visible.insert("node-c".to_string(), ts(80, 0, "node-c"));

        let mut store = Store::new();
        replay_record(
            &mut store,
            WalRecord::SessionClaims {
                applied,
                visible,
                failed: vec!["p".into()],
            },
        );
        assert_eq!(store.applied_origin("node-b"), Some(&ts(70, 0, "node-b")));
        assert_eq!(
            store.visible_origins().get("node-c"),
            Some(&ts(80, 0, "node-c"))
        );
        assert!(store.merge_failed_contains("p"));
    }

    #[test]
    fn failed_append_is_repaired_before_the_next_write() {
        // A partial frame from a failed write_all must be truncated away
        // before the next append; otherwise the later (successful) record
        // would sit behind garbage and read as mid-log CORRUPTION instead
        // of a torn tail.
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::open(cfg(dir.path())).unwrap();
        wal.append(&upsert("before", 1, 1)).unwrap();
        wal.simulate_torn_append(&[0xDE, 0xAD, 0xBE]);
        wal.append(&upsert("after", 1, 2)).unwrap();
        drop(wal);

        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Clean);
        assert_eq!(read.records.len(), 2);
    }

    #[test]
    fn rotation_repairs_a_tainted_segment_before_sealing() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::open(cfg(dir.path())).unwrap();
        wal.append(&upsert("before", 1, 1)).unwrap();
        wal.simulate_torn_append(&[0xFF; 5]);
        wal.rotate().unwrap();
        wal.append(&upsert("after", 1, 2)).unwrap();
        drop(wal);

        // The sealed segment must not end in a partial frame (a non-final
        // torn tail reads as corruption).
        let read = read_all_segments(dir.path()).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::Clean);
        assert_eq!(read.records.len(), 2);
    }

    #[test]
    fn oversized_record_append_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::open(cfg(dir.path())).unwrap();
        let huge = WalRecord::MergeFailed {
            keys: vec!["x".repeat((MAX_RECORD_LEN as usize) + 16)],
        };
        let err = wal.append(&huge).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn sync_failure_message_names_the_cause() {
        let msg = wal_sync_failure_message(&io::Error::other("boom"));
        assert!(msg.contains("boom"));
        assert!(msg.contains("aborting"));
    }
}
