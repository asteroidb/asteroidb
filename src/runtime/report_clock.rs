//! Write-ahead physical-clock floor for frontier report HLCs (M-12).
//!
//! The frontier `digest_hash` binds real store content (the M-7 root
//! digest), so the equivocation invariant "an honest authority never signs
//! two different digests for one `frontier_hlc`" is no longer guaranteed by
//! digest determinism — the store legitimately changes between ticks. The
//! invariant instead rests on the report HLC being strictly monotonic
//! ACROSS RESTARTS: if a wall-clock rollback let a restarted node re-issue
//! an HLC it already signed (with different store content), the node would
//! frame itself with a perfectly valid-looking equivocation pair.
//!
//! [`ReportClockFloor`] closes that hole with a leased, fsynced floor:
//! before any report at HLC `t` leaves the process (before it is signed or
//! self-observed), `cover(t)` guarantees a persisted lease strictly above
//! `t.physical`. On restart the runner seeds its HLC from the lease
//! (via `Hlc::seed_recovered`, which deliberately bypasses the `update`
//! skew guard — a large wall-clock rollback is exactly the case the floor
//! exists for), so the first post-restart report is strictly above every
//! report ever issued. The lease width keeps the steady-state cost at one
//! fsync per [`FLOOR_LEASE_MS`] rather than one per tick, and the induced
//! artificial skew is bounded by the lease width (well inside the
//! detector's 60s future-skew guard and the cluster skew budget).
//!
//! **Existence is evidence — two rules keep it honest.** The runner treats
//! the file's presence at boot as proof that the lease covers every report
//! this authority ever signed (that is what justifies signing content-
//! bound `sd2:` reports immediately, with no activation grace):
//!
//! 1. **A floorless boot signs nothing until the grace has fully elapsed**
//!    (`NodeRunner`'s `DIGEST_ACTIVATION_GRACE`), so this file is only
//!    ever created by the first covered post-grace report tick. A crash
//!    during the grace leaves no file behind and the next boot restarts
//!    the grace from scratch — the file can never testify to a
//!    partially-served grace.
//! 2. **The file must never be restored from a backup** (ops rule,
//!    documented in the ops guide's data-recovery table): a restored copy
//!    carries the lease persisted at BACKUP time, strictly below the
//!    reports signed between the backup and the crash, and staleness is
//!    not locally detectable — the runner would trust it and sign fresh
//!    `sd2:` reports over an HLC range peers may still retain. When a data
//!    dir is restored from backup, this file must be DELETED so the
//!    activation grace applies instead.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::hlc::HlcTimestamp;

/// Lease width in milliseconds: how far above the last covered report HLC
/// the persisted floor is placed. Bounds both the fsync rate (one per
/// lease width at a 1s report interval) and the artificial clock skew a
/// restart can introduce (`MAX_CLOCK_SKEW_MS` = 60s remains authoritative).
pub const FLOOR_LEASE_MS: u64 = 10_000;

/// On-disk layout of the floor file (`frontier_report_clock.json`).
#[derive(Serialize, Deserialize)]
struct PersistedFloor {
    version: u32,
    leased_physical_ms: u64,
}

/// Persisted write-ahead floor over frontier report HLC physical times.
///
/// Invariant (write-ahead): whenever [`cover`](Self::cover) has returned
/// `Ok` for an issued timestamp, the durably persisted lease is STRICTLY
/// greater than that timestamp's `physical`. A report whose `cover` failed
/// must not be signed or otherwise leave the process.
pub struct ReportClockFloor {
    path: PathBuf,
    leased_physical_ms: u64,
}

impl ReportClockFloor {
    /// Load the floor from `path`.
    ///
    /// Returns the floor plus `existed`: `true` only when the file was
    /// present AND parsed — the caller treats `false` (first boot, or a
    /// corrupt/lost file) as "no restart-monotonicity evidence" and holds
    /// the store-digest report format back for an activation grace.
    pub fn load(path: PathBuf) -> (Self, bool) {
        let (leased, existed) = match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<PersistedFloor>(&bytes) {
                Ok(p) => (p.leased_physical_ms, true),
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "corrupt frontier report clock floor; treating as absent"
                    );
                    (0, false)
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (0, false),
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "unreadable frontier report clock floor; treating as absent"
                );
                (0, false)
            }
        };
        (
            Self {
                path,
                leased_physical_ms: leased,
            },
            existed,
        )
    }

    /// The currently persisted lease (0 when the file was absent/corrupt).
    pub fn leased(&self) -> u64 {
        self.leased_physical_ms
    }

    /// Ensure the persisted lease strictly covers `issued`.
    ///
    /// No-op when `issued.physical` is already strictly below the lease;
    /// otherwise persists `issued.physical + FLOOR_LEASE_MS` (tmp file →
    /// fsync → rename → dir fsync via `write_atomic`) BEFORE updating the
    /// in-memory lease, so an `Ok` return proves durability. On `Err` the
    /// caller must skip the whole report tick (the issued HLC has claimed
    /// nothing yet, so discarding it is safe).
    ///
    /// ORDERING CONTRACT: the caller must issue the HLC FIRST
    /// (`Hlc::now()`) and cover it afterwards. A "check the wall clock,
    /// then issue" scheme is forbidden — the wall clock can cross the
    /// lease between the check and the issue, breaking write-ahead-ness.
    pub fn cover(&mut self, issued: &HlcTimestamp) -> std::io::Result<()> {
        if issued.physical < self.leased_physical_ms {
            return Ok(());
        }
        let new_lease = issued.physical.saturating_add(FLOOR_LEASE_MS);
        let payload = serde_json::to_vec(&PersistedFloor {
            version: 1,
            leased_physical_ms: new_lease,
        })
        .map_err(std::io::Error::other)?;
        crate::ops::write_atomic(&self.path, &payload).map_err(std::io::Error::other)?;
        self.leased_physical_ms = new_lease;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(physical: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical: 0,
            node_id: "auth-1".into(),
        }
    }

    #[test]
    fn floor_roundtrip_and_corrupt_file_treated_as_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("frontier_report_clock.json");

        // Absent file: not existed, lease 0.
        let (mut floor, existed) = ReportClockFloor::load(path.clone());
        assert!(!existed);
        assert_eq!(floor.leased(), 0);

        // Cover persists; a reload sees the lease and reports existed.
        floor.cover(&ts(5_000)).unwrap();
        let (reloaded, existed) = ReportClockFloor::load(path.clone());
        assert!(existed);
        assert_eq!(reloaded.leased(), 5_000 + FLOOR_LEASE_MS);

        // A corrupt file is absent (WARN), never a startup failure.
        std::fs::write(&path, b"{not json").unwrap();
        let (corrupt, existed) = ReportClockFloor::load(path);
        assert!(!existed);
        assert_eq!(corrupt.leased(), 0);
    }

    #[test]
    fn cover_is_noop_below_lease_and_bumps_at_or_above() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("frontier_report_clock.json");
        let (mut floor, _) = ReportClockFloor::load(path.clone());

        floor.cover(&ts(1_000)).unwrap();
        let lease = floor.leased();
        assert_eq!(lease, 1_000 + FLOOR_LEASE_MS);
        let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();

        // Strictly below the lease: no-op, no rewrite.
        floor.cover(&ts(lease - 1)).unwrap();
        assert_eq!(floor.leased(), lease);
        assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), mtime);

        // EXACTLY at the lease: must bump — the invariant is persisted
        // lease STRICTLY greater than every covered physical.
        floor.cover(&ts(lease)).unwrap();
        assert_eq!(floor.leased(), lease + FLOOR_LEASE_MS);
        assert!(floor.leased() > lease);
    }

    #[test]
    fn cover_persists_before_returning() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("frontier_report_clock.json");
        let (mut floor, _) = ReportClockFloor::load(path.clone());

        // Atomic replace: after every successful cover the on-disk state
        // is complete and already carries the NEW lease (write-ahead), and
        // no temp file is left behind.
        for physical in [2_000u64, 40_000, 41_000_000] {
            floor.cover(&ts(physical)).unwrap();
            let on_disk: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            assert_eq!(on_disk["version"], 1);
            assert_eq!(
                on_disk["leased_physical_ms"].as_u64().unwrap(),
                floor.leased()
            );
            assert!(on_disk["leased_physical_ms"].as_u64().unwrap() > physical);
        }
        let stray: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(".tmp."))
            .collect();
        assert!(stray.is_empty(), "no temp files may survive a cover");

        // A cover against an unwritable path fails and leaves the lease
        // unchanged (the caller skips the tick).
        let bad = ReportClockFloor {
            path: dir.path().join("no-such-dir-file/floor.json"),
            leased_physical_ms: 0,
        };
        let mut bad = bad;
        // write_atomic creates parent dirs, so point INTO a regular file
        // to force a real failure.
        std::fs::write(dir.path().join("blocker"), b"x").unwrap();
        bad.path = dir.path().join("blocker/floor.json");
        assert!(bad.cover(&ts(1)).is_err());
        assert_eq!(bad.leased(), 0);
    }
}
