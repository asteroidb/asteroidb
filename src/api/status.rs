use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::hlc::HlcTimestamp;
use crate::types::CertificationStatus;

/// Identifies a specific write operation by its key and timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WriteId {
    /// The key that was written.
    pub key: String,
    /// The HLC timestamp when the write was issued.
    pub timestamp: HlcTimestamp,
}

/// Entry tracking a single write's certification progress.
#[derive(Debug, Clone)]
pub struct StatusEntry {
    /// The write this entry tracks.
    pub write_id: WriteId,
    /// Current certification status.
    pub status: CertificationStatus,
    /// When the write was registered.
    pub created_at: HlcTimestamp,
    /// When the status was last updated.
    pub updated_at: HlcTimestamp,
    /// Number of authority acks received so far.
    pub acks_received: usize,
    /// Number of acks required for certification (majority threshold).
    pub acks_required: usize,
}

/// Tracks certification status of write operations.
///
/// Each write is identified by a `WriteId` (key + timestamp).
/// The tracker monitors acknowledgements from authority nodes and
/// automatically promotes writes to `Certified` once the majority
/// threshold is reached.
pub struct CertificationTracker {
    entries: HashMap<WriteId, StatusEntry>,
    default_timeout_ms: u64,
}

impl CertificationTracker {
    /// Creates a new tracker with the default timeout of 30 seconds.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            default_timeout_ms: 30_000,
        }
    }

    /// Creates a new tracker with a custom timeout in milliseconds.
    pub fn with_timeout(timeout_ms: u64) -> Self {
        Self {
            entries: HashMap::new(),
            default_timeout_ms: timeout_ms,
        }
    }

    /// Registers a new pending write operation.
    ///
    /// The write starts in `Pending` status and will be promoted to
    /// `Certified` once `acks_required` acknowledgements are received.
    pub fn register_write(&mut self, write_id: WriteId, acks_required: usize, now: HlcTimestamp) {
        let entry = StatusEntry {
            write_id: write_id.clone(),
            status: CertificationStatus::Pending,
            created_at: now.clone(),
            updated_at: now,
            acks_received: 0,
            acks_required,
        };
        self.entries.insert(write_id, entry);
    }

    /// Returns the current certification status of a write.
    pub fn get_status(&self, write_id: &WriteId) -> Option<CertificationStatus> {
        self.entries.get(write_id).map(|e| e.status)
    }

    /// Returns a reference to the full status entry for a write.
    pub fn get_entry(&self, write_id: &WriteId) -> Option<&StatusEntry> {
        self.entries.get(write_id)
    }

    /// Records an authority acknowledgement for a write.
    ///
    /// If the ack count reaches the required threshold, the status is
    /// automatically promoted to `Certified`. Only `Pending` writes
    /// can receive acks.
    ///
    /// Returns the updated status, or `None` if the write is not found.
    pub fn record_ack(
        &mut self,
        write_id: &WriteId,
        now: HlcTimestamp,
    ) -> Option<CertificationStatus> {
        let entry = self.entries.get_mut(write_id)?;

        if entry.status != CertificationStatus::Pending {
            return Some(entry.status);
        }

        entry.acks_received += 1;
        entry.updated_at = now;

        if entry.acks_received >= entry.acks_required {
            entry.status = CertificationStatus::Certified;
        }

        Some(entry.status)
    }

    /// Marks a write as rejected.
    ///
    /// Only `Pending` writes can be rejected.
    pub fn reject(&mut self, write_id: &WriteId, now: HlcTimestamp) {
        if let Some(entry) = self.entries.get_mut(write_id)
            && entry.status == CertificationStatus::Pending
        {
            entry.status = CertificationStatus::Rejected;
            entry.updated_at = now;
        }
    }

    /// Scans all pending entries and marks those that have exceeded
    /// the timeout as `Timeout`.
    ///
    /// A write is considered timed out when the difference between
    /// `now` and its `created_at` physical time exceeds `default_timeout_ms`.
    pub fn check_timeouts(&mut self, now: &HlcTimestamp) {
        for entry in self.entries.values_mut() {
            if entry.status == CertificationStatus::Pending
                && now.physical.saturating_sub(entry.created_at.physical) >= self.default_timeout_ms
            {
                entry.status = CertificationStatus::Timeout;
                entry.updated_at = now.clone();
            }
        }
    }

    /// Returns the number of writes currently in `Pending` status.
    pub fn pending_count(&self) -> usize {
        self.entries
            .values()
            .filter(|e| e.status == CertificationStatus::Pending)
            .count()
    }

    /// Returns all status entries for a given key, across all timestamps.
    pub fn get_status_by_key(&self, key: &str) -> Vec<&StatusEntry> {
        self.entries
            .values()
            .filter(|e| e.write_id.key == key)
            .collect()
    }

    /// Removes all completed entries (certified, rejected, or timed out).
    pub fn remove_completed(&mut self) {
        self.entries
            .retain(|_, e| e.status == CertificationStatus::Pending);
    }

    /// Removes entries older than `ttl_ms` regardless of status.
    ///
    /// An entry is removed when `now.physical - entry.created_at.physical >= ttl_ms`.
    pub fn remove_expired(&mut self, now: &HlcTimestamp, ttl_ms: u64) {
        self.entries
            .retain(|_, e| now.physical.saturating_sub(e.created_at.physical) < ttl_ms);
    }

    /// Full cleanup: check timeouts, then remove all completed and expired entries.
    ///
    /// This is the recommended periodic maintenance method. It:
    /// 1. Marks stale `Pending` entries as `Timeout`.
    /// 2. Removes all non-`Pending` entries.
    /// 3. Removes any remaining entries older than `ttl_ms`.
    pub fn cleanup(&mut self, now: &HlcTimestamp, ttl_ms: u64) {
        self.check_timeouts(now);
        self.remove_completed();
        self.remove_expired(now, ttl_ms);
    }

    /// Returns the total number of tracked entries (all statuses).
    pub fn total_count(&self) -> usize {
        self.entries.len()
    }
}

impl Default for CertificationTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    fn write_id(key: &str, physical: u64) -> WriteId {
        WriteId {
            key: key.into(),
            timestamp: ts(physical, 0, "node-a"),
        }
    }

    #[test]
    fn register_write_status_is_pending() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("key-1", 1000);
        tracker.register_write(wid.clone(), 3, ts(1000, 0, "node-a"));

        assert_eq!(tracker.get_status(&wid), Some(CertificationStatus::Pending));
    }

    #[test]
    fn record_ack_below_threshold_stays_pending() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("key-1", 1000);
        tracker.register_write(wid.clone(), 3, ts(1000, 0, "node-a"));

        let status = tracker.record_ack(&wid, ts(1001, 0, "auth-1"));
        assert_eq!(status, Some(CertificationStatus::Pending));

        let status = tracker.record_ack(&wid, ts(1002, 0, "auth-2"));
        assert_eq!(status, Some(CertificationStatus::Pending));

        assert_eq!(tracker.get_status(&wid), Some(CertificationStatus::Pending));
    }

    #[test]
    fn record_ack_reaching_threshold_promotes_to_certified() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("key-1", 1000);
        tracker.register_write(wid.clone(), 3, ts(1000, 0, "node-a"));

        tracker.record_ack(&wid, ts(1001, 0, "auth-1"));
        tracker.record_ack(&wid, ts(1002, 0, "auth-2"));
        let status = tracker.record_ack(&wid, ts(1003, 0, "auth-3"));

        assert_eq!(status, Some(CertificationStatus::Certified));
        assert_eq!(
            tracker.get_status(&wid),
            Some(CertificationStatus::Certified)
        );
    }

    #[test]
    fn reject_marks_status_rejected() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("key-1", 1000);
        tracker.register_write(wid.clone(), 3, ts(1000, 0, "node-a"));

        tracker.reject(&wid, ts(1001, 0, "auth-1"));

        assert_eq!(
            tracker.get_status(&wid),
            Some(CertificationStatus::Rejected)
        );
    }

    #[test]
    fn check_timeouts_marks_old_pending_as_timeout() {
        let mut tracker = CertificationTracker::with_timeout(5000);
        let wid = write_id("key-1", 1000);
        tracker.register_write(wid.clone(), 3, ts(1000, 0, "node-a"));

        // Not yet timed out
        tracker.check_timeouts(&ts(5999, 0, "node-a"));
        assert_eq!(tracker.get_status(&wid), Some(CertificationStatus::Pending));

        // Now timed out (6000 - 1000 = 5000 >= 5000)
        tracker.check_timeouts(&ts(6000, 0, "node-a"));
        assert_eq!(tracker.get_status(&wid), Some(CertificationStatus::Timeout));
    }

    #[test]
    fn get_status_by_key_returns_multiple_writes() {
        let mut tracker = CertificationTracker::new();
        let wid1 = write_id("key-1", 1000);
        let wid2 = write_id("key-1", 2000);
        let wid3 = write_id("key-2", 3000);

        tracker.register_write(wid1, 3, ts(1000, 0, "node-a"));
        tracker.register_write(wid2, 3, ts(2000, 0, "node-a"));
        tracker.register_write(wid3, 3, ts(3000, 0, "node-a"));

        let entries = tracker.get_status_by_key("key-1");
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.write_id.key == "key-1"));

        let entries = tracker.get_status_by_key("key-2");
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn remove_completed_cleans_up_finished_entries() {
        let mut tracker = CertificationTracker::with_timeout(5000);
        let wid_pending = write_id("pending", 10000);
        let wid_certified = write_id("certified", 2000);
        let wid_rejected = write_id("rejected", 3000);
        let wid_timeout = write_id("timeout", 4000);

        tracker.register_write(wid_pending.clone(), 3, ts(10000, 0, "node-a"));
        tracker.register_write(wid_certified.clone(), 1, ts(2000, 0, "node-a"));
        tracker.register_write(wid_rejected.clone(), 3, ts(3000, 0, "node-a"));
        tracker.register_write(wid_timeout.clone(), 3, ts(100, 0, "node-a"));

        // Certify one
        tracker.record_ack(&wid_certified, ts(2001, 0, "auth-1"));
        // Reject one
        tracker.reject(&wid_rejected, ts(3001, 0, "auth-1"));
        // Timeout one
        tracker.check_timeouts(&ts(10000, 0, "node-a"));

        // Verify states before cleanup
        assert_eq!(
            tracker.get_status(&wid_pending),
            Some(CertificationStatus::Pending)
        );
        assert_eq!(
            tracker.get_status(&wid_certified),
            Some(CertificationStatus::Certified)
        );
        assert_eq!(
            tracker.get_status(&wid_rejected),
            Some(CertificationStatus::Rejected)
        );
        assert_eq!(
            tracker.get_status(&wid_timeout),
            Some(CertificationStatus::Timeout)
        );

        tracker.remove_completed();

        // Only pending should remain
        assert_eq!(
            tracker.get_status(&wid_pending),
            Some(CertificationStatus::Pending)
        );
        assert_eq!(tracker.get_status(&wid_certified), None);
        assert_eq!(tracker.get_status(&wid_rejected), None);
        assert_eq!(tracker.get_status(&wid_timeout), None);
    }

    #[test]
    fn pending_count_accuracy() {
        let mut tracker = CertificationTracker::new();
        assert_eq!(tracker.pending_count(), 0);

        tracker.register_write(write_id("a", 1000), 2, ts(1000, 0, "node-a"));
        tracker.register_write(write_id("b", 2000), 2, ts(2000, 0, "node-a"));
        tracker.register_write(write_id("c", 3000), 1, ts(3000, 0, "node-a"));
        assert_eq!(tracker.pending_count(), 3);

        // Certify one
        tracker.record_ack(&write_id("c", 3000), ts(3001, 0, "auth-1"));
        assert_eq!(tracker.pending_count(), 2);

        // Reject one
        tracker.reject(&write_id("a", 1000), ts(1001, 0, "auth-1"));
        assert_eq!(tracker.pending_count(), 1);
    }

    #[test]
    fn status_entry_fields_are_correct() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("key-1", 1000);
        let created = ts(1000, 0, "node-a");
        tracker.register_write(wid.clone(), 3, created.clone());

        let entry = tracker.get_entry(&wid).unwrap();
        assert_eq!(entry.write_id, wid);
        assert_eq!(entry.status, CertificationStatus::Pending);
        assert_eq!(entry.created_at, created);
        assert_eq!(entry.updated_at, created);
        assert_eq!(entry.acks_received, 0);
        assert_eq!(entry.acks_required, 3);

        // After an ack, updated_at and acks_received should change
        let ack_time = ts(1001, 0, "auth-1");
        tracker.record_ack(&wid, ack_time.clone());
        let entry = tracker.get_entry(&wid).unwrap();
        assert_eq!(entry.acks_received, 1);
        assert_eq!(entry.updated_at, ack_time);
    }

    #[test]
    fn record_ack_for_unknown_write_returns_none() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("unknown", 9999);
        assert_eq!(tracker.record_ack(&wid, ts(10000, 0, "auth-1")), None);
    }

    #[test]
    fn reject_unknown_write_is_no_op() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("unknown", 9999);
        tracker.reject(&wid, ts(10000, 0, "auth-1"));
        assert_eq!(tracker.get_status(&wid), None);
    }

    #[test]
    fn ack_after_certified_does_not_change_status() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("key-1", 1000);
        tracker.register_write(wid.clone(), 1, ts(1000, 0, "node-a"));

        tracker.record_ack(&wid, ts(1001, 0, "auth-1"));
        assert_eq!(
            tracker.get_status(&wid),
            Some(CertificationStatus::Certified)
        );

        // Extra ack should not change anything
        let status = tracker.record_ack(&wid, ts(1002, 0, "auth-2"));
        assert_eq!(status, Some(CertificationStatus::Certified));
    }

    #[test]
    fn reject_after_certified_is_no_op() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("key-1", 1000);
        tracker.register_write(wid.clone(), 1, ts(1000, 0, "node-a"));

        tracker.record_ack(&wid, ts(1001, 0, "auth-1"));
        tracker.reject(&wid, ts(1002, 0, "auth-2"));

        // Should still be certified
        assert_eq!(
            tracker.get_status(&wid),
            Some(CertificationStatus::Certified)
        );
    }

    #[test]
    fn default_trait_implementation() {
        let tracker = CertificationTracker::default();
        assert_eq!(tracker.pending_count(), 0);
    }

    #[test]
    fn write_id_serde_roundtrip() {
        let wid = write_id("test-key", 42000);
        let json = serde_json::to_string(&wid).unwrap();
        let back: WriteId = serde_json::from_str(&json).unwrap();
        assert_eq!(wid, back);
    }

    // ---------------------------------------------------------------
    // remove_expired tests
    // ---------------------------------------------------------------

    #[test]
    fn remove_expired_removes_old_entries() {
        let mut tracker = CertificationTracker::new();
        let wid_old = write_id("old-key", 1000);
        let wid_new = write_id("new-key", 9000);

        tracker.register_write(wid_old.clone(), 3, ts(1000, 0, "node-a"));
        tracker.register_write(wid_new.clone(), 3, ts(9000, 0, "node-a"));

        // TTL of 5000ms: old-key (created at 1000) should be expired at 10000.
        tracker.remove_expired(&ts(10000, 0, "node-a"), 5000);

        assert_eq!(tracker.get_status(&wid_old), None);
        assert_eq!(
            tracker.get_status(&wid_new),
            Some(CertificationStatus::Pending)
        );
    }

    #[test]
    fn remove_expired_removes_completed_entries_too() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("key-1", 1000);
        tracker.register_write(wid.clone(), 1, ts(1000, 0, "node-a"));
        tracker.record_ack(&wid, ts(1001, 0, "auth-1"));

        assert_eq!(
            tracker.get_status(&wid),
            Some(CertificationStatus::Certified)
        );

        tracker.remove_expired(&ts(10000, 0, "node-a"), 5000);
        assert_eq!(tracker.get_status(&wid), None);
    }

    // ---------------------------------------------------------------
    // cleanup tests
    // ---------------------------------------------------------------

    #[test]
    fn cleanup_full_lifecycle() {
        let mut tracker = CertificationTracker::with_timeout(5000);
        let wid_pending = write_id("pending", 10000);
        let wid_certified = write_id("certified", 2000);
        let wid_old_pending = write_id("old-pending", 1000);

        tracker.register_write(wid_pending.clone(), 3, ts(10000, 0, "node-a"));
        tracker.register_write(wid_certified.clone(), 1, ts(2000, 0, "node-a"));
        tracker.register_write(wid_old_pending.clone(), 3, ts(1000, 0, "node-a"));

        // Certify one.
        tracker.record_ack(&wid_certified, ts(2001, 0, "auth-1"));

        // Full cleanup with ttl_ms=8000 at time 10000.
        // old-pending (1000): pending → timeout (5000ms timeout), then removed (completed).
        // certified (2000): already certified → removed (completed), also > 8000ms old.
        // pending (10000): still pending, not old enough.
        tracker.cleanup(&ts(10000, 0, "node-a"), 8000);

        assert_eq!(
            tracker.get_status(&wid_pending),
            Some(CertificationStatus::Pending)
        );
        assert_eq!(tracker.get_status(&wid_certified), None);
        assert_eq!(tracker.get_status(&wid_old_pending), None);
    }

    // ---------------------------------------------------------------
    // total_count tests
    // ---------------------------------------------------------------

    #[test]
    fn total_count_tracks_all_entries() {
        let mut tracker = CertificationTracker::new();
        assert_eq!(tracker.total_count(), 0);

        tracker.register_write(write_id("a", 1000), 2, ts(1000, 0, "node-a"));
        tracker.register_write(write_id("b", 2000), 1, ts(2000, 0, "node-a"));
        assert_eq!(tracker.total_count(), 2);

        // Certify one — total_count still 2.
        tracker.record_ack(&write_id("b", 2000), ts(2001, 0, "auth-1"));
        assert_eq!(tracker.total_count(), 2);

        // After remove_completed, only pending remains.
        tracker.remove_completed();
        assert_eq!(tracker.total_count(), 1);
    }

    // ---------------------------------------------------------------
    // Bounded growth under sustained writes
    // ---------------------------------------------------------------

    #[test]
    fn bounded_growth_with_cleanup() {
        let mut tracker = CertificationTracker::with_timeout(100);

        for i in 0..50u64 {
            let wid = write_id(&format!("key-{i}"), i * 10);
            tracker.register_write(wid.clone(), 1, ts(i * 10, 0, "node-a"));

            // Certify each write immediately.
            tracker.record_ack(&wid, ts(i * 10 + 1, 0, "auth-1"));
        }

        assert_eq!(tracker.total_count(), 50);

        // Cleanup should remove all certified entries.
        tracker.remove_completed();
        assert_eq!(tracker.total_count(), 0);
    }

    #[test]
    fn bounded_growth_with_ttl_cleanup() {
        let mut tracker = CertificationTracker::with_timeout(100);

        // Register writes at different times.
        for i in 0..20u64 {
            let wid = write_id(&format!("key-{i}"), i * 50);
            tracker.register_write(wid, 3, ts(i * 50, 0, "node-a"));
        }

        assert_eq!(tracker.total_count(), 20);

        // Full cleanup at time 2000 with TTL 500ms.
        // Entries older than 1500 (created_at < 1500) will be:
        // 1. Timeout-checked (entries created at < 1900 with 100ms timeout → all marked Timeout)
        // 2. Removed as completed (Timeout status)
        // 3. Remaining entries > 500ms old removed by TTL
        tracker.cleanup(&ts(2000, 0, "node-a"), 500);

        // Only entries created at >= 1500 (i.e., physical 1500, 1550, ..., 1950) survive.
        // But they were all marked as Timeout by check_timeouts (2000 - 1500 = 500 >= 100).
        // So remove_completed removes them too.
        // Actually all entries will be timed out since timeout is 100ms.
        // The only entries that survive TTL are those created at >= 1500 (2000 - 500).
        // But those are also timed out. So total_count should be 0.
        assert_eq!(tracker.total_count(), 0);
    }
}
