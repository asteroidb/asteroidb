use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::hlc::HlcTimestamp;
use crate::types::{CertificationStatus, NodeId};

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
    /// Set of authority node IDs that have acknowledged this write.
    pub acked_by: HashSet<NodeId>,
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
            acked_by: HashSet::new(),
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
    /// Duplicate acks from the same authority are ignored to prevent
    /// a single authority from inflating the ack count.
    /// If the unique ack count reaches the required threshold, the status
    /// is automatically promoted to `Certified`. Only `Pending` writes
    /// can receive acks.
    ///
    /// Returns the updated status, or `None` if the write is not found.
    pub fn record_ack(
        &mut self,
        write_id: &WriteId,
        authority_id: NodeId,
        now: HlcTimestamp,
    ) -> Option<CertificationStatus> {
        let entry = self.entries.get_mut(write_id)?;

        if entry.status != CertificationStatus::Pending {
            return Some(entry.status);
        }

        entry.acked_by.insert(authority_id);
        entry.updated_at = now;

        if entry.acked_by.len() >= entry.acks_required {
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

    fn auth(name: &str) -> NodeId {
        NodeId(name.into())
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

        let status = tracker.record_ack(&wid, auth("auth-1"), ts(1001, 0, "auth-1"));
        assert_eq!(status, Some(CertificationStatus::Pending));

        let status = tracker.record_ack(&wid, auth("auth-2"), ts(1002, 0, "auth-2"));
        assert_eq!(status, Some(CertificationStatus::Pending));

        assert_eq!(tracker.get_status(&wid), Some(CertificationStatus::Pending));
    }

    #[test]
    fn record_ack_reaching_threshold_promotes_to_certified() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("key-1", 1000);
        tracker.register_write(wid.clone(), 3, ts(1000, 0, "node-a"));

        tracker.record_ack(&wid, auth("auth-1"), ts(1001, 0, "auth-1"));
        tracker.record_ack(&wid, auth("auth-2"), ts(1002, 0, "auth-2"));
        let status = tracker.record_ack(&wid, auth("auth-3"), ts(1003, 0, "auth-3"));

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
        tracker.record_ack(&wid_certified, auth("auth-1"), ts(2001, 0, "auth-1"));
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
        tracker.record_ack(&write_id("c", 3000), auth("auth-1"), ts(3001, 0, "auth-1"));
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
        assert!(entry.acked_by.is_empty());
        assert_eq!(entry.acks_required, 3);

        // After an ack, updated_at and acked_by should change
        let ack_time = ts(1001, 0, "auth-1");
        tracker.record_ack(&wid, auth("auth-1"), ack_time.clone());
        let entry = tracker.get_entry(&wid).unwrap();
        assert_eq!(entry.acked_by.len(), 1);
        assert!(entry.acked_by.contains(&auth("auth-1")));
        assert_eq!(entry.updated_at, ack_time);
    }

    #[test]
    fn record_ack_for_unknown_write_returns_none() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("unknown", 9999);
        assert_eq!(
            tracker.record_ack(&wid, auth("auth-1"), ts(10000, 0, "auth-1")),
            None
        );
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

        tracker.record_ack(&wid, auth("auth-1"), ts(1001, 0, "auth-1"));
        assert_eq!(
            tracker.get_status(&wid),
            Some(CertificationStatus::Certified)
        );

        // Extra ack should not change anything
        let status = tracker.record_ack(&wid, auth("auth-2"), ts(1002, 0, "auth-2"));
        assert_eq!(status, Some(CertificationStatus::Certified));
    }

    #[test]
    fn reject_after_certified_is_no_op() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("key-1", 1000);
        tracker.register_write(wid.clone(), 1, ts(1000, 0, "node-a"));

        tracker.record_ack(&wid, auth("auth-1"), ts(1001, 0, "auth-1"));
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

    #[test]
    fn duplicate_ack_same_authority_does_not_inflate_count() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("key-1", 1000);
        tracker.register_write(wid.clone(), 3, ts(1000, 0, "node-a"));

        // Same authority acks 5 times
        for i in 0..5 {
            tracker.record_ack(&wid, auth("auth-1"), ts(1001 + i, 0, "auth-1"));
        }

        // Should still be pending because only 1 unique authority acked
        assert_eq!(tracker.get_status(&wid), Some(CertificationStatus::Pending));
        let entry = tracker.get_entry(&wid).unwrap();
        assert_eq!(entry.acked_by.len(), 1);
    }

    #[test]
    fn duplicate_acks_do_not_promote_pending_write() {
        let mut tracker = CertificationTracker::new();
        let wid = write_id("key-1", 1000);
        // Require 2 unique acks for certification
        tracker.register_write(wid.clone(), 2, ts(1000, 0, "node-a"));

        // Same authority acks many times — should NOT promote
        tracker.record_ack(&wid, auth("auth-1"), ts(1001, 0, "auth-1"));
        tracker.record_ack(&wid, auth("auth-1"), ts(1002, 0, "auth-1"));
        tracker.record_ack(&wid, auth("auth-1"), ts(1003, 0, "auth-1"));
        assert_eq!(tracker.get_status(&wid), Some(CertificationStatus::Pending));

        // A different authority acks → now reaches threshold
        let status = tracker.record_ack(&wid, auth("auth-2"), ts(1004, 0, "auth-2"));
        assert_eq!(status, Some(CertificationStatus::Certified));
    }
}
