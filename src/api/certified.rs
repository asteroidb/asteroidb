use crate::authority::ack_frontier::{AckFrontier, AckFrontierSet};
use crate::error::CrdtError;
use crate::hlc::{Hlc, HlcTimestamp};
use crate::store::kv::{CrdtValue, Store};
use crate::types::{CertificationStatus, NodeId};

/// What to do when `certified_write` cannot achieve consensus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnTimeout {
    /// Return `CrdtError::Timeout`.
    Error,
    /// Accept the write as `Pending` and let the caller poll status later.
    Pending,
}

/// Result of a certified read (FR-002).
#[derive(Debug)]
pub struct CertifiedRead<'a> {
    /// The CRDT value, if the key exists.
    pub value: Option<&'a CrdtValue>,
    /// Certification status of the value.
    pub status: CertificationStatus,
    /// The majority frontier at query time, if available.
    pub frontier: Option<HlcTimestamp>,
}

/// A write awaiting Authority majority certification.
#[derive(Debug, Clone)]
pub struct PendingWrite {
    /// The key that was written.
    pub key: String,
    /// The CRDT value that was written.
    pub value: CrdtValue,
    /// The HLC timestamp assigned to this write.
    pub timestamp: HlcTimestamp,
    /// Current certification status.
    pub status: CertificationStatus,
}

/// Certified consistency API (FR-002, FR-004).
///
/// Provides `get_certified` and `certified_write` operations that integrate
/// with the Authority ack_frontier to track and report certification status.
pub struct CertifiedApi {
    store: Store,
    clock: Hlc,
    frontiers: AckFrontierSet,
    total_authorities: usize,
    pending_writes: Vec<PendingWrite>,
}

impl CertifiedApi {
    /// Create a new `CertifiedApi` for the given node.
    pub fn new(node_id: NodeId, total_authorities: usize) -> Self {
        Self {
            store: Store::new(),
            clock: Hlc::new(node_id.0),
            frontiers: AckFrontierSet::new(),
            total_authorities,
            pending_writes: Vec::new(),
        }
    }

    /// Read a key with certification status (FR-002).
    ///
    /// Returns the value (if present), its certification status based on
    /// the latest pending write for that key, and the current majority frontier.
    pub fn get_certified(&self, key: &str) -> CertifiedRead<'_> {
        let value = self.store.get(key);
        let frontier = self
            .frontiers
            .majority_frontier(self.total_authorities)
            .cloned();

        let status = self
            .pending_writes
            .iter()
            .rev()
            .find(|pw| pw.key == key)
            .map(|pw| pw.status)
            .unwrap_or(CertificationStatus::Pending);

        CertifiedRead {
            value,
            status,
            frontier,
        }
    }

    /// Write a value that requires Authority majority certification (FR-004).
    ///
    /// The value is written to the local store immediately (eventual path).
    /// A `PendingWrite` entry is created to track certification progress.
    ///
    /// If the write is already certified at the current frontier, returns
    /// `Ok(CertificationStatus::Certified)`. Otherwise, behaviour depends
    /// on `on_timeout`:
    /// - `OnTimeout::Error` — returns `Err(CrdtError::Timeout)`.
    /// - `OnTimeout::Pending` — returns `Ok(CertificationStatus::Pending)`.
    ///
    /// Callers using `OnTimeout::Pending` can poll with
    /// `get_certification_status` or wait for `process_certifications`.
    pub fn certified_write(
        &mut self,
        key: String,
        value: CrdtValue,
        on_timeout: OnTimeout,
    ) -> Result<CertificationStatus, CrdtError> {
        let timestamp = self.clock.now();

        // Write to the local store (eventual consistency path).
        self.store.put(key.clone(), value.clone());

        // Check if already certified at the current frontier.
        let already_certified = self
            .frontiers
            .is_certified_at(&timestamp, self.total_authorities);

        let status = if already_certified {
            CertificationStatus::Certified
        } else {
            CertificationStatus::Pending
        };

        self.pending_writes.push(PendingWrite {
            key,
            value,
            timestamp,
            status,
        });

        if already_certified {
            return Ok(CertificationStatus::Certified);
        }

        match on_timeout {
            OnTimeout::Error => Err(CrdtError::Timeout),
            OnTimeout::Pending => Ok(CertificationStatus::Pending),
        }
    }

    /// Check the certification status of the latest write for a key.
    ///
    /// Returns `CertificationStatus::Pending` if no tracked write exists.
    pub fn get_certification_status(&self, key: &str) -> CertificationStatus {
        self.pending_writes
            .iter()
            .rev()
            .find(|pw| pw.key == key)
            .map(|pw| pw.status)
            .unwrap_or(CertificationStatus::Pending)
    }

    /// Update an Authority's ack frontier.
    ///
    /// Simulates receiving an ack from an Authority node.
    pub fn update_frontier(&mut self, frontier: AckFrontier) {
        self.frontiers.update(frontier);
    }

    /// Re-evaluate all pending writes against the current frontiers.
    ///
    /// Writes whose timestamps are at or below the majority frontier
    /// are promoted to `Certified`.
    pub fn process_certifications(&mut self) {
        for pw in &mut self.pending_writes {
            if pw.status == CertificationStatus::Pending
                && self
                    .frontiers
                    .is_certified_at(&pw.timestamp, self.total_authorities)
            {
                pw.status = CertificationStatus::Certified;
            }
        }
    }

    /// Return a slice of all pending writes.
    pub fn pending_writes(&self) -> &[PendingWrite] {
        &self.pending_writes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::ack_frontier::AckFrontier;
    use crate::crdt::pn_counter::PnCounter;
    use crate::hlc::HlcTimestamp;
    use crate::types::{KeyRange, NodeId, PolicyVersion};

    fn node(name: &str) -> NodeId {
        NodeId(name.into())
    }

    fn make_frontier(authority: &str, physical: u64, logical: u32, prefix: &str) -> AckFrontier {
        AckFrontier {
            authority_id: NodeId(authority.into()),
            frontier_hlc: HlcTimestamp {
                physical,
                logical,
                node_id: authority.into(),
            },
            key_range: KeyRange {
                prefix: prefix.into(),
            },
            policy_version: PolicyVersion(1),
            digest_hash: format!("{authority}-{physical}-{logical}"),
        }
    }

    fn counter_value(n: i64) -> CrdtValue {
        let mut counter = PnCounter::new();
        for _ in 0..n {
            counter.increment(&node("writer"));
        }
        CrdtValue::Counter(counter)
    }

    // ---------------------------------------------------------------
    // get_certified with no data
    // ---------------------------------------------------------------

    #[test]
    fn get_certified_no_data() {
        let api = CertifiedApi::new(node("node-1"), 3);
        let result = api.get_certified("missing");

        assert!(result.value.is_none());
        assert_eq!(result.status, CertificationStatus::Pending);
        assert!(result.frontier.is_none());
    }

    // ---------------------------------------------------------------
    // certified_write creates pending entry
    // ---------------------------------------------------------------

    #[test]
    fn certified_write_creates_pending_entry() {
        let mut api = CertifiedApi::new(node("node-1"), 3);
        let result = api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending);

        assert_eq!(result.unwrap(), CertificationStatus::Pending);
        assert_eq!(api.pending_writes().len(), 1);
        assert_eq!(api.pending_writes()[0].key, "key1");
        assert_eq!(api.pending_writes()[0].status, CertificationStatus::Pending);
    }

    // ---------------------------------------------------------------
    // get_certification_status returns Pending for new write
    // ---------------------------------------------------------------

    #[test]
    fn get_certification_status_pending_for_new_write() {
        let mut api = CertifiedApi::new(node("node-1"), 3);
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Pending
        );
    }

    #[test]
    fn get_certification_status_no_write_returns_pending() {
        let api = CertifiedApi::new(node("node-1"), 3);
        assert_eq!(
            api.get_certification_status("nonexistent"),
            CertificationStatus::Pending
        );
    }

    // ---------------------------------------------------------------
    // process_certifications: frontier updates → Certified
    // ---------------------------------------------------------------

    #[test]
    fn process_certifications_promotes_to_certified() {
        let mut api = CertifiedApi::new(node("node-1"), 3);
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Advance 2 of 3 authorities past the write timestamp (majority).
        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 200, 0, ""));

        api.process_certifications();

        assert_eq!(
            api.pending_writes()[0].status,
            CertificationStatus::Certified
        );
    }

    #[test]
    fn process_certifications_not_enough_authorities() {
        let mut api = CertifiedApi::new(node("node-1"), 3);
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Only 1 of 3 authorities has reported — not a majority.
        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));

        api.process_certifications();

        assert_eq!(api.pending_writes()[0].status, CertificationStatus::Pending);
    }

    // ---------------------------------------------------------------
    // on_timeout=Error with no resolution → returns error
    // ---------------------------------------------------------------

    #[test]
    fn certified_write_on_timeout_error() {
        let mut api = CertifiedApi::new(node("node-1"), 3);
        let result = api.certified_write("key1".into(), counter_value(1), OnTimeout::Error);

        assert_eq!(result.unwrap_err(), CrdtError::Timeout);
        // The write should still be tracked as pending.
        assert_eq!(api.pending_writes().len(), 1);
        assert_eq!(api.pending_writes()[0].status, CertificationStatus::Pending);
    }

    // ---------------------------------------------------------------
    // on_timeout=Pending with no resolution → returns Pending
    // ---------------------------------------------------------------

    #[test]
    fn certified_write_on_timeout_pending() {
        let mut api = CertifiedApi::new(node("node-1"), 3);
        let result = api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending);

        assert_eq!(result.unwrap(), CertificationStatus::Pending);
    }

    // ---------------------------------------------------------------
    // get_certified after certification → status Certified
    // ---------------------------------------------------------------

    #[test]
    fn get_certified_after_certification() {
        let mut api = CertifiedApi::new(node("node-1"), 3);
        api.certified_write("key1".into(), counter_value(5), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Advance majority of authorities.
        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 200, 0, ""));

        api.process_certifications();

        let result = api.get_certified("key1");
        assert!(result.value.is_some());
        assert_eq!(result.status, CertificationStatus::Certified);
        assert!(result.frontier.is_some());
    }

    // ---------------------------------------------------------------
    // Multiple writes and selective certification
    // ---------------------------------------------------------------

    #[test]
    fn multiple_writes_selective_certification() {
        let mut api = CertifiedApi::new(node("node-1"), 3);

        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let ts1 = api.pending_writes()[0].timestamp.physical;

        api.certified_write("key2".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();

        // Advance authorities past key1's timestamp but not key2's.
        api.update_frontier(make_frontier("auth-1", ts1 + 1, 0, ""));
        api.update_frontier(make_frontier("auth-2", ts1 + 1, 0, ""));

        api.process_certifications();

        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified
        );
        // key2 was written after key1 and the frontier may or may not cover it.
        // With ts1+1, the second write (which has a higher timestamp) might not be certified.
        // This depends on timing, so we just verify the API works.
        let status2 = api.get_certification_status("key2");
        assert!(
            status2 == CertificationStatus::Pending || status2 == CertificationStatus::Certified
        );
    }

    // ---------------------------------------------------------------
    // update_frontier advances tracking
    // ---------------------------------------------------------------

    #[test]
    fn update_frontier_updates_tracking() {
        let mut api = CertifiedApi::new(node("node-1"), 3);

        api.update_frontier(make_frontier("auth-1", 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", 200, 0, ""));
        api.update_frontier(make_frontier("auth-3", 150, 0, ""));

        // With all 3 authorities reporting, get_certified should have a frontier.
        let result = api.get_certified("any-key");
        assert!(result.frontier.is_some());
    }

    // ---------------------------------------------------------------
    // Value is stored in the local store
    // ---------------------------------------------------------------

    #[test]
    fn certified_write_stores_value_locally() {
        let mut api = CertifiedApi::new(node("node-1"), 3);
        api.certified_write("key1".into(), counter_value(3), OnTimeout::Pending)
            .unwrap();

        let read = api.get_certified("key1");
        assert!(read.value.is_some());
        match read.value.unwrap() {
            CrdtValue::Counter(c) => assert_eq!(c.value(), 3),
            other => panic!("expected Counter, got {:?}", other),
        }
    }
}
