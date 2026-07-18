use std::collections::HashMap;

use crate::error::CrdtError;
use crate::hlc::{Hlc, HlcTimestamp};
use crate::session::SessionToken;
use crate::store::kv::{CrdtValue, Store};
#[cfg(not(target_arch = "wasm32"))]
use crate::store::wal::{WalPos, WalRecord, WalWriter};
use crate::types::NodeId;

use crate::crdt::lww_register::LwwRegister;
use crate::crdt::or_map::OrMap;
use crate::crdt::or_set::OrSet;
use crate::crdt::pn_counter::PnCounter;

/// Eventual consistency API (FR-002, FR-004).
///
/// Reads and writes are local-first: writes are accepted immediately
/// and propagated asynchronously. Reads return the local CRDT state,
/// which converges across replicas via merge.
///
/// ## Durability (WAL)
///
/// When constructed via [`recovered`](Self::recovered) with a
/// [`WalWriter`], every mutation appends a state-based redo record (the
/// post-mutation CRDT value + HLC) to the write-ahead log BEFORE the call
/// returns. A failed append surfaces as [`CrdtError::Storage`]: the
/// in-memory effect remains (un-acked; it converges via anti-entropy), but
/// the caller must not acknowledge durability. `new` builds a WAL-less
/// API with the historical purely-in-memory behaviour.
pub struct EventualApi {
    store: Store,
    clock: Hlc,
    node_id: NodeId,
    /// Write-ahead log appender; `None` = persistence disabled.
    #[cfg(not(target_arch = "wasm32"))]
    wal: Option<WalWriter>,
    /// Position of the most recent WAL append, for `wait_durable`.
    #[cfg(not(target_arch = "wasm32"))]
    last_wal_pos: Option<WalPos>,
    /// Poison marks whose `MergeFailed` append failed. They are re-appended
    /// in front of the NEXT record (see `wal_append`): a frontier-advancing
    /// record must never reach the log while a poison mark is missing from
    /// it, or a crash would replay the frontier without the poison — a
    /// false session success (the invariant on `WalRecord::MergeFailed`).
    #[cfg(not(target_arch = "wasm32"))]
    pending_poison: Vec<String>,
}

impl EventualApi {
    /// Create a new EventualApi for the given node (no WAL: in-memory only).
    pub fn new(node_id: NodeId) -> Self {
        let clock = Hlc::new(node_id.0.clone());
        Self {
            store: Store::new(),
            clock,
            node_id,
            #[cfg(not(target_arch = "wasm32"))]
            wal: None,
            #[cfg(not(target_arch = "wasm32"))]
            last_wal_pos: None,
            #[cfg(not(target_arch = "wasm32"))]
            pending_poison: Vec::new(),
        }
    }

    /// Create an EventualApi from a recovered store (snapshot + WAL replay).
    ///
    /// Seeds the HLC clock from the highest recovered timestamp so writes
    /// issued after the restart are strictly greater than anything already
    /// persisted (clock rollback would break LWW and delta sync).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn recovered(node_id: NodeId, store: Store, wal: Option<WalWriter>) -> Self {
        let mut clock = Hlc::new(node_id.0.clone());
        if let Some(max) = store.max_known_hlc() {
            clock.seed_recovered(&max);
        }
        Self {
            store,
            clock,
            node_id,
            wal,
            last_wal_pos: None,
            pending_poison: Vec::new(),
        }
    }

    /// Fence the local origin's applied frontier against a possible lost
    /// acked suffix (recovery gap, session guarantees).
    ///
    /// Called by crash recovery whenever the WAL could NOT guarantee that
    /// every acked write survived: sync policy `interval`/`off`,
    /// persistence disabled entirely, or an explicit truncating
    /// corruption recovery. In those modes an acked suffix of this
    /// node's writes may be gone while `applied_origins[self]` still
    /// reads the pre-loss frontier — and the FIRST post-restart local
    /// write would max-merge the frontier past the hole, so a session
    /// token for a lost write would wrongly pass evidence path A
    /// (`api-reference.md` promises 412, never a false success).
    ///
    /// The fence spans `(applied_origins[self] as recovered, ceiling]`
    /// where the ceiling is the END of the current physical millisecond
    /// (`(now_ms, u32::MAX)`): everything provably applied stays
    /// covered, and the lost range answers 412 until anti-entropy
    /// adoption ([`Store::merge_applied_origins`]) proves it re-covered
    /// by a peer that still holds the writes — at which point tokens
    /// turn 200 again. Until then the fence only produces
    /// contract-permitted false negatives.
    ///
    /// The clock is then advanced past the ceiling so every post-restart
    /// local write lands strictly above it (a node must never fence its
    /// own fresh writes). Covering the whole millisecond matters: lost
    /// pre-crash writes routinely share a physical millisecond with the
    /// recovery instant at higher logical counters, and a `clock.now()`
    /// ceiling would let exactly those slip past the fence. Residual
    /// exposure: a pre-crash clock that ran AHEAD of this boot's wall
    /// clock (peer-skew propagation, wall-clock regression across the
    /// reboot) can have issued lost writes above the ceiling — the same
    /// clock-trust assumption the rest of the HLC design makes.
    ///
    /// **Durability**: this only mutates the IN-MEMORY store — no WAL
    /// record exists for fences, and `recovery_gaps` persist exclusively
    /// via snapshots. The recovery caller (`persistence::recover_eventual`)
    /// therefore forces a snapshot right after installing the fence,
    /// before any traffic is served; otherwise a second crash before the
    /// first periodic checkpoint would silently drop the fence while WAL
    /// replay of post-recovery writes re-opens the false-success path.
    ///
    /// Not available on `wasm32` (no crash-recovery persistence there).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn install_recovery_fence(&mut self) -> Result<(), CrdtError> {
        let floor = self
            .store
            .applied_origin(&self.node_id.0)
            .cloned()
            .unwrap_or(HlcTimestamp {
                physical: 0,
                logical: 0,
                node_id: String::new(),
            });
        let now = self.clock.now()?;
        let ceiling = HlcTimestamp {
            physical: now.physical,
            logical: u32::MAX,
            node_id: self.node_id.0.clone(),
        };
        // Advance the clock past the ceiling (into the next physical
        // millisecond) so no post-restart write can fall inside the gap.
        // Running ≤1 ms ahead of the wall clock is well within the HLC's
        // normal skew tolerance.
        self.clock.seed_recovered(&HlcTimestamp {
            physical: now.physical.saturating_add(1),
            logical: 0,
            node_id: self.node_id.0.clone(),
        });
        self.store
            .add_recovery_gap(self.node_id.0.clone(), floor, ceiling);
        Ok(())
    }

    // ---------------------------------------------------------------
    // WAL plumbing
    // ---------------------------------------------------------------

    /// Append a record to the WAL (no-op when persistence is disabled).
    ///
    /// Poison marks whose own `MergeFailed` append previously failed are
    /// flushed FIRST: no later record — in particular no frontier-advancing
    /// `UpsertApplied`/`SessionClaims` — may reach the log while a poison
    /// mark is missing from it. If the flush fails, the new record is not
    /// appended either (ordering is the invariant, not best-effort).
    #[cfg(not(target_arch = "wasm32"))]
    fn wal_append(&mut self, record: WalRecord) -> Result<(), CrdtError> {
        let Some(wal) = self.wal.as_mut() else {
            return Ok(());
        };
        if !self.pending_poison.is_empty() {
            let poison = WalRecord::MergeFailed {
                keys: self.pending_poison.clone(),
            };
            match wal.append(&poison) {
                Ok(pos) => {
                    self.last_wal_pos = Some(pos);
                    self.pending_poison.clear();
                }
                Err(e) => {
                    return Err(CrdtError::Storage(format!(
                        "WAL append failed (pending poison marks must precede new records): {e}"
                    )));
                }
            }
        }
        match wal.append(&record) {
            Ok(pos) => self.last_wal_pos = Some(pos),
            Err(e) => return Err(CrdtError::Storage(format!("WAL append failed: {e}"))),
        }
        Ok(())
    }

    /// Log the post-mutation state of `key` as an `UpsertApplied` record.
    #[cfg(not(target_arch = "wasm32"))]
    fn wal_log_applied(&mut self, key: &str, hlc: &HlcTimestamp) -> Result<(), CrdtError> {
        if self.wal.is_none() {
            return Ok(());
        }
        let value = self
            .store
            .get(key)
            .cloned()
            .ok_or_else(|| CrdtError::Internal(format!("no post-state for WAL key {key}")))?;
        self.wal_append(WalRecord::UpsertApplied {
            key: key.to_string(),
            value,
            hlc: hlc.clone(),
        })
    }

    #[cfg(target_arch = "wasm32")]
    fn wal_log_applied(&mut self, _key: &str, _hlc: &HlcTimestamp) -> Result<(), CrdtError> {
        Ok(())
    }

    /// Log the post-mutation state of `key` as an `UpsertVisible` record.
    #[cfg(not(target_arch = "wasm32"))]
    fn wal_log_visible(&mut self, key: &str, hlc: &HlcTimestamp) -> Result<(), CrdtError> {
        if self.wal.is_none() {
            return Ok(());
        }
        let value = self
            .store
            .get(key)
            .cloned()
            .ok_or_else(|| CrdtError::Internal(format!("no post-state for WAL key {key}")))?;
        self.wal_append(WalRecord::UpsertVisible {
            key: key.to_string(),
            value,
            hlc: hlc.clone(),
        })
    }

    #[cfg(target_arch = "wasm32")]
    fn wal_log_visible(&mut self, _key: &str, _hlc: &HlcTimestamp) -> Result<(), CrdtError> {
        Ok(())
    }

    /// Persist a merge-failure poison mark. The caller is already
    /// propagating the merge error, so an append failure here does not
    /// surface — but it is NOT merely logged: the key is queued in
    /// `pending_poison` and the `MergeFailed` record is re-appended in
    /// front of the next successful append (`wal_append`), so a later
    /// frontier-advancing record can never reach the log without it.
    /// Losing the poison while keeping the frontier would produce false
    /// session successes after a crash.
    #[cfg(not(target_arch = "wasm32"))]
    fn wal_log_merge_failed(&mut self, key: &str) {
        if self.wal.is_none() {
            return;
        }
        if let Err(e) = self.wal_append(WalRecord::MergeFailed {
            keys: vec![key.to_string()],
        }) {
            tracing::warn!(
                key = %key,
                error = %e,
                "failed to WAL-log merge poison; queued for re-append before the next record"
            );
            if !self.pending_poison.iter().any(|k| k == key) {
                self.pending_poison.push(key.to_string());
            }
        }
    }

    #[cfg(target_arch = "wasm32")]
    fn wal_log_merge_failed(&mut self, _key: &str) {}

    /// Finish a local write: record the change position, advance the applied
    /// frontier, and log the post-state as an `UpsertApplied` record.
    ///
    /// If the WAL append fails, the key is poisoned exactly as
    /// [`merge_remote`](Self::merge_remote) does: `note_applied` has already
    /// advanced `applied_origins`, but the data record is NOT in the WAL,
    /// while a *later* successful append (e.g. an adopted `SessionClaims` or
    /// the next mutation) can still persist a frontier that covers this write.
    /// Without the poison, recovery replays that later frontier, `note_applied`
    /// re-advances past the never-logged record, and a read-your-writes token
    /// for this write passes `session_check` on a store that is missing the
    /// data — a false success the fail-closed design promises never happens.
    fn finish_local_write(&mut self, key: &str, ts: &HlcTimestamp) -> Result<(), CrdtError> {
        self.store.record_change(key, ts.clone());
        self.store.note_applied(ts);
        if let Err(e) = self.wal_log_applied(key, ts) {
            self.store.note_merge_failed(key);
            self.wal_log_merge_failed(key);
            return Err(e);
        }
        Ok(())
    }

    /// Position of the most recent WAL append (for durability waits).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn last_wal_pos(&self) -> Option<WalPos> {
        self.last_wal_pos
    }

    /// Seal the active WAL segment and start a new one (checkpoint step 1).
    ///
    /// Returns the sealed segment sequence, or `None` when persistence is
    /// disabled. Must be called in the same critical section as the store
    /// clone that will be snapshotted, so the snapshot provably covers
    /// every record in segments `<= sealed`.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn wal_rotate(&mut self) -> std::io::Result<Option<u64>> {
        self.wal.as_mut().map(|w| w.rotate()).transpose()
    }

    /// Read the local CRDT value for a key (FR-002).
    ///
    /// Returns `None` if the key does not exist.
    pub fn get_eventual(&self, key: &str) -> Option<&CrdtValue> {
        self.store.get(key)
    }

    /// Write a CRDT value locally (FR-004).
    ///
    /// The value is accepted immediately and will be propagated
    /// to other nodes asynchronously. Atomically records the HLC timestamp
    /// for delta sync tracking so the entry is immediately visible to
    /// `delta_sync` without a separate `record_change` call.
    ///
    /// The value is CRDT-**merged** into any existing entry (never a plain
    /// replace), and a type-changing write is rejected with `TypeMismatch`.
    /// This is required for crash safety, not just CRDT hygiene: WAL
    /// recovery rebuilds state by merging the logged post-states, so the
    /// live path must only produce post-states that dominate the previous
    /// ones for the key. A replace can regress CRDT state (e.g. overwrite
    /// counter `{a:2}` with a fresh `{b:1}`), and replay's merge would
    /// then resurrect the replaced contributions — recovered state would
    /// diverge from the acked pre-crash state.
    pub fn eventual_write(
        &mut self,
        key: String,
        value: CrdtValue,
    ) -> Result<HlcTimestamp, CrdtError> {
        let ts = self.clock.now()?;
        self.store.merge_value(key.clone(), &value)?;
        self.finish_local_write(&key, &ts)?;
        Ok(ts)
    }

    /// Increment a PN-Counter at the given key.
    ///
    /// Creates the counter if the key does not exist.
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    pub fn eventual_counter_inc(&mut self, key: &str) -> Result<HlcTimestamp, CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Counter(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Counter".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                self.store
                    .put(key.to_string(), CrdtValue::Counter(PnCounter::new()));
            }
        }
        // Safe: we just ensured a Counter exists at this key.
        if let Some(CrdtValue::Counter(c)) = self.store.get_mut(key) {
            c.increment(&self.node_id);
        }
        let ts = self.clock.now()?;
        self.finish_local_write(key, &ts)?;
        Ok(ts)
    }

    /// Decrement a PN-Counter at the given key.
    ///
    /// Creates the counter if the key does not exist.
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    pub fn eventual_counter_dec(&mut self, key: &str) -> Result<HlcTimestamp, CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Counter(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Counter".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                self.store
                    .put(key.to_string(), CrdtValue::Counter(PnCounter::new()));
            }
        }
        if let Some(CrdtValue::Counter(c)) = self.store.get_mut(key) {
            c.decrement(&self.node_id);
        }
        let ts = self.clock.now()?;
        self.finish_local_write(key, &ts)?;
        Ok(ts)
    }

    /// Add an element to an OR-Set at the given key.
    ///
    /// Creates the set if the key does not exist.
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    pub fn eventual_set_add(
        &mut self,
        key: &str,
        element: String,
    ) -> Result<HlcTimestamp, CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Set(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Set".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                self.store
                    .put(key.to_string(), CrdtValue::Set(OrSet::new()));
            }
        }
        if let Some(CrdtValue::Set(s)) = self.store.get_mut(key) {
            s.add(element, &self.node_id);
        }
        let ts = self.clock.now()?;
        self.finish_local_write(key, &ts)?;
        Ok(ts)
    }

    /// Remove an element from an OR-Set at the given key.
    ///
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    /// Returns `KeyNotFound` if the key does not exist.
    pub fn eventual_set_remove(
        &mut self,
        key: &str,
        element: &str,
    ) -> Result<HlcTimestamp, CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Set(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Set".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                return Err(CrdtError::KeyNotFound(key.to_string()));
            }
        }
        if let Some(CrdtValue::Set(s)) = self.store.get_mut(key) {
            s.remove(&element.to_string());
        }
        let ts = self.clock.now()?;
        self.finish_local_write(key, &ts)?;
        Ok(ts)
    }

    /// Set a key-value pair in an OR-Map at the given key.
    ///
    /// Creates the map if the key does not exist.
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    pub fn eventual_map_set(
        &mut self,
        key: &str,
        map_key: String,
        map_value: String,
    ) -> Result<HlcTimestamp, CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Map(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Map".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                self.store
                    .put(key.to_string(), CrdtValue::Map(OrMap::new()));
            }
        }
        let ts = self.clock.now()?;
        if let Some(CrdtValue::Map(m)) = self.store.get_mut(key) {
            m.set(map_key, map_value, ts.clone(), &self.node_id);
        }
        self.finish_local_write(key, &ts)?;
        Ok(ts)
    }

    /// Delete a key from an OR-Map at the given key.
    ///
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    /// Returns `KeyNotFound` if the key does not exist.
    pub fn eventual_map_delete(
        &mut self,
        key: &str,
        map_key: &str,
    ) -> Result<HlcTimestamp, CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Map(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Map".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                return Err(CrdtError::KeyNotFound(key.to_string()));
            }
        }
        if let Some(CrdtValue::Map(m)) = self.store.get_mut(key) {
            m.delete(&map_key.to_string());
        }
        let ts = self.clock.now()?;
        self.finish_local_write(key, &ts)?;
        Ok(ts)
    }

    /// Set a LWW-Register value at the given key.
    ///
    /// Creates the register if the key does not exist.
    /// Returns `TypeMismatch` if the key exists with a different CRDT type.
    pub fn eventual_register_set(
        &mut self,
        key: &str,
        value: String,
    ) -> Result<HlcTimestamp, CrdtError> {
        match self.store.get(key) {
            Some(CrdtValue::Register(_)) => {}
            Some(other) => {
                return Err(CrdtError::TypeMismatch {
                    expected: "Register".into(),
                    actual: other.type_name().into(),
                });
            }
            None => {
                self.store
                    .put(key.to_string(), CrdtValue::Register(LwwRegister::new()));
            }
        }
        let ts = self.clock.now()?;
        if let Some(CrdtValue::Register(r)) = self.store.get_mut(key) {
            r.set(value, ts.clone());
        }
        self.finish_local_write(key, &ts)?;
        Ok(ts)
    }

    /// Merge a CRDT value received from a remote node.
    ///
    /// Delegates to `Store::merge_value`, which handles type checking
    /// and CRDT-specific merge semantics. Records the HLC timestamp
    /// for delta sync tracking.
    pub fn merge_remote(&mut self, key: String, remote_value: &CrdtValue) -> Result<(), CrdtError> {
        if let Err(e) = self.store.merge_value(key.clone(), remote_value) {
            // Poison the key for session checks: a remote contribution was
            // dropped, so the applied_origins invariant no longer holds
            // for this key (the frontier may advance via other keys).
            self.store.note_merge_failed(&key);
            // Persist the poison too: losing it while a later snapshot /
            // claims record keeps the frontier would fake session success.
            self.wal_log_merge_failed(&key);
            return Err(e);
        }
        let ts = self.clock.now()?;
        self.store.record_change(&key, ts.clone());
        // The push path carries no origin HLC; the merged state is
        // re-stamped with a local timestamp, so only the LOCAL origin
        // frontier may advance. Claiming the remote origin here would be
        // unsound (push batches are partial, not a complete prefix).
        self.store.note_applied(&ts);
        if let Err(e) = self.wal_log_applied(&key, &ts) {
            // The remote contribution IS merged and visible in memory, but
            // its data record is NOT in the WAL — while later successful
            // appends (e.g. an adopted SessionClaims) may still persist a
            // frontier covering it. Poison the key so session checks stay
            // fail-closed across a crash instead of claiming data whose
            // record was never logged (frontier-without-data).
            self.store.note_merge_failed(&key);
            self.wal_log_merge_failed(&key);
            return Err(e);
        }
        Ok(())
    }

    /// Merge a CRDT value received from a remote node with a pre-assigned HLC.
    ///
    /// Used by delta sync to preserve the original modification timestamp.
    /// Only updates the change timestamp if the incoming HLC is newer than
    /// the existing one for that key, preventing an older remote timestamp
    /// from overwriting a newer local one.
    ///
    /// Session guarantees: this method makes **no per-origin claim**
    /// (`applied_origins` is untouched). A per-entry claim on the entry's
    /// origin would be unsound: even a transfer that is complete relative
    /// to the SENDER's state does not prove the sender itself holds the
    /// entry origin's full write prefix (the entry may be a third-party
    /// write the sender received through a gappy delta). Claims are made
    /// exclusively by adopting the sender's transmitted `applied_origins`
    /// map ([`Store::merge_applied_origins`]) when the transfer is
    /// provably complete — see `NodeRunner::apply_delta_response`.
    ///
    /// The entry's origin position IS recorded in the store's visible
    /// frontier ([`Store::note_visible`]) so response session tokens
    /// cover everything a reader can observe here.
    pub fn merge_remote_with_hlc(
        &mut self,
        key: String,
        remote_value: &CrdtValue,
        hlc: HlcTimestamp,
    ) -> Result<(), CrdtError> {
        // Clock update errors (ClockSkew, Overflow) must NOT prevent the CRDT
        // merge. Discarding the merge on ClockSkew causes permanent data loss
        // because node_runner advances the peer frontier regardless of per-entry
        // errors, so skipped entries are never re-requested. The clock update is
        // advisory (ordering only); CRDT correctness does not depend on it.
        let _ = self.clock.update(&hlc);
        if let Err(e) = self.store.merge_value(key.clone(), remote_value) {
            // Poison the key for session checks (see merge_remote).
            self.store.note_merge_failed(&key);
            self.wal_log_merge_failed(&key);
            return Err(e);
        }
        // The merged contribution is now visible; response tokens must
        // cover it even though no applied claim is made.
        self.store.note_visible(&hlc);
        // Always record the change using the maximum of the incoming HLC
        // and any existing timestamp for this key. This ensures that
        // merges are never silently dropped from the change log, which
        // would cause delta-sync peers to miss updates.
        self.store.record_change_max(&key, hlc.clone());
        if let Err(e) = self.wal_log_visible(&key, &hlc) {
            // Same as merge_remote: the entry is merged in memory but its
            // data record never reached the WAL, and the caller
            // (apply_delta_response) may still adopt the sender's applied
            // frontier covering it. Poison the key so a crash cannot
            // replay a frontier that claims data whose record was lost.
            self.store.note_merge_failed(&key);
            self.wal_log_merge_failed(&key);
            return Err(e);
        }
        Ok(())
    }

    /// Adopt a sync sender's session metadata (frontier adoption).
    ///
    /// Applies the three maps to the store and persists them as ONE
    /// `SessionClaims` WAL record, so the applied frontier and the poison
    /// set can never be separated by a crash (a frontier without its
    /// poison set produces false session successes after restart).
    ///
    /// Callers pass an empty `applied`/`failed` when the transfer's
    /// completeness could not be verified (visible-only adoption).
    pub fn adopt_session_claims(
        &mut self,
        applied: &HashMap<String, HlcTimestamp>,
        visible: &HashMap<String, HlcTimestamp>,
        failed: Vec<String>,
    ) -> Result<(), CrdtError> {
        if applied.is_empty() && visible.is_empty() && failed.is_empty() {
            return Ok(());
        }
        self.store.merge_applied_origins(applied);
        self.store.merge_visible_origins(visible);
        self.store.merge_failed_extend(failed.iter().cloned());
        #[cfg(not(target_arch = "wasm32"))]
        self.wal_append(WalRecord::SessionClaims {
            applied: applied.clone(),
            visible: visible.clone(),
            failed,
        })?;
        Ok(())
    }

    /// Check whether the local store satisfies a session token for `key`.
    ///
    /// Delegates to [`SessionToken::is_satisfied`]; see there for the
    /// soundness argument. Returns `false` when the replica cannot prove
    /// it has caught up (fail-closed — never a false success).
    pub fn session_check(&self, key: &str, token: &SessionToken) -> bool {
        token.is_satisfied(&self.store, key)
    }

    /// Return all keys in the store.
    pub fn keys(&self) -> Vec<&String> {
        self.store.keys()
    }

    /// Return keys that start with the given prefix.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<&String> {
        self.store.keys_with_prefix(prefix)
    }

    /// Return a reference to the underlying store.
    ///
    /// Used by the anti-entropy sync layer to read all entries for
    /// push-based replication.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Return a mutable reference to the underlying `Store`.
    ///
    /// Needed by tombstone GC and compaction to modify CRDT deferred sets
    /// and prune change-tracking timestamps in-place.
    ///
    /// # MAINTAINER WARNING
    /// Mutations through this reference BYPASS the write-ahead log. That
    /// is intentional (and safe) for compaction / tombstone GC only: their
    /// effects are captured by the next snapshot, and losing them to a
    /// crash merely re-runs the GC. Any NEW data mutation added through
    /// this path would be silently lost on crash — route it through the
    /// `EventualApi` mutation methods (or `adopt_session_claims`) instead.
    pub fn store_mut(&mut self) -> &mut Store {
        &mut self.store
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId(name.into())
    }

    // ---------------------------------------------------------------
    // get_eventual
    // ---------------------------------------------------------------

    #[test]
    fn get_eventual_empty_store_returns_none() {
        let api = EventualApi::new(node("node-a"));
        assert!(api.get_eventual("missing").is_none());
    }

    // ---------------------------------------------------------------
    // eventual_write + get_eventual round-trip
    // ---------------------------------------------------------------

    #[test]
    fn eventual_write_and_get_round_trip() {
        let mut api = EventualApi::new(node("node-a"));

        let mut counter = PnCounter::new();
        counter.increment(&node("node-a"));
        api.eventual_write("hits".into(), CrdtValue::Counter(counter))
            .unwrap();

        match api.get_eventual("hits") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn eventual_write_overwrites() {
        let mut api = EventualApi::new(node("node-a"));

        let mut c1 = PnCounter::new();
        c1.increment(&node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Counter(c1))
            .unwrap();

        let mut c2 = PnCounter::new();
        c2.increment(&node("node-a"));
        c2.increment(&node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Counter(c2))
            .unwrap();

        match api.get_eventual("k") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // Counter inc/dec
    // ---------------------------------------------------------------

    #[test]
    fn counter_inc_creates_and_increments() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_inc("count").unwrap();
        api.eventual_counter_inc("count").unwrap();
        api.eventual_counter_inc("count").unwrap();

        match api.get_eventual("count") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 3),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn counter_dec_creates_and_decrements() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_dec("count").unwrap();
        api.eventual_counter_dec("count").unwrap();

        match api.get_eventual("count") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), -2),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn counter_inc_and_dec_combined() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_inc("count").unwrap();
        api.eventual_counter_inc("count").unwrap();
        api.eventual_counter_inc("count").unwrap();
        api.eventual_counter_dec("count").unwrap();

        match api.get_eventual("count") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn counter_inc_type_mismatch() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Set(OrSet::new()))
            .unwrap();

        let err = api.eventual_counter_inc("k").unwrap_err();
        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Counter".into(),
                actual: "Set".into(),
            }
        );
    }

    #[test]
    fn counter_dec_type_mismatch() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Register(LwwRegister::new()))
            .unwrap();

        let err = api.eventual_counter_dec("k").unwrap_err();
        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Counter".into(),
                actual: "Register".into(),
            }
        );
    }

    // ---------------------------------------------------------------
    // Set add/remove
    // ---------------------------------------------------------------

    #[test]
    fn set_add_creates_and_adds() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_set_add("users", "alice".into()).unwrap();
        api.eventual_set_add("users", "bob".into()).unwrap();

        match api.get_eventual("users") {
            Some(CrdtValue::Set(s)) => {
                assert!(s.contains(&"alice".to_string()));
                assert!(s.contains(&"bob".to_string()));
                assert_eq!(s.len(), 2);
            }
            other => panic!("expected Set, got {:?}", other),
        }
    }

    #[test]
    fn set_remove_removes_element() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_set_add("users", "alice".into()).unwrap();
        api.eventual_set_add("users", "bob".into()).unwrap();
        api.eventual_set_remove("users", "alice").unwrap();

        match api.get_eventual("users") {
            Some(CrdtValue::Set(s)) => {
                assert!(!s.contains(&"alice".to_string()));
                assert!(s.contains(&"bob".to_string()));
                assert_eq!(s.len(), 1);
            }
            other => panic!("expected Set, got {:?}", other),
        }
    }

    #[test]
    fn set_remove_nonexistent_key_returns_key_not_found() {
        let mut api = EventualApi::new(node("node-a"));
        let err = api.eventual_set_remove("missing", "x").unwrap_err();
        assert_eq!(err, CrdtError::KeyNotFound("missing".into()));
    }

    #[test]
    fn set_add_type_mismatch() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Counter(PnCounter::new()))
            .unwrap();

        let err = api.eventual_set_add("k", "x".into()).unwrap_err();
        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Set".into(),
                actual: "Counter".into(),
            }
        );
    }

    // ---------------------------------------------------------------
    // Map set/delete
    // ---------------------------------------------------------------

    #[test]
    fn map_set_creates_and_sets() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_map_set("config", "name".into(), "AsteroidDB".into())
            .unwrap();

        match api.get_eventual("config") {
            Some(CrdtValue::Map(m)) => {
                assert_eq!(m.get(&"name".to_string()), Some(&"AsteroidDB".to_string()));
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[test]
    fn map_set_overwrites_value() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_map_set("config", "name".into(), "old".into())
            .unwrap();
        api.eventual_map_set("config", "name".into(), "new".into())
            .unwrap();

        match api.get_eventual("config") {
            Some(CrdtValue::Map(m)) => {
                assert_eq!(m.get(&"name".to_string()), Some(&"new".to_string()));
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[test]
    fn map_delete_removes_entry() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_map_set("config", "name".into(), "AsteroidDB".into())
            .unwrap();
        api.eventual_map_set("config", "version".into(), "1.0".into())
            .unwrap();
        api.eventual_map_delete("config", "name").unwrap();

        match api.get_eventual("config") {
            Some(CrdtValue::Map(m)) => {
                assert!(!m.contains_key(&"name".to_string()));
                assert_eq!(m.get(&"version".to_string()), Some(&"1.0".to_string()));
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[test]
    fn map_delete_nonexistent_key_returns_key_not_found() {
        let mut api = EventualApi::new(node("node-a"));
        let err = api.eventual_map_delete("missing", "k").unwrap_err();
        assert_eq!(err, CrdtError::KeyNotFound("missing".into()));
    }

    #[test]
    fn map_set_type_mismatch() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Set(OrSet::new()))
            .unwrap();

        let err = api
            .eventual_map_set("k", "key".into(), "val".into())
            .unwrap_err();
        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Map".into(),
                actual: "Set".into(),
            }
        );
    }

    // ---------------------------------------------------------------
    // Register set
    // ---------------------------------------------------------------

    #[test]
    fn register_set_creates_and_sets() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_register_set("greeting", "hello".into())
            .unwrap();

        match api.get_eventual("greeting") {
            Some(CrdtValue::Register(r)) => {
                assert_eq!(r.get(), Some(&"hello".to_string()));
            }
            other => panic!("expected Register, got {:?}", other),
        }
    }

    #[test]
    fn register_set_overwrites_value() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_register_set("greeting", "hello".into())
            .unwrap();
        api.eventual_register_set("greeting", "world".into())
            .unwrap();

        match api.get_eventual("greeting") {
            Some(CrdtValue::Register(r)) => {
                assert_eq!(r.get(), Some(&"world".to_string()));
            }
            other => panic!("expected Register, got {:?}", other),
        }
    }

    #[test]
    fn register_set_type_mismatch() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Counter(PnCounter::new()))
            .unwrap();

        let err = api.eventual_register_set("k", "val".into()).unwrap_err();
        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Register".into(),
                actual: "Counter".into(),
            }
        );
    }

    // ---------------------------------------------------------------
    // merge_remote
    // ---------------------------------------------------------------

    #[test]
    fn merge_remote_matching_types() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_inc("count").unwrap();
        api.eventual_counter_inc("count").unwrap();

        let mut remote = PnCounter::new();
        remote.increment(&node("node-b"));
        remote.increment(&node("node-b"));
        remote.increment(&node("node-b"));

        api.merge_remote("count".into(), &CrdtValue::Counter(remote))
            .unwrap();

        match api.get_eventual("count") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 5),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn merge_remote_into_nonexistent_key() {
        let mut api = EventualApi::new(node("node-a"));

        let mut remote = PnCounter::new();
        remote.increment(&node("node-b"));

        api.merge_remote("new_key".into(), &CrdtValue::Counter(remote))
            .unwrap();

        match api.get_eventual("new_key") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn merge_remote_type_mismatch() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_write("k".into(), CrdtValue::Counter(PnCounter::new()))
            .unwrap();

        let err = api
            .merge_remote("k".into(), &CrdtValue::Set(OrSet::new()))
            .unwrap_err();
        assert_eq!(
            err,
            CrdtError::TypeMismatch {
                expected: "Counter".into(),
                actual: "Set".into(),
            }
        );
    }

    // ---------------------------------------------------------------
    // keys / keys_with_prefix
    // ---------------------------------------------------------------

    #[test]
    fn keys_returns_all_keys() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_inc("a").unwrap();
        api.eventual_counter_inc("b").unwrap();
        api.eventual_counter_inc("c").unwrap();

        let mut keys = api.keys();
        keys.sort();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn keys_with_prefix_filters() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_inc("user/alice").unwrap();
        api.eventual_counter_inc("user/bob").unwrap();
        api.eventual_counter_inc("config/db").unwrap();

        let mut user_keys = api.keys_with_prefix("user/");
        user_keys.sort();
        assert_eq!(user_keys, vec!["user/alice", "user/bob"]);

        let config_keys = api.keys_with_prefix("config/");
        assert_eq!(config_keys.len(), 1);
    }

    #[test]
    fn keys_with_prefix_no_match() {
        let mut api = EventualApi::new(node("node-a"));
        api.eventual_counter_inc("user/alice").unwrap();

        let keys = api.keys_with_prefix("log/");
        assert!(keys.is_empty());
    }

    // ---------------------------------------------------------------
    // Delta sync visibility (API layer)
    // ---------------------------------------------------------------

    /// Verify that `eventual_write` produces an entry immediately visible
    /// to delta sync (`entries_since` / `delta_entries_since`) at the API
    /// layer — not just at the `Store` level.
    #[test]
    fn eventual_write_is_visible_to_delta_sync() {
        let mut api = EventualApi::new(node("node-a"));

        // Record the frontier before any writes so we can query delta since
        // the beginning.
        let frontier_before = crate::hlc::HlcTimestamp {
            physical: 0,
            logical: 0,
            node_id: "".into(),
        };

        // Write a value via the EventualApi.
        let mut counter = PnCounter::new();
        counter.increment(&node("node-a"));
        api.eventual_write("hits".into(), CrdtValue::Counter(counter))
            .unwrap();

        // The written key must appear in entries_since (the delta sync view).
        let delta = api.store().entries_since(&frontier_before);
        assert_eq!(
            delta.len(),
            1,
            "expected exactly one delta entry after eventual_write"
        );
        assert_eq!(delta[0].0, "hits");

        // The entry must also appear via delta_entries_since.
        let delta2 = api.store().delta_entries_since(&frontier_before);
        assert_eq!(delta2.len(), 1);
        assert_eq!(delta2[0].0, "hits");
    }

    /// Verify that multiple `eventual_write` calls each produce delta-visible
    /// entries, covering heterogeneous CRDT types.
    #[test]
    fn eventual_write_multiple_keys_all_visible_to_delta_sync() {
        let mut api = EventualApi::new(node("node-a"));

        let frontier_before = crate::hlc::HlcTimestamp {
            physical: 0,
            logical: 0,
            node_id: "".into(),
        };

        api.eventual_write("key1".into(), CrdtValue::Counter(PnCounter::new()))
            .unwrap();
        api.eventual_write("key2".into(), CrdtValue::Set(OrSet::new()))
            .unwrap();
        api.eventual_write("key3".into(), CrdtValue::Register(LwwRegister::new()))
            .unwrap();

        let delta = api.store().entries_since(&frontier_before);
        assert_eq!(
            delta.len(),
            3,
            "all three eventual_write calls must be visible to delta sync"
        );

        let mut keys: Vec<&str> = delta.iter().map(|(k, _, _)| k.as_str()).collect();
        keys.sort();
        assert_eq!(keys, vec!["key1", "key2", "key3"]);
    }

    // ---------------------------------------------------------------
    // Session guarantees (read-your-writes / monotonic reads)
    // ---------------------------------------------------------------

    fn hlc_ts(physical: u64, logical: u32, node: &str) -> crate::hlc::HlcTimestamp {
        crate::hlc::HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    /// Every mutation must return an HLC that immediately satisfies a
    /// session check on the same node (read-your-writes locally).
    #[test]
    fn mutations_return_hlc_that_satisfies_local_session_check() {
        let mut api = EventualApi::new(node("node-a"));

        let checks: Vec<(String, crate::hlc::HlcTimestamp)> = vec![
            ("cnt".into(), api.eventual_counter_inc("cnt").unwrap()),
            ("cnt".into(), api.eventual_counter_dec("cnt").unwrap()),
            (
                "set".into(),
                api.eventual_set_add("set", "x".into()).unwrap(),
            ),
            ("set".into(), api.eventual_set_remove("set", "x").unwrap()),
            (
                "map".into(),
                api.eventual_map_set("map", "k".into(), "v".into()).unwrap(),
            ),
            ("map".into(), api.eventual_map_delete("map", "k").unwrap()),
            (
                "reg".into(),
                api.eventual_register_set("reg", "v".into()).unwrap(),
            ),
            (
                "raw".into(),
                api.eventual_write("raw".into(), CrdtValue::Counter(PnCounter::new()))
                    .unwrap(),
            ),
        ];

        for (key, ts) in checks {
            assert_eq!(ts.node_id, "node-a");
            assert!(
                api.store().applied_origin("node-a").unwrap() >= &ts,
                "applied frontier must cover the returned HLC"
            );
            let token = SessionToken::from_hlc(&ts);
            assert!(
                api.session_check(&key, &token),
                "write to {key} must satisfy its own token locally"
            );
        }
    }

    /// Fail-closed: a token from another origin is unsatisfied before any
    /// merge from that origin.
    #[test]
    fn foreign_token_unsatisfied_before_merge() {
        let mut api = EventualApi::new(node("node-b"));
        api.eventual_counter_inc("k").unwrap();

        let token = SessionToken::from_hlc(&hlc_ts(1, 0, "node-a"));
        assert!(!api.session_check("k", &token));
    }

    /// Regression test for the "false success" trap: a concurrent LOCAL
    /// write advances the per-key timestamp past the token, but the
    /// token's origin write has NOT been applied. `timestamp_for(key) >=
    /// token` would wrongly answer true here; the session check must not.
    #[test]
    fn concurrent_local_write_does_not_fake_satisfaction() {
        let mut api = EventualApi::new(node("node-b"));
        api.eventual_counter_inc("k").unwrap();
        api.eventual_counter_inc("k").unwrap();

        // Token from node-a, older than node-b's per-key timestamp.
        let token_hlc = hlc_ts(1, 0, "node-a");
        assert!(
            api.store().timestamp_for("k").unwrap() > &token_hlc,
            "precondition: per-key ts is past the token"
        );
        let token = SessionToken::from_hlc(&token_hlc);
        assert!(
            !api.session_check("k", &token),
            "per-key timestamp alone must not satisfy a foreign token"
        );
    }

    /// merge_remote_with_hlc (pull path) must NOT claim the entry's
    /// origin by itself — even a sender-complete delta does not prove the
    /// sender holds the entry origin's full write prefix. Claims come
    /// only from adopting the sender's applied_origins; the merged
    /// position is still recorded in the visible frontier for response
    /// tokens.
    #[test]
    fn merge_with_hlc_makes_no_claim_adoption_does() {
        let mut api = EventualApi::new(node("node-b"));

        let mut remote = PnCounter::new();
        remote.increment(&node("node-a"));
        let write_hlc = hlc_ts(100, 0, "node-a");
        api.merge_remote_with_hlc("k".into(), &CrdtValue::Counter(remote), write_hlc.clone())
            .unwrap();

        // No per-entry claim (would be a potential false success)...
        assert!(api.store().applied_origin("node-a").is_none());
        assert!(!api.session_check("k", &SessionToken::from_hlc(&write_hlc)));
        // ...but the visible frontier covers the merged contribution.
        assert_eq!(
            api.store().visible_origins().get("node-a"),
            Some(&write_hlc)
        );

        // Adoption of the sender's applied_origins makes the claim.
        let mut sender_applied = std::collections::HashMap::new();
        sender_applied.insert("node-a".to_string(), write_hlc.clone());
        api.store_mut().merge_applied_origins(&sender_applied);

        assert!(api.session_check("k", &SessionToken::from_hlc(&write_hlc)));
        assert!(api.session_check("k", &SessionToken::from_hlc(&hlc_ts(50, 0, "node-a"))));
        assert!(!api.session_check("k", &SessionToken::from_hlc(&hlc_ts(101, 0, "node-a"))));
    }

    /// A failed merge (type mismatch) must not advance the applied
    /// frontier and must poison the key.
    #[test]
    fn failed_merge_poisons_key_without_advancing_frontier() {
        let mut api = EventualApi::new(node("node-b"));
        api.eventual_counter_inc("k").unwrap();

        let err = api
            .merge_remote_with_hlc(
                "k".into(),
                &CrdtValue::Set(OrSet::new()),
                hlc_ts(100, 0, "node-a"),
            )
            .unwrap_err();
        assert!(matches!(err, CrdtError::TypeMismatch { .. }));
        assert!(api.store().applied_origin("node-a").is_none());
        assert!(api.store().merge_failed_contains("k"));
    }

    /// Poison soundness: origin frontier advanced by adoption after a
    /// successful merge on k2 must not satisfy tokens for the poisoned
    /// key k1 — not even after later successful merges on k1 (poison is
    /// permanent).
    #[test]
    fn poisoned_key_never_satisfied_via_applied_origins() {
        let mut api = EventualApi::new(node("node-b"));
        api.eventual_counter_inc("k1").unwrap();

        // node-a's write to k1 fails to merge (type mismatch) → poison.
        let failed_hlc = hlc_ts(100, 0, "node-a");
        api.merge_remote_with_hlc(
            "k1".into(),
            &CrdtValue::Set(OrSet::new()),
            failed_hlc.clone(),
        )
        .unwrap_err();

        // node-a's later write to k2 merges fine and the sender's
        // applied_origins is adopted → applied[node-a] = 200.
        let mut c = PnCounter::new();
        c.increment(&node("node-a"));
        api.merge_remote_with_hlc(
            "k2".into(),
            &CrdtValue::Counter(c),
            hlc_ts(200, 0, "node-a"),
        )
        .unwrap();
        let mut sender_applied = std::collections::HashMap::new();
        sender_applied.insert("node-a".to_string(), hlc_ts(200, 0, "node-a"));
        api.store_mut().merge_applied_origins(&sender_applied);
        assert_eq!(
            api.store().applied_origin("node-a"),
            Some(&hlc_ts(200, 0, "node-a"))
        );

        // k1's token must stay unsatisfied even though applied >= token.
        let token = SessionToken::from_hlc(&failed_hlc);
        assert!(!api.session_check("k1", &token), "poisoned key must fail");
        assert!(api.session_check("k2", &token), "k2 is not poisoned");

        // A later successful counter merge on k1 must NOT clear the poison
        // (the dropped set contribution is still missing).
        let mut c2 = PnCounter::new();
        c2.increment(&node("node-a"));
        api.merge_remote_with_hlc(
            "k1".into(),
            &CrdtValue::Counter(c2),
            hlc_ts(300, 0, "node-a"),
        )
        .unwrap();
        assert!(!api.session_check("k1", &token), "poison must be permanent");
    }

    /// Path B: register value evidence satisfies a token when the LWW
    /// timestamp dominates it, independently of applied_origins — but
    /// NEVER on a poisoned key (see the M-1 regression test below).
    #[test]
    fn register_value_evidence_path() {
        let mut api = EventualApi::new(node("node-b"));

        let mut reg = LwwRegister::new();
        reg.set("v".to_string(), hlc_ts(100, 0, "node-c"));
        // Merge WITHOUT origin HLC (push path) so applied_origins does not
        // cover node-a or node-c.
        api.merge_remote("reg".into(), &CrdtValue::Register(reg))
            .unwrap();
        assert!(api.store().applied_origin("node-a").is_none());

        // Register internal ts (100@node-c) dominates a token at 50@node-a.
        assert!(api.session_check("reg", &SessionToken::from_hlc(&hlc_ts(50, 0, "node-a"))));
        // ...but not a token above it.
        assert!(!api.session_check("reg", &SessionToken::from_hlc(&hlc_ts(101, 0, "node-a"))));

        // Path B must not fire for non-register types.
        let mut c = PnCounter::new();
        c.increment(&node("node-a"));
        api.merge_remote("cnt".into(), &CrdtValue::Counter(c))
            .unwrap();
        assert!(!api.session_check("cnt", &SessionToken::from_hlc(&hlc_ts(50, 0, "node-a"))));
    }

    /// M-1 regression: register value evidence must NOT satisfy a token on
    /// a poisoned key. The LWW-dominance argument assumes the token's
    /// write was itself a register write; a poisoned key has DROPPED a
    /// different-typed contribution (e.g. an OrSet write that hit
    /// TypeMismatch against this replica's Register), so the register
    /// timestamp proves nothing about the dropped write — answering 200
    /// would be a read-your-writes lie. Only false negatives (412) are
    /// contract-permitted.
    #[test]
    fn register_evidence_disabled_on_poisoned_key() {
        let mut api = EventualApi::new(node("node-b"));

        // The key is a Register locally with a high LWW timestamp.
        let mut reg = LwwRegister::new();
        reg.set("v".to_string(), hlc_ts(1_000, 0, "node-b"));
        api.merge_remote("k".into(), &CrdtValue::Register(reg))
            .unwrap();

        // node-a's OrSet write to the same key fails to merge → poison.
        let mut set = OrSet::new();
        set.add("member".to_string(), &node("node-a"));
        let set_write_hlc = hlc_ts(500, 0, "node-a");
        let err = api
            .merge_remote_with_hlc("k".into(), &CrdtValue::Set(set), set_write_hlc.clone())
            .unwrap_err();
        assert!(matches!(err, CrdtError::TypeMismatch { .. }));
        assert!(api.store().merge_failed_contains("k"));

        // The register timestamp (1000) dominates the token (500), but the
        // token's OrSet write was dropped: the session check must fail.
        assert!(
            !api.session_check("k", &SessionToken::from_hlc(&set_write_hlc)),
            "register evidence must be disabled on a poisoned key"
        );
    }

    /// The push path (merge_remote, no origin HLC) must only advance the
    /// LOCAL origin frontier — never the remote value's origins.
    #[test]
    fn merge_remote_advances_only_local_origin() {
        let mut api = EventualApi::new(node("node-b"));

        let mut remote = PnCounter::new();
        remote.increment(&node("node-a"));
        api.merge_remote("k".into(), &CrdtValue::Counter(remote))
            .unwrap();

        assert!(
            api.store().applied_origin("node-a").is_none(),
            "push path must not claim the remote origin"
        );
        assert!(api.store().applied_origin("node-b").is_some());
    }

    /// merge_remote failure must also poison the key (full-sync fallback
    /// path without per-key HLC).
    #[test]
    fn merge_remote_failure_poisons_key() {
        let mut api = EventualApi::new(node("node-b"));
        api.eventual_counter_inc("k").unwrap();

        api.merge_remote("k".into(), &CrdtValue::Set(OrSet::new()))
            .unwrap_err();
        assert!(api.store().merge_failed_contains("k"));
    }
}
