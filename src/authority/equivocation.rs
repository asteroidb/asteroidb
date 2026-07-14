//! Equivocation and split-view detection for signed frontier attestations.
//!
//! An **equivocation** is a pair of *signature-verified* attestations from
//! the same authority, for the same scope (`key_range`, `policy_version`)
//! and the *exact same* `frontier_hlc`, whose `digest_hash` values differ.
//! Because the report signature covers every field of the `AckFrontier`
//! ([`create_frontier_report_message`](crate::authority::frontier_sig::create_frontier_report_message)
//! binds `digest_hash`), a differing digest under an identical scope and HLC
//! is mathematically equivalent to two distinct signed report messages —
//! the pair is a self-contained, non-repudiable proof of misbehaviour (POM)
//! that any third party can re-verify against the registry key.
//!
//! What is deliberately **not** flagged (zero false positives):
//!
//! - Frontier HLC advancement within the same checkpoint bucket — a later
//!   HLC is a different index key, matching the legitimate-progress case
//!   fixed by `attestation_pool::duplicate_authority_counted_once`.
//! - Re-signing the *same* digest under a rotated keyset version — the
//!   comparison key is `digest_hash` only, never the signature bytes or
//!   `keyset_version`.
//!
//! The current digest is a deterministic function of `(node_id, hlc)`
//! (`frontier_reporter`), so an honest authority can never produce two
//! digests for one `frontier_hlc`. If the digest is ever changed to a
//! Merkle root over real data, the invariant "same `(authority,
//! frontier_hlc)` implies same digest" must be preserved for this
//! definition to stay false-positive-free; the detection pipeline itself
//! does not depend on digest semantics.
//!
//! Detection is **cheap and passive** (CT-gossip Protocol 2 style summaries
//! piggyback on the existing frontier push); *enforcement* is intentionally
//! out of scope — excluding a misbehaving authority requires consensus, so
//! the detector only records evidence, warns, and exposes it to operators.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::authority::ack_frontier::AckFrontier;
use crate::authority::frontier_sig::FrontierSignature;
use crate::hlc::HlcTimestamp;
use crate::types::{KeyRange, NodeId, PolicyVersion};

/// Maximum observed attestations retained per scope. Matches the
/// `AttestationPool` checkpoint cap so the detection window is roughly the
/// same (~2 minutes at the 1s report interval); older heads are evicted.
pub const MAX_OBSERVED_PER_SCOPE: usize = 128;

/// Maximum number of distinct `(authority, key_range, policy_version)`
/// scopes tracked in the observation index (memory DoS bound). When the
/// index is full, the least-recently-touched scope is evicted — new scopes
/// are always tracked, so detection is never silently disabled.
pub const MAX_TRACKED_SCOPES: usize = 1024;

/// Maximum scopes tracked per authority (fairness bound). Scope components
/// are attacker-chosen (a compromised authority can sign arbitrary
/// `policy_version` / `key_range` values), so without a per-authority cap a
/// single authority could occupy the whole index and blind detection for
/// everyone else. Beyond this cap the authority only evicts its *own*
/// least-recently-touched scope.
pub const MAX_TRACKED_SCOPES_PER_AUTHORITY: usize = 64;

/// Maximum evidence entries stored per accused authority. Further conflicts
/// only bump the overflow counter — the earliest evidence is what matters
/// for accountability and is never evicted.
pub const MAX_EVIDENCE_PER_AUTHORITY: usize = 16;

/// Reject observations whose `frontier_hlc.physical` is further than this
/// into the future (same value as `AttestationPool`'s skew guard). Prevents
/// far-future HLC floods from evicting genuine index entries.
pub const MAX_FUTURE_SKEW_MS: u64 = 60_000;

/// Per-request cap on relayed observations processed by the receive path.
/// Each relayed observation may cost two Ed25519 verifications, so this is
/// deliberately conservative (CPU DoS bound).
pub const MAX_OBSERVED_PER_REQUEST: usize = 64;

/// Maximum observations attached to an outgoing frontier push (gossip lane).
pub const GOSSIP_SAMPLE_MAX: usize = 64;

/// A signature-verified `(frontier, signature)` raw pair.
///
/// This is both the unit of evidence storage and the wire type for the
/// split-view gossip lane (`FrontierPushRequest::observed`): it carries the
/// full report signature, so a receiver can independently re-verify it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservedAttestation {
    /// The attested frontier (includes `frontier_hlc` and `digest_hash`).
    pub frontier: AckFrontier,
    /// The raw wire signature (includes the report signature over the
    /// entire frontier).
    pub signature: FrontierSignature,
}

/// Non-repudiable equivocation evidence: both conflicting signed messages
/// are stored verbatim, so the pair can be presented to a third party and
/// re-verified against the registry key (proof of misbehaviour).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EquivocationEvidence {
    /// The authority that signed both conflicting attestations.
    pub authority_id: NodeId,
    /// The key range scope both attestations cover.
    pub key_range: KeyRange,
    /// The policy version scope both attestations cover.
    pub policy_version: PolicyVersion,
    /// The exact frontier HLC both attestations claim.
    pub frontier_hlc: HlcTimestamp,
    /// The first observed signed attestation.
    pub first: ObservedAttestation,
    /// The conflicting signed attestation observed later.
    pub second: ObservedAttestation,
    /// Wall-clock time (ms since epoch) when the conflict was detected.
    pub detected_at_ms: u64,
}

/// Outcome of feeding one verified attestation into the detector.
#[derive(Debug)]
pub enum ObserveOutcome {
    /// New observation, registered in the index.
    FirstSeen,
    /// Matches a known observation byte-for-byte in the fields that matter
    /// (legitimate resend, gossip echo, or an already-recorded conflict).
    Consistent,
    /// A new conflicting pair was detected and recorded.
    Equivocation(Box<EquivocationEvidence>),
    /// Not recorded: future-skew guard.
    Skipped,
}

/// Observation index scope: one authority within one attestation scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ObsScope {
    authority_id: NodeId,
    key_range: KeyRange,
    policy_version: PolicyVersion,
}

impl ObsScope {
    fn of(frontier: &AckFrontier) -> Self {
        Self {
            authority_id: frontier.authority_id.clone(),
            key_range: frontier.key_range.clone(),
            policy_version: frontier.policy_version,
        }
    }
}

/// JSON layout of the persisted evidence file
/// (`<data-dir>/equivocation_evidence.json`).
#[derive(Serialize, Deserialize)]
struct PersistedEvidence {
    evidence: Vec<EquivocationEvidence>,
    accused: Vec<NodeId>,
}

/// Per-scope observation window plus an LRU stamp for scope eviction.
#[derive(Default)]
struct ScopeState {
    /// Observed attestations indexed by exact frontier HLC.
    heads: BTreeMap<HlcTimestamp, ObservedAttestation>,
    /// Monotonic touch stamp (from `DetectorState::touch_counter`), bumped
    /// on every observation for this scope. Least-recently-touched scopes
    /// are evicted first when a capacity bound is hit.
    last_touch: u64,
}

struct DetectorState {
    /// Observed attestations, indexed by scope then exact frontier HLC.
    observed: HashMap<ObsScope, ScopeState>,
    /// Stored evidence per accused authority (never evicted).
    evidence: HashMap<NodeId, Vec<EquivocationEvidence>>,
    /// Authorities with at least one detected equivocation.
    accused: HashSet<NodeId>,
    /// Conflicts detected beyond `MAX_EVIDENCE_PER_AUTHORITY` (not stored).
    evidence_overflow_total: u64,
    /// Monotonic counter feeding `ScopeState::last_touch`.
    touch_counter: u64,
    /// Rotating start position for the evidence share of the gossip sample,
    /// so every stored pair propagates across successive samples even when
    /// the evidence store exceeds its reserved budget.
    gossip_evidence_cursor: usize,
}

impl DetectorState {
    fn empty() -> Self {
        Self {
            observed: HashMap::new(),
            evidence: HashMap::new(),
            accused: HashSet::new(),
            evidence_overflow_total: 0,
            touch_counter: 0,
            gossip_evidence_cursor: 0,
        }
    }

    /// True when `digest` already appears in a stored evidence pair at this
    /// `(scope, frontier_hlc)` — i.e. the attestation is a gossip echo of a
    /// half of an already-recorded conflict, not a new conflict.
    fn evidence_digest_recorded(&self, scope: &ObsScope, hlc: &HlcTimestamp, digest: &str) -> bool {
        self.evidence
            .get(&scope.authority_id)
            .is_some_and(|entries| {
                entries.iter().any(|ev| {
                    ev.key_range == scope.key_range
                        && ev.policy_version == scope.policy_version
                        && ev.frontier_hlc == *hlc
                        && (ev.first.frontier.digest_hash == digest
                            || ev.second.frontier.digest_hash == digest)
                })
            })
    }

    /// Make room for a new scope owned by `authority`.
    ///
    /// Enforces the per-authority fairness cap first (an authority beyond
    /// its share only evicts its *own* least-recently-touched scope), then
    /// the global cap (evicting the globally least-recently-touched scope).
    /// Eviction instead of rejection means a new scope is always tracked —
    /// detection can never be permanently disabled by scope floods or by
    /// organic policy-version churn.
    fn make_room_for_scope(&mut self, authority: &NodeId) {
        let own_scopes = self
            .observed
            .keys()
            .filter(|s| s.authority_id == *authority)
            .count();
        let victim = if own_scopes >= MAX_TRACKED_SCOPES_PER_AUTHORITY {
            self.observed
                .iter()
                .filter(|(s, _)| s.authority_id == *authority)
                .min_by_key(|(_, st)| st.last_touch)
                .map(|(s, _)| s.clone())
        } else if self.observed.len() >= MAX_TRACKED_SCOPES {
            self.observed
                .iter()
                .min_by_key(|(_, st)| st.last_touch)
                .map(|(s, _)| s.clone())
        } else {
            None
        };
        if let Some(victim) = victim {
            tracing::debug!(
                authority = %victim.authority_id.0,
                key_range = %victim.key_range.prefix,
                policy_version = victim.policy_version.0,
                "evicting least-recently-touched equivocation scope (capacity)"
            );
            self.observed.remove(&victim);
        }
    }
}

/// Local equivocation detector and evidence store.
///
/// Thread-safe via an internal mutex; all methods take `&self`. The lock is
/// never held across I/O or `.await` — persistence is split into
/// [`EquivocationDetector::persist_payload`] (lock, serialize, release) and
/// a caller-side atomic write on a blocking thread.
pub struct EquivocationDetector {
    inner: Mutex<DetectorState>,
    persist_path: Option<PathBuf>,
    /// Serializes evidence writers: snapshots are taken *while holding*
    /// this gate, so two concurrent persists can never land in an order
    /// where an older snapshot overwrites a newer one. Never held together
    /// with `inner` (the snapshot itself locks `inner` briefly inside).
    /// Only used by [`EquivocationDetector::spawn_persist`], which needs
    /// the tokio blocking pool.
    #[cfg(feature = "native-runtime")]
    persist_gate: Mutex<()>,
}

impl std::fmt::Debug for EquivocationDetector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EquivocationDetector")
            .field("persist_path", &self.persist_path)
            .finish()
    }
}

impl EquivocationDetector {
    /// Create a detector.
    ///
    /// When `persist_path` is `Some` and the file exists, previously stored
    /// evidence and accused authorities are restored (a corrupt file is
    /// logged and ignored — detection restarts with an empty store). The
    /// observation index is intentionally volatile.
    pub fn new(persist_path: Option<PathBuf>) -> Self {
        let mut state = DetectorState::empty();
        if let Some(path) = &persist_path
            && path.exists()
        {
            match std::fs::read(path)
                .map_err(|e| e.to_string())
                .and_then(|b| {
                    serde_json::from_slice::<PersistedEvidence>(&b).map_err(|e| e.to_string())
                }) {
                Ok(persisted) => {
                    for node in persisted.accused {
                        state.accused.insert(node);
                    }
                    for ev in persisted.evidence {
                        state.accused.insert(ev.authority_id.clone());
                        state
                            .evidence
                            .entry(ev.authority_id.clone())
                            .or_default()
                            .push(ev);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "failed to restore equivocation evidence; starting empty"
                    );
                }
            }
        }
        Self {
            inner: Mutex::new(state),
            persist_path,
            #[cfg(feature = "native-runtime")]
            persist_gate: Mutex::new(()),
        }
    }

    /// Feed one attestation into the detector.
    ///
    /// **Precondition**: the `(frontier, sig)` pair must already have passed
    /// [`verify_frontier_signature`](crate::authority::frontier_sig::verify_frontier_signature)
    /// (or be this node's own signature). Unverified pairs would let an
    /// attacker frame an honest authority with forged evidence.
    pub fn observe(
        &self,
        frontier: &AckFrontier,
        sig: &FrontierSignature,
        now_ms: u64,
    ) -> ObserveOutcome {
        // Future-skew guard: far-future HLCs are not indexed, so they can
        // neither evict genuine heads nor produce (dubious) evidence.
        if frontier.frontier_hlc.physical > now_ms.saturating_add(MAX_FUTURE_SKEW_MS) {
            return ObserveOutcome::Skipped;
        }

        let scope = ObsScope::of(frontier);
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());

        // New scopes always get tracked: capacity pressure evicts the
        // least-recently-touched scope (per-authority fairness first, then
        // the global bound) instead of silently dropping the observation.
        if !state.observed.contains_key(&scope) {
            state.make_room_for_scope(&scope.authority_id);
        }
        state.touch_counter += 1;
        let touch = state.touch_counter;

        // Cloned lookup keeps the borrow short; the clone is needed for the
        // evidence pair anyway when a conflict is found.
        let existing = {
            let entry = state.observed.entry(scope.clone()).or_default();
            entry.last_touch = touch;
            entry.heads.get(&frontier.frontier_hlc).cloned()
        };
        match existing {
            None => {
                let entry = state
                    .observed
                    .get_mut(&scope)
                    .expect("scope entry inserted above");
                entry.heads.insert(
                    frontier.frontier_hlc.clone(),
                    ObservedAttestation {
                        frontier: frontier.clone(),
                        signature: sig.clone(),
                    },
                );
                // Evict the oldest head once over capacity; conflicts older
                // than this window can no longer be detected locally (the
                // documented ~2 minute detection window).
                while entry.heads.len() > MAX_OBSERVED_PER_SCOPE {
                    entry.heads.pop_first();
                }
                ObserveOutcome::FirstSeen
            }
            Some(existing) if existing.frontier.digest_hash == frontier.digest_hash => {
                ObserveOutcome::Consistent
            }
            Some(existing) => {
                // Conflicting digest for the exact same (scope, hlc). Only
                // an echo of a digest already stored in an evidence pair is
                // consistent; a *distinct* new digest is a further, separate
                // equivocation and must be detected as such.
                if state.evidence_digest_recorded(
                    &scope,
                    &frontier.frontier_hlc,
                    &frontier.digest_hash,
                ) {
                    // Already recorded — a gossip echo of a known conflict.
                    return ObserveOutcome::Consistent;
                }
                let evidence = EquivocationEvidence {
                    authority_id: frontier.authority_id.clone(),
                    key_range: frontier.key_range.clone(),
                    policy_version: frontier.policy_version,
                    frontier_hlc: frontier.frontier_hlc.clone(),
                    first: existing,
                    second: ObservedAttestation {
                        frontier: frontier.clone(),
                        signature: sig.clone(),
                    },
                    detected_at_ms: now_ms,
                };
                state.accused.insert(frontier.authority_id.clone());
                let entries = state
                    .evidence
                    .entry(frontier.authority_id.clone())
                    .or_default();
                if entries.len() < MAX_EVIDENCE_PER_AUTHORITY {
                    entries.push(evidence.clone());
                } else {
                    state.evidence_overflow_total += 1;
                }
                ObserveOutcome::Equivocation(Box::new(evidence))
            }
        }
    }

    /// True when the exact `(authority, scope, frontier_hlc, digest_hash)`
    /// is already indexed, or already stored as a half of an evidence pair.
    /// Used by the gossip receive path to skip signature verification for
    /// byte-equivalent echoes (CPU DoS mitigation) — including the eternally
    /// re-gossiped halves of recorded conflicts, which the observed index
    /// alone cannot represent (it holds one attestation per HLC).
    pub fn is_known_exact(&self, frontier: &AckFrontier) -> bool {
        let scope = ObsScope::of(frontier);
        let state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        state
            .observed
            .get(&scope)
            .and_then(|m| m.heads.get(&frontier.frontier_hlc))
            .is_some_and(|obs| obs.frontier.digest_hash == frontier.digest_hash)
            || state.evidence_digest_recorded(&scope, &frontier.frontier_hlc, &frontier.digest_hash)
    }

    /// True when at least one equivocation has been detected for `authority`.
    pub fn is_accused(&self, authority: &NodeId) -> bool {
        let state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        state.accused.contains(authority)
    }

    /// All accused authorities (sorted for deterministic output).
    pub fn accused(&self) -> Vec<NodeId> {
        let state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut nodes: Vec<NodeId> = state.accused.iter().cloned().collect();
        nodes.sort_by(|a, b| a.0.cmp(&b.0));
        nodes
    }

    /// Number of accused authorities.
    pub fn accused_count(&self) -> u64 {
        let state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        state.accused.len() as u64
    }

    /// All stored evidence entries.
    pub fn evidence(&self) -> Vec<EquivocationEvidence> {
        let state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut all: Vec<EquivocationEvidence> =
            state.evidence.values().flatten().cloned().collect();
        all.sort_by(|a, b| {
            (&a.authority_id.0, a.detected_at_ms).cmp(&(&b.authority_id.0, b.detected_at_ms))
        });
        all
    }

    /// Number of conflicts detected but not stored due to the per-authority
    /// evidence cap.
    pub fn evidence_overflow_total(&self) -> u64 {
        let state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        state.evidence_overflow_total
    }

    /// Build the split-view gossip sample attached to outgoing frontier
    /// pushes (CT-gossip Protocol 2 style).
    ///
    /// Stored evidence pairs lead the sample so a detected split view
    /// actively propagates to all peers — but they are capped to half the
    /// budget, so the (never-evicted) evidence store can never starve the
    /// plain observed heads that let peers cross-detect *future* split
    /// views. A rotating cursor guarantees every pair still propagates
    /// across successive samples when evidence exceeds its share. The rest
    /// of the budget is filled round-robin across scopes, newest head first
    /// — frontier HLCs are monotone append heads, so newer heads subsume
    /// older ones and the gossip state does not grow linearly. Any budget
    /// left after the heads is topped up with further evidence halves.
    pub fn gossip_summaries(&self, max: usize) -> Vec<ObservedAttestation> {
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let state = &mut *state;
        let mut out: Vec<ObservedAttestation> = Vec::new();
        let mut seen: HashSet<(NodeId, KeyRange, PolicyVersion, HlcTimestamp, String)> =
            HashSet::new();
        let mut push = |obs: &ObservedAttestation, out: &mut Vec<ObservedAttestation>| {
            let key = (
                obs.frontier.authority_id.clone(),
                obs.frontier.key_range.clone(),
                obs.frontier.policy_version,
                obs.frontier.frontier_hlc.clone(),
                obs.frontier.digest_hash.clone(),
            );
            if seen.insert(key) {
                out.push(obs.clone());
            }
        };

        // Deterministic evidence order so the rotating cursor is stable
        // across calls (the backing HashMap iteration order is not).
        let mut evidence: Vec<&EquivocationEvidence> = state.evidence.values().flatten().collect();
        evidence.sort_by(|a, b| {
            (
                &a.authority_id.0,
                &a.key_range.prefix,
                a.policy_version,
                &a.frontier_hlc,
                &a.second.frontier.digest_hash,
            )
                .cmp(&(
                    &b.authority_id.0,
                    &b.key_range.prefix,
                    b.policy_version,
                    &b.frontier_hlc,
                    &b.second.frontier.digest_hash,
                ))
        });

        // (a) Evidence pairs first, capped to half the budget, starting at
        // the rotating cursor. Pairs travel whole (both halves or neither).
        let evidence_budget = max / 2;
        if !evidence.is_empty() {
            let start = state.gossip_evidence_cursor % evidence.len();
            let mut used = 0;
            for i in 0..evidence.len() {
                if out.len() + 2 > evidence_budget {
                    break;
                }
                let ev = evidence[(start + i) % evidence.len()];
                push(&ev.first, &mut out);
                push(&ev.second, &mut out);
                used += 1;
            }
            state.gossip_evidence_cursor = (start + used) % evidence.len();
        }

        // (b) Round-robin across scopes, newest observation first.
        let mut cursors: Vec<_> = state
            .observed
            .values()
            .map(|m| m.heads.values().rev())
            .collect();
        while out.len() < max {
            let mut progressed = false;
            for cursor in cursors.iter_mut() {
                if out.len() >= max {
                    break;
                }
                if let Some(obs) = cursor.next() {
                    push(obs, &mut out);
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }

        // (c) Top up any remaining budget with evidence beyond the reserved
        // share (already-sampled halves are deduplicated by `seen`).
        for ev in &evidence {
            if out.len() >= max {
                break;
            }
            push(&ev.first, &mut out);
            if out.len() < max {
                push(&ev.second, &mut out);
            }
        }
        out
    }

    /// Serialize the evidence store for persistence.
    ///
    /// Returns `(path, json_bytes)` when a persist path is configured. The
    /// caller must perform the actual write *outside* any lock — prefer
    /// [`EquivocationDetector::spawn_persist`], which also serializes
    /// concurrent writers so an older snapshot can never overwrite a newer
    /// one. The store is bounded (authorities x
    /// `MAX_EVIDENCE_PER_AUTHORITY`), so a full rewrite per detection is
    /// fine.
    pub fn persist_payload(&self) -> Option<(PathBuf, Vec<u8>)> {
        let path = self.persist_path.clone()?;
        let persisted = {
            let state = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let mut accused: Vec<NodeId> = state.accused.iter().cloned().collect();
            accused.sort_by(|a, b| a.0.cmp(&b.0));
            PersistedEvidence {
                evidence: state.evidence.values().flatten().cloned().collect(),
                accused,
            }
        };
        match serde_json::to_vec_pretty(&persisted) {
            Ok(bytes) => Some((path, bytes)),
            Err(e) => {
                tracing::warn!(error = %e, "failed to serialize equivocation evidence");
                None
            }
        }
    }
}

#[cfg(feature = "native-runtime")]
impl EquivocationDetector {
    /// Persist the evidence store on a blocking thread (no-op without a
    /// configured persist path).
    ///
    /// The snapshot is taken *while holding* the internal writer gate, so
    /// concurrent detections cannot race their writes into an order where
    /// an older snapshot overwrites a newer one: every writer that acquires
    /// the gate re-serializes the then-current store before writing.
    pub fn spawn_persist(self: &std::sync::Arc<Self>) {
        if self.persist_path.is_none() {
            return;
        }
        let detector = std::sync::Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let _gate = detector
                .persist_gate
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some((path, bytes)) = detector.persist_payload()
                && let Err(e) = crate::ops::write_atomic(&path, &bytes)
            {
                tracing::warn!(error = %e, "failed to persist equivocation evidence");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::certificate::{EpochConfig, KeysetRegistry, KeysetVersion};
    use crate::authority::frontier_sig::{
        NodeSigner, create_frontier_report_message, verify_frontier_signature,
    };
    use ed25519_dalek::Verifier;

    const NOW_MS: u64 = 5_000;

    fn node(name: &str) -> NodeId {
        NodeId(name.into())
    }

    fn make_signer(name: &str, byte: u8) -> NodeSigner {
        let mut seed = [0u8; 32];
        seed[0] = byte;
        #[cfg(feature = "native-crypto")]
        return NodeSigner::from_seed(node(name), &seed, false);
        #[cfg(not(feature = "native-crypto"))]
        NodeSigner::from_seed(node(name), &seed)
    }

    fn make_frontier(
        authority: &str,
        prefix: &str,
        policy: u64,
        physical: u64,
        logical: u32,
        digest: &str,
    ) -> AckFrontier {
        AckFrontier {
            authority_id: node(authority),
            frontier_hlc: HlcTimestamp {
                physical,
                logical,
                node_id: authority.into(),
            },
            key_range: KeyRange {
                prefix: prefix.into(),
            },
            policy_version: PolicyVersion(policy),
            digest_hash: digest.into(),
        }
    }

    fn signed(signer: &NodeSigner, frontier: &AckFrontier) -> (AckFrontier, FrontierSignature) {
        (
            frontier.clone(),
            signer.sign_frontier(frontier, KeysetVersion(1)),
        )
    }

    #[test]
    fn detect_conflicting_digest_same_hlc() {
        let signer = make_signer("auth-1", 1);
        let det = EquivocationDetector::new(None);

        let f_a = make_frontier("auth-1", "user/", 1, 4_000, 3, "digest-a");
        let f_b = make_frontier("auth-1", "user/", 1, 4_000, 3, "digest-b");
        let (fa, sa) = signed(&signer, &f_a);
        let (fb, sb) = signed(&signer, &f_b);

        assert!(matches!(
            det.observe(&fa, &sa, NOW_MS),
            ObserveOutcome::FirstSeen
        ));
        let ObserveOutcome::Equivocation(ev) = det.observe(&fb, &sb, NOW_MS) else {
            panic!("conflicting digest must be detected");
        };

        // Evidence holds both raw signed pairs.
        assert_eq!(ev.authority_id, node("auth-1"));
        assert_eq!(ev.first.frontier.digest_hash, "digest-a");
        assert_eq!(ev.second.frontier.digest_hash, "digest-b");
        assert_eq!(ev.frontier_hlc, f_a.frontier_hlc);

        // Non-repudiability: both report signatures re-verify against the
        // signer's public key over the canonical report message.
        for obs in [&ev.first, &ev.second] {
            let msg = create_frontier_report_message(&obs.frontier);
            let sig_bytes: [u8; 64] = hex::decode(&obs.signature.report_signature)
                .unwrap()
                .try_into()
                .unwrap();
            let sig = ed25519_dalek::Signature::from_bytes(&sig_bytes);
            signer
                .verifying_key()
                .verify(&msg, &sig)
                .expect("evidence report signature must verify");
        }

        // Evidence also passes the full registry verification path.
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![(node("auth-1"), signer.verifying_key())],
            )
            .unwrap();
        for obs in [&ev.first, &ev.second] {
            verify_frontier_signature(
                &obs.frontier,
                &obs.signature,
                &registry,
                0,
                &EpochConfig::default(),
            )
            .expect("evidence must remain verifiable end-to-end");
        }

        assert!(det.is_accused(&node("auth-1")));
        assert_eq!(det.accused(), vec![node("auth-1")]);
        assert_eq!(det.evidence().len(), 1);
    }

    #[test]
    fn same_digest_resubmit_is_consistent() {
        let signer = make_signer("auth-1", 2);
        let det = EquivocationDetector::new(None);
        let f = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-a");
        let (f, s) = signed(&signer, &f);

        assert!(matches!(
            det.observe(&f, &s, NOW_MS),
            ObserveOutcome::FirstSeen
        ));
        // Legitimate resend / gossip echo.
        assert!(matches!(
            det.observe(&f, &s, NOW_MS),
            ObserveOutcome::Consistent
        ));
        assert!(det.evidence().is_empty());
        assert!(!det.is_accused(&node("auth-1")));
    }

    #[test]
    fn no_false_positive_hlc_advance_within_checkpoint() {
        let signer = make_signer("auth-1", 3);
        let det = EquivocationDetector::new(None);

        // Same checkpoint bucket (physical 4000..4999), advancing HLC with a
        // changing digest — the legitimate-progress case fixed by
        // attestation_pool::duplicate_authority_counted_once.
        let f1 = make_frontier("auth-1", "user/", 1, 4_100, 0, "digest-1");
        let f2 = make_frontier("auth-1", "user/", 1, 4_100, 1, "digest-2");
        let f3 = make_frontier("auth-1", "user/", 1, 4_900, 0, "digest-3");
        for f in [&f1, &f2, &f3] {
            let (f, s) = signed(&signer, f);
            assert!(matches!(
                det.observe(&f, &s, NOW_MS),
                ObserveOutcome::FirstSeen
            ));
        }

        // Different policy_version / key_range are separate scopes even at
        // the same HLC, so a differing digest is not a conflict.
        let base = make_frontier("auth-1", "user/", 1, 4_100, 0, "digest-1");
        let other_policy = make_frontier("auth-1", "user/", 2, 4_100, 0, "digest-x");
        let other_range = make_frontier("auth-1", "order/", 1, 4_100, 0, "digest-y");
        let _ = base;
        for f in [&other_policy, &other_range] {
            let (f, s) = signed(&signer, f);
            assert!(matches!(
                det.observe(&f, &s, NOW_MS),
                ObserveOutcome::FirstSeen
            ));
        }

        assert!(det.evidence().is_empty(), "no false positives");
        assert!(!det.is_accused(&node("auth-1")));
    }

    #[test]
    fn duplicate_evidence_deduped() {
        let signer = make_signer("auth-1", 4);
        let det = EquivocationDetector::new(None);
        let f_a = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-a");
        let f_b = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-b");
        let (fa, sa) = signed(&signer, &f_a);
        let (fb, sb) = signed(&signer, &f_b);

        det.observe(&fa, &sa, NOW_MS);
        assert!(matches!(
            det.observe(&fb, &sb, NOW_MS),
            ObserveOutcome::Equivocation(_)
        ));
        // Re-observing the same conflict (gossip echo) must not double-count.
        assert!(matches!(
            det.observe(&fb, &sb, NOW_MS),
            ObserveOutcome::Consistent
        ));
        assert_eq!(det.evidence().len(), 1);
    }

    #[test]
    fn caps_and_eviction() {
        let signer = make_signer("auth-1", 5);
        let det = EquivocationDetector::new(None);

        // Fill one scope beyond capacity: oldest HLC gets evicted.
        for i in 0..(MAX_OBSERVED_PER_SCOPE as u64 + 1) {
            let f = make_frontier("auth-1", "user/", 1, 1_000 + i, 0, &format!("d{i}"));
            let (f, s) = signed(&signer, &f);
            assert!(matches!(
                det.observe(&f, &s, NOW_MS),
                ObserveOutcome::FirstSeen
            ));
        }
        // The evicted (oldest) head re-observes as FirstSeen, not Consistent.
        let oldest = make_frontier("auth-1", "user/", 1, 1_000, 0, "d0");
        assert!(!det.is_known_exact(&oldest));

        // Future-skew guard.
        let future = make_frontier(
            "auth-1",
            "user/",
            1,
            NOW_MS + MAX_FUTURE_SKEW_MS + 1,
            0,
            "f",
        );
        let (f, s) = signed(&signer, &future);
        assert!(matches!(
            det.observe(&f, &s, NOW_MS),
            ObserveOutcome::Skipped
        ));

        // Per-authority scope cap: one authority beyond its fairness share
        // evicts its *own* least-recently-touched scope — the new scope is
        // always tracked (never silently skipped).
        let det2 = EquivocationDetector::new(None);
        for i in 0..MAX_TRACKED_SCOPES_PER_AUTHORITY {
            let f = make_frontier("auth-1", &format!("p{i}/"), 1, 2_000, 0, "d");
            let (f, s) = signed(&signer, &f);
            assert!(matches!(
                det2.observe(&f, &s, NOW_MS),
                ObserveOutcome::FirstSeen
            ));
        }
        let f = make_frontier("auth-1", "overflow/", 1, 2_000, 0, "d");
        let (f, s) = signed(&signer, &f);
        assert!(
            matches!(det2.observe(&f, &s, NOW_MS), ObserveOutcome::FirstSeen),
            "a new scope must be tracked by evicting, not skipped"
        );
        // The authority's own oldest scope was evicted...
        assert!(!det2.is_known_exact(&make_frontier("auth-1", "p0/", 1, 2_000, 0, "d")));
        // ...while its newest scope and the overflow scope stay indexed.
        let newest = format!("p{}/", MAX_TRACKED_SCOPES_PER_AUTHORITY - 1);
        assert!(det2.is_known_exact(&make_frontier("auth-1", &newest, 1, 2_000, 0, "d")));
        assert!(det2.is_known_exact(&f));

        // Evidence cap: beyond MAX_EVIDENCE_PER_AUTHORITY the conflict is
        // still reported (and accusation stands) but only the overflow
        // counter grows; stored evidence is never evicted.
        let det3 = EquivocationDetector::new(None);
        for i in 0..(MAX_EVIDENCE_PER_AUTHORITY as u64 + 2) {
            let f_a = make_frontier("auth-1", "user/", 1, 3_000 + i, 0, "a");
            let f_b = make_frontier("auth-1", "user/", 1, 3_000 + i, 0, "b");
            let (fa, sa) = signed(&signer, &f_a);
            let (fb, sb) = signed(&signer, &f_b);
            det3.observe(&fa, &sa, NOW_MS);
            assert!(matches!(
                det3.observe(&fb, &sb, NOW_MS),
                ObserveOutcome::Equivocation(_)
            ));
        }
        assert_eq!(det3.evidence().len(), MAX_EVIDENCE_PER_AUTHORITY);
        assert_eq!(det3.evidence_overflow_total(), 2);
        assert!(det3.is_accused(&node("auth-1")));
        // The earliest evidence is retained.
        assert!(
            det3.evidence()
                .iter()
                .any(|ev| ev.frontier_hlc.physical == 3_000)
        );
    }

    #[test]
    fn gossip_summaries_prioritize_evidence_and_cap() {
        let signer = make_signer("auth-1", 6);
        let det = EquivocationDetector::new(None);

        // One conflict -> two evidence halves.
        let f_a = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-a");
        let f_b = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-b");
        let (fa, sa) = signed(&signer, &f_a);
        let (fb, sb) = signed(&signer, &f_b);
        det.observe(&fa, &sa, NOW_MS);
        det.observe(&fb, &sb, NOW_MS);

        // Fill two more scopes with plain observations.
        for i in 0..5u64 {
            let f = make_frontier("auth-1", "order/", 1, 3_000 + i, 0, &format!("o{i}"));
            let (f, s) = signed(&signer, &f);
            det.observe(&f, &s, NOW_MS);
            let f = make_frontier("auth-1", "item/", 1, 3_000 + i, 0, &format!("i{i}"));
            let (f, s) = signed(&signer, &f);
            det.observe(&f, &s, NOW_MS);
        }

        let sample = det.gossip_summaries(4);
        assert_eq!(sample.len(), 4, "capped at max");
        // Both evidence halves lead the sample.
        let digests: Vec<&str> = sample
            .iter()
            .take(2)
            .map(|o| o.frontier.digest_hash.as_str())
            .collect();
        assert!(digests.contains(&"digest-a"));
        assert!(digests.contains(&"digest-b"));
        // The remaining budget carries the newest heads of other scopes.
        assert!(
            sample[2..]
                .iter()
                .all(|o| o.frontier.frontier_hlc.physical == 3_004)
        );

        // Large budget: no duplicates.
        let all = det.gossip_summaries(1_000);
        let mut keys: Vec<String> = all
            .iter()
            .map(|o| {
                format!(
                    "{}|{}|{}|{}",
                    o.frontier.key_range.prefix,
                    o.frontier.frontier_hlc.physical,
                    o.frontier.frontier_hlc.logical,
                    o.frontier.digest_hash
                )
            })
            .collect();
        let before = keys.len();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), before, "gossip sample must not duplicate");
    }

    #[test]
    fn persist_roundtrip_and_corrupt_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("equivocation_evidence.json");

        let signer = make_signer("auth-1", 7);
        let det = EquivocationDetector::new(Some(path.clone()));
        let f_a = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-a");
        let f_b = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-b");
        let (fa, sa) = signed(&signer, &f_a);
        let (fb, sb) = signed(&signer, &f_b);
        det.observe(&fa, &sa, NOW_MS);
        assert!(matches!(
            det.observe(&fb, &sb, NOW_MS),
            ObserveOutcome::Equivocation(_)
        ));

        let (out_path, bytes) = det.persist_payload().expect("persist path configured");
        assert_eq!(out_path, path);
        std::fs::write(&out_path, &bytes).unwrap();

        // Restore: accusations and evidence survive a restart.
        let restored = EquivocationDetector::new(Some(path.clone()));
        assert!(restored.is_accused(&node("auth-1")));
        let evidence = restored.evidence();
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].first.frontier.digest_hash, "digest-a");
        assert_eq!(evidence[0].second.frontier.digest_hash, "digest-b");

        // A corrupt file logs a warning and starts empty.
        std::fs::write(&path, b"{not json").unwrap();
        let corrupt = EquivocationDetector::new(Some(path));
        assert!(corrupt.evidence().is_empty());
        assert_eq!(corrupt.accused_count(), 0);
    }

    #[test]
    fn detector_without_persist_path_returns_no_payload() {
        let det = EquivocationDetector::new(None);
        assert!(det.persist_payload().is_none());
    }

    #[test]
    fn scope_flood_does_not_blind_other_authorities() {
        let attacker = make_signer("evil", 8);
        let honest = make_signer("auth-1", 9);
        let det = EquivocationDetector::new(None);

        // A single compromised authority floods more distinct scopes than
        // the whole index could previously hold (attacker-chosen key ranges
        // under its own valid signature).
        for i in 0..(MAX_TRACKED_SCOPES + 8) {
            let f = make_frontier("evil", &format!("flood{i}/"), 1, 2_000, 0, "d");
            let (f, s) = signed(&attacker, &f);
            assert!(matches!(
                det.observe(&f, &s, NOW_MS),
                ObserveOutcome::FirstSeen
            ));
        }

        // The flood only recycles the attacker's own fairness share, so a
        // *subsequent* equivocation by another authority in a brand-new
        // scope is still tracked and detected.
        let f_a = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-a");
        let f_b = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-b");
        let (fa, sa) = signed(&honest, &f_a);
        let (fb, sb) = signed(&honest, &f_b);
        assert!(matches!(
            det.observe(&fa, &sa, NOW_MS),
            ObserveOutcome::FirstSeen
        ));
        assert!(matches!(
            det.observe(&fb, &sb, NOW_MS),
            ObserveOutcome::Equivocation(_)
        ));
        assert!(det.is_accused(&node("auth-1")));
        assert!(!det.is_accused(&node("evil")));
    }

    #[test]
    fn global_scope_cap_evicts_lru_instead_of_skipping() {
        // 17 authorities x the per-authority share exceeds the global cap,
        // so the globally least-recently-touched scopes get evicted.
        let signer = make_signer("any", 10);
        let det = EquivocationDetector::new(None);
        const { assert!(17 * MAX_TRACKED_SCOPES_PER_AUTHORITY > MAX_TRACKED_SCOPES) };
        for a in 0..17 {
            for i in 0..MAX_TRACKED_SCOPES_PER_AUTHORITY {
                let f = make_frontier(&format!("auth-{a}"), &format!("p{i}/"), 1, 2_000, 0, "d");
                let (f, s) = signed(&signer, &f);
                assert!(matches!(
                    det.observe(&f, &s, NOW_MS),
                    ObserveOutcome::FirstSeen
                ));
            }
        }
        // The globally oldest scope was evicted to make room.
        assert!(!det.is_known_exact(&make_frontier("auth-0", "p0/", 1, 2_000, 0, "d")));

        // A brand-new authority's scope is still tracked at the full index,
        // and equivocation detection keeps working inside it.
        let f_a = make_frontier("auth-new", "fresh/", 1, 4_000, 0, "digest-a");
        let f_b = make_frontier("auth-new", "fresh/", 1, 4_000, 0, "digest-b");
        let (fa, sa) = signed(&signer, &f_a);
        let (fb, sb) = signed(&signer, &f_b);
        assert!(matches!(
            det.observe(&fa, &sa, NOW_MS),
            ObserveOutcome::FirstSeen
        ));
        assert!(matches!(
            det.observe(&fb, &sb, NOW_MS),
            ObserveOutcome::Equivocation(_)
        ));
    }

    #[test]
    fn third_distinct_digest_is_a_new_equivocation() {
        let signer = make_signer("auth-1", 12);
        let det = EquivocationDetector::new(None);
        let f_a = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-a");
        let f_b = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-b");
        let f_c = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-c");
        let (fa, sa) = signed(&signer, &f_a);
        let (fb, sb) = signed(&signer, &f_b);
        let (fc, sc) = signed(&signer, &f_c);

        det.observe(&fa, &sa, NOW_MS);
        assert!(matches!(
            det.observe(&fb, &sb, NOW_MS),
            ObserveOutcome::Equivocation(_)
        ));

        // Both recorded halves are now "known exact", so gossip echoes skip
        // signature re-verification — including the non-indexed second half.
        assert!(det.is_known_exact(&f_a));
        assert!(det.is_known_exact(&f_b));
        assert!(matches!(
            det.observe(&fb, &sb, NOW_MS),
            ObserveOutcome::Consistent
        ));

        // A genuinely distinct third digest at the same (scope, hlc) is NOT
        // an echo: it must be detected and recorded as further evidence.
        let ObserveOutcome::Equivocation(ev) = det.observe(&fc, &sc, NOW_MS) else {
            panic!("a third distinct digest is a new equivocation");
        };
        assert_eq!(ev.second.frontier.digest_hash, "digest-c");
        assert_eq!(det.evidence().len(), 2);

        // And once recorded, its own echoes are consistent.
        assert!(det.is_known_exact(&f_c));
        assert!(matches!(
            det.observe(&fc, &sc, NOW_MS),
            ObserveOutcome::Consistent
        ));
    }

    #[test]
    fn gossip_sample_reserves_budget_for_observed_heads() {
        let det = EquivocationDetector::new(None);

        // Two colluding authorities at the evidence cap: 32 stored pairs =
        // 64 halves, enough to consume the whole GOSSIP_SAMPLE_MAX budget
        // under the old evidence-first scheme.
        for (name, byte) in [("evil-1", 13u8), ("evil-2", 14u8)] {
            let signer = make_signer(name, byte);
            for i in 0..(MAX_EVIDENCE_PER_AUTHORITY as u64) {
                let f_a = make_frontier(name, "user/", 1, 3_000 + i, 0, "a");
                let f_b = make_frontier(name, "user/", 1, 3_000 + i, 0, "b");
                let (fa, sa) = signed(&signer, &f_a);
                let (fb, sb) = signed(&signer, &f_b);
                det.observe(&fa, &sa, NOW_MS);
                assert!(matches!(
                    det.observe(&fb, &sb, NOW_MS),
                    ObserveOutcome::Equivocation(_)
                ));
            }
        }
        assert_eq!(det.evidence().len(), 2 * MAX_EVIDENCE_PER_AUTHORITY);

        // A third authority's plain observation must still ride the gossip
        // lane — it is the only way peers can cross-detect a *future* split
        // view involving that authority.
        let signer3 = make_signer("auth-3", 15);
        let f = make_frontier("auth-3", "user/", 1, 4_000, 0, "head");
        let (f, s) = signed(&signer3, &f);
        det.observe(&f, &s, NOW_MS);

        let sample = det.gossip_summaries(GOSSIP_SAMPLE_MAX);
        assert!(sample.len() <= GOSSIP_SAMPLE_MAX);
        assert!(
            sample
                .iter()
                .any(|o| o.frontier.authority_id == node("auth-3")),
            "observed heads must never be starved out by stored evidence"
        );

        // The rotating cursor covers every stored pair across successive
        // samples even though only half the budget is reserved for them.
        // The "b" halves are never in the observed index, so seeing them
        // all proves the *evidence* share itself rotated.
        let mut covered: HashSet<(NodeId, u64)> = HashSet::new();
        for _ in 0..4 {
            for obs in det.gossip_summaries(GOSSIP_SAMPLE_MAX) {
                if obs.frontier.digest_hash == "b" {
                    covered.insert((
                        obs.frontier.authority_id.clone(),
                        obs.frontier.frontier_hlc.physical,
                    ));
                }
            }
        }
        assert_eq!(
            covered.len(),
            2 * MAX_EVIDENCE_PER_AUTHORITY,
            "every evidence pair must propagate across successive samples"
        );
    }

    #[cfg(feature = "native-runtime")]
    #[tokio::test]
    async fn spawn_persist_writes_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("equivocation_evidence.json");

        let signer = make_signer("auth-1", 16);
        let det = std::sync::Arc::new(EquivocationDetector::new(Some(path.clone())));
        let f_a = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-a");
        let f_b = make_frontier("auth-1", "user/", 1, 4_000, 0, "digest-b");
        let (fa, sa) = signed(&signer, &f_a);
        let (fb, sb) = signed(&signer, &f_b);
        det.observe(&fa, &sa, NOW_MS);
        assert!(matches!(
            det.observe(&fb, &sb, NOW_MS),
            ObserveOutcome::Equivocation(_)
        ));

        det.spawn_persist();
        let mut persisted = false;
        for _ in 0..200 {
            if path.exists() {
                persisted = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(persisted, "spawn_persist must write the evidence file");

        let restored = EquivocationDetector::new(Some(path));
        assert!(restored.is_accused(&node("auth-1")));
        assert_eq!(restored.evidence().len(), 1);
    }
}
