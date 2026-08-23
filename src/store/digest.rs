//! Two-level key-range digest for digest-based anti-entropy.
//!
//! When delta sync cannot be used (high change rate, pruned change log,
//! decode failures, reconnect after a long partition), the legacy fallback
//! is a full key dump. The digest protocol avoids that: replicas exchange
//! a fixed-depth, two-level digest of their CRDT state — one root digest
//! plus up to [`DIGEST_BUCKET_COUNT`] bucket digests — and transfer only
//! the keys in mismatched buckets. A root match completes with zero data
//! transfer.
//!
//! Layout (scheme version 2):
//! - per-key digest: `D(k) = SHA256( str(k) ‖ canonical CRDT stream )`
//!   (see [`CrdtValue::canonical_digest_into`])
//! - bucket assignment: `bucket(k) = SHA256(k)[0]` — deterministic and
//!   replica-independent, never dependent on insertion order
//! - bucket digest: `B_i = SHA256( D(k_1) ‖ D(k_2) ‖ … )` with the keys of
//!   bucket `i` in lexicographic (byte) order; an empty bucket is all-zero
//! - root digest: `root = SHA256( B_0 ‖ B_1 ‖ … ‖ B_255 )`
//!
//! Because the per-bucket key order is a subsequence of the global
//! lexicographic order, one in-order pass over the store's `BTreeMap`
//! computes every bucket. Identical key sets with identical CANONICAL
//! CRDT states produce identical digests on every replica — the property
//! the whole protocol rests on ("digest matched" ⟺ canonical state
//! equality up to SHA-256 collisions), and what makes adopting the
//! sender's session claims on a match as sound as after a full dump.
//!
//! The digest deliberately EXCLUDES `Store::timestamps` (per-key HLCs):
//! push-path merges re-stamp entries with a local clock tick and pruning
//! removes entries one-sidedly, so per-key HLCs never converge across
//! replicas and would cause permanent false mismatches.
//!
//! Scheme v2 (M-8): `OrSet`/`OrMap` streams now include the per-value
//! `compaction_floor` and EXCLUDE deferred (tombstone) dots covered by
//! it (the canonical form). A covered own tombstone — a fresh remove
//! below the floor, retained only for its origin's gated sweep — is
//! information-equivalent to "floor + absence", so including it would
//! cause a false mismatch on every remove until the origin sweeps.
//! Under v1 the situation was worse than a false mismatch: the sweep
//! physically deleted tombstones, the resulting genuine mismatch caused
//! a bucket transfer, and the old union-merge re-adopted the peer's
//! stale tombstones — rolling GC back on every exchange, so tombstone GC
//! never converged cluster-wide under sustained digest fallback (the M-8
//! livelock). In v2 the same transfer propagates the floor instead and
//! the asymmetry heals in one round trip.

use std::collections::{BTreeMap, HashSet};

use sha2::{Digest, Sha256};

use crate::crdt::digest::write_str;
use crate::store::kv::CrdtValue;

/// Version of the digest wire scheme (bucket layout + canonical CRDT
/// streams). Bump on ANY change to the canonical encoding — peers with a
/// different version answer `scheme_ok = false` and the requester falls
/// back to the legacy full sync (rolling-upgrade safe).
///
/// v2: `OrSet`/`OrMap` streams include the `compaction_floor` and
/// exclude floor-covered deferred dots (M-8 canonical form).
pub const DIGEST_SCHEME_VERSION: u32 = 2;

/// Number of key-range buckets (fixed; part of the wire scheme).
pub const DIGEST_BUCKET_COUNT: usize = 256;

/// Byte length of every digest on the wire (SHA-256).
pub const DIGEST_LEN: usize = 32;

/// Digest of an empty bucket (all zeroes; never sent on the wire —
/// absence from the sparse bucket list means "empty").
pub const EMPTY_BUCKET_DIGEST: [u8; DIGEST_LEN] = [0u8; DIGEST_LEN];

/// Deterministic bucket assignment for a key: first byte of `SHA256(key)`.
///
/// Replica-independent and insertion-order-independent by construction.
/// Note this is unrelated to the `BTreeMap` sort order — bucket membership
/// is scattered across the key space, which keeps buckets balanced without
/// requiring replicas to agree on range boundaries.
pub fn bucket_of(key: &str) -> usize {
    Sha256::digest(key.as_bytes())[0] as usize
}

/// Two-level digest of a store's CRDT state at one point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreDigest {
    /// `SHA256(B_0 ‖ … ‖ B_255)`.
    pub root: [u8; DIGEST_LEN],
    /// Per-bucket digests; [`EMPTY_BUCKET_DIGEST`] for empty buckets.
    pub buckets: [[u8; DIGEST_LEN]; DIGEST_BUCKET_COUNT],
    /// Number of keys in each bucket (local bookkeeping / metrics).
    pub key_counts: [u32; DIGEST_BUCKET_COUNT],
    /// Total number of keys digested.
    pub total_keys: u64,
}

impl StoreDigest {
    /// Iterate the non-empty buckets as `(index, digest)` pairs — the
    /// sparse form sent on the wire.
    pub fn non_empty_buckets(&self) -> impl Iterator<Item = (u16, &[u8; DIGEST_LEN])> {
        self.buckets
            .iter()
            .enumerate()
            .filter(|(_, digest)| **digest != EMPTY_BUCKET_DIGEST)
            .map(|(i, digest)| (i as u16, digest))
    }
}

/// Per-key digest `D(k) = SHA256( str(k) ‖ canonical CRDT stream )`.
///
/// This is THE per-key hash of the wire scheme — [`digest_pass`] (full
/// pass) and [`DigestCache`] (incremental refresh) both call it, so the
/// cached and the from-scratch digests are computed by the same code
/// path by construction.
pub(crate) fn key_digest(key: &str, value: &CrdtValue) -> [u8; DIGEST_LEN] {
    let mut key_hasher = Sha256::new();
    write_str(&mut key_hasher, key);
    value.canonical_digest_into(&mut key_hasher);
    key_hasher.finalize().into()
}

/// One full in-order digest pass over `entries`, notifying `per_key` with
/// `(key, bucket, per-key digest)` for every entry (in lexicographic key
/// order — the scheme's per-bucket order). The sink lets callers build
/// side tables (per-key digest cache, bucket index) in the SAME pass that
/// produces the wire digest, so they can never drift from it.
pub fn digest_pass(
    entries: &BTreeMap<String, CrdtValue>,
    mut per_key: impl FnMut(&str, u8, &[u8; DIGEST_LEN]),
) -> StoreDigest {
    let mut hashers: Vec<Option<Sha256>> = (0..DIGEST_BUCKET_COUNT).map(|_| None).collect();
    let mut key_counts = [0u32; DIGEST_BUCKET_COUNT];
    let mut total_keys = 0u64;

    for (key, value) in entries {
        let kd = key_digest(key, value);
        let bucket = bucket_of(key);
        hashers[bucket].get_or_insert_with(Sha256::new).update(kd);
        key_counts[bucket] += 1;
        total_keys += 1;
        per_key(key, bucket as u8, &kd);
    }

    let mut buckets = [EMPTY_BUCKET_DIGEST; DIGEST_BUCKET_COUNT];
    let mut root_hasher = Sha256::new();
    for (i, hasher) in hashers.into_iter().enumerate() {
        if let Some(h) = hasher {
            buckets[i] = h.finalize().into();
        }
        root_hasher.update(buckets[i]);
    }

    StoreDigest {
        root: root_hasher.finalize().into(),
        buckets,
        key_counts,
        total_keys,
    }
}

/// Compute the two-level digest of a store snapshot.
///
/// `entries` must be the store's data map (a `BTreeMap`, i.e. already in
/// lexicographic key order). One in-order pass feeds each per-key digest
/// into its bucket's hasher; the per-bucket order is thereby the
/// lexicographic key order required by the scheme.
///
/// Cost is O(total CRDT state size) — callers on the sync path snapshot
/// the map under the store lock, release it, and run this inside
/// `spawn_blocking`. The warm path ([`Store::digest`](crate::store::kv::Store::digest))
/// avoids both the snapshot and the full pass via [`DigestCache`].
pub fn compute_store_digest(entries: &BTreeMap<String, CrdtValue>) -> StoreDigest {
    digest_pass(entries, |_, _, _| {})
}

/// Upper bound on the number of dirty keys [`DigestCache::refresh`] will
/// re-hash INLINE (i.e. while the caller holds the store lock). Above
/// this, [`DigestCache::is_cold`] reports `true` and callers should run
/// the off-lock warm-up protocol (`ensure_digest_warm`) or fall back to
/// the legacy snapshot path. Note the inline work hashes only the DIRTY
/// keys' values — always ≤ the full-store clone+hash the legacy path
/// performs under the same lock.
pub const REFRESH_INLINE_MAX: usize = 512;

/// Upper bound on the tracked dirty-key set. When a mutation touches a
/// NEW key while the set is already this large, the cache collapses to
/// `invalid` (dropping all per-key state) instead of growing further —
/// see [`DigestCache::note_dirty`]. Bounds the cache's residency for
/// stores whose digest is never refreshed.
pub const DIRTY_COLLAPSE_MAX: usize = 1 << 16;

/// Cached per-key digest metadata: the key's (immutable) bucket and its
/// last-computed per-key digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyMeta {
    /// `bucket_of(key)` — a pure function of the key, cached to avoid
    /// re-hashing the key on every bucket rebuild / bucket filter.
    pub bucket: u8,
    /// `key_digest(key, value)` as of the last refresh.
    pub digest: [u8; DIGEST_LEN],
}

/// Off-lock warm-up work extracted from a cold [`DigestCache`]
/// (see [`DigestCache::cold_work`]): either a full snapshot to rebuild
/// from scratch, or just the dirty keys' current values.
pub enum DigestColdWork {
    /// Cache is invalid (deserialized / migrated store): full rebuild.
    Full(BTreeMap<String, CrdtValue>),
    /// Cache meta is valid but too many keys are dirty for an inline
    /// refresh: re-hash only the dirty keys (`None` value = key absent).
    Dirty(Vec<(String, Option<CrdtValue>)>),
}

/// The hashed results of a [`DigestColdWork`], ready for adoption under
/// the store lock (see [`DigestCache::adopt`]).
pub enum DigestColdResults {
    /// Full rebuild output: the wire digest plus the complete meta table
    /// (boxed: `StoreDigest` is ~9 KiB and would dominate the enum).
    Full {
        digest: Box<StoreDigest>,
        key_meta: BTreeMap<String, KeyMeta>,
    },
    /// Per-dirty-key meta (`None` = key absent → remove from the table).
    Dirty(Vec<(String, Option<KeyMeta>)>),
}

impl DigestColdWork {
    /// Perform the CPU-heavy hashing OFF the store lock (callers wrap
    /// this in `spawn_blocking`). Pure function of the captured work.
    pub fn compute(self) -> DigestColdResults {
        match self {
            DigestColdWork::Full(data) => {
                let mut key_meta = BTreeMap::new();
                let digest = digest_pass(&data, |key, bucket, kd| {
                    key_meta.insert(
                        key.to_string(),
                        KeyMeta {
                            bucket,
                            digest: *kd,
                        },
                    );
                });
                DigestColdResults::Full {
                    digest: Box::new(digest),
                    key_meta,
                }
            }
            DigestColdWork::Dirty(items) => DigestColdResults::Dirty(
                items
                    .into_iter()
                    .map(|(key, value)| {
                        let meta = value.map(|v| KeyMeta {
                            bucket: bucket_of(&key) as u8,
                            digest: key_digest(&key, &v),
                        });
                        (key, meta)
                    })
                    .collect(),
            ),
        }
    }
}

/// Incremental two-level digest cache, embedded in
/// [`Store`](crate::store::kv::Store) (M-7).
///
/// # Invariants (kept by `Store`, verified by the debug oracle below)
/// - INV-1 (coverage): every mutation of `Store::data` calls
///   [`note_dirty`](Self::note_dirty) for the touched key in the same
///   `&mut Store` call.
/// - INV-2 (generation): `note_dirty` ALWAYS bumps `generation`, so
///   "generation unchanged between two lock scopes" ⟺ "`data` physically
///   unchanged in between" ⟹ the digest is unchanged (the digest is a
///   pure function of `data`; `timestamps` and session metadata are not
///   digest inputs). `u64` wrapping would need 2^64 mutations to alias.
/// - INV-3 (meta freshness): when `!invalid`, every non-dirty key's
///   `key_meta` entry equals `(bucket_of(k), key_digest(k, data[k]))`
///   and `key_meta`'s key set equals `data`'s key set (modulo dirty
///   keys).
/// - INV-5 (per-key all-or-nothing): off-lock results
///   ([`DigestColdResults`]) are adopted only when either (a) the
///   generation observed at capture time still holds at adoption time
///   (data physically unchanged — adopt everything), or (b) the capture
///   window opened by [`cold_work`](Self::cold_work) tracked EVERY key
///   mutated since the capture, in which case the capture-time results
///   are adopted and exactly those window keys stay dirty. Either way
///   every non-dirty key's meta is fresh (INV-3), so a stale hash can
///   never be SERVED from the meta table; an overflowed or superseded
///   window discards the results WHOLE. There is no other
///   partial-adoption API.
///
/// Over-invalidation (e.g. `Store::get_mut` on a caller that never
/// writes) is safe: the dirty key is simply re-hashed to the same value.
/// The refreshed digest is therefore always bit-identical to
/// `compute_store_digest(&data)` — debug builds assert exactly that on
/// every refresh.
#[derive(Debug, Clone)]
pub struct DigestCache {
    /// Per-key bucket + last digest. Key set mirrors `Store::data` when
    /// clean (INV-3).
    key_meta: BTreeMap<String, KeyMeta>,
    /// Keys whose meta may be stale (mutated since the last refresh).
    dirty: HashSet<String>,
    /// Mutation epoch: bumped on EVERY `note_dirty`, even for
    /// already-dirty keys (INV-2).
    generation: u64,
    /// True when `key_meta` says nothing about `data` (deserialized /
    /// migrated store): only a full rebuild may clear it.
    invalid: bool,
    /// The last refreshed wire digest (`None` until the first rebuild).
    cached: Option<StoreDigest>,
    /// In-flight off-lock capture window (INV-5b): opened by
    /// [`cold_work`](Self::cold_work), it records every key mutated
    /// since the capture — even while `invalid`, when the main dirty
    /// set is not tracked — so [`adopt`](Self::adopt) can accept the
    /// capture-time results under concurrent writes, leaving exactly
    /// the window keys dirty. Without it, a large store under
    /// sustained writes could never warm up (the full off-lock hash
    /// takes longer than the write inter-arrival time), permanently
    /// paying warm-up attempts ON TOP of the legacy path.
    capture_window: Option<CaptureWindow>,
}

/// See [`DigestCache::capture_window`].
#[derive(Debug, Clone)]
struct CaptureWindow {
    /// `generation` at capture time: adoption requires the results to
    /// have been captured at exactly this epoch (a second capture
    /// supersedes the window, so stale results can never pair with a
    /// younger window).
    at_generation: u64,
    /// Keys mutated since the capture.
    dirtied: HashSet<String>,
    /// Set when the window exceeded [`DIRTY_COLLAPSE_MAX`] keys: the
    /// tracking is no longer complete, so adoption under a moved
    /// generation is forfeited (fail-safe; the memory is freed).
    overflowed: bool,
}

impl Default for DigestCache {
    /// The DESERIALIZATION default (`#[serde(skip)]` on the `Store`
    /// field): the data map arrived wholesale, so the cache knows
    /// nothing — `invalid` forces a full rebuild. Freshly constructed
    /// empty stores use [`DigestCache::warm_empty`] instead.
    fn default() -> Self {
        Self {
            key_meta: BTreeMap::new(),
            dirty: HashSet::new(),
            generation: 0,
            invalid: true,
            cached: None,
            capture_window: None,
        }
    }
}

impl DigestCache {
    /// Cache for a brand-new EMPTY store: valid by construction (there
    /// is nothing the meta table could be stale about), so stores built
    /// purely through the mutation API stay warm from birth.
    pub fn warm_empty() -> Self {
        Self {
            invalid: false,
            ..Self::default()
        }
    }

    /// Record that `key`'s value may have changed (INV-1) and bump the
    /// mutation epoch (INV-2, unconditionally — the epoch is what makes
    /// off-lock work verifiable, so it must move even for keys that are
    /// already dirty).
    pub fn note_dirty(&mut self, key: &str) {
        self.generation = self.generation.wrapping_add(1);
        // Capture-window tracking (INV-5b): while off-lock warm-up work
        // is in flight, record every mutated key — even while `invalid`
        // — so the capture-time results stay adoptable under concurrent
        // writes (the window keys simply remain dirty afterwards). The
        // same collapse bound applies; an overflowed window forfeits
        // adoption instead of tracking unboundedly.
        if let Some(window) = &mut self.capture_window
            && !window.overflowed
        {
            if window.dirtied.len() >= DIRTY_COLLAPSE_MAX {
                window.overflowed = true;
                window.dirtied = HashSet::new();
            } else {
                window.dirtied.insert(key.to_string());
            }
        }
        // While invalid, per-key tracking is pointless (only a full
        // rebuild can help) — skip the string clone, e.g. during WAL
        // replay into a freshly deserialized store.
        if self.invalid {
            return;
        }
        // Memory safety valve: a store whose digest is never read (e.g.
        // the certified store, or an eventual store with digest sync
        // disabled) would otherwise retain one dirty String per distinct
        // key ever touched — including deleted ones. Beyond this bound,
        // per-key tracking retains more than a full rebuild would save:
        // collapse to `invalid` and free everything (correct by INV-1's
        // "invalid ⟹ full rebuild"; while invalid, tracking stays off).
        if self.dirty.len() >= DIRTY_COLLAPSE_MAX && !self.dirty.contains(key) {
            self.invalid = true;
            self.dirty = HashSet::new();
            self.key_meta = BTreeMap::new();
            self.cached = None;
            return;
        }
        self.dirty.insert(key.to_string());
    }

    /// Current mutation epoch (see INV-2).
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// True when an inline [`refresh`](Self::refresh) would do more than
    /// [`REFRESH_INLINE_MAX`] keys' worth of hashing under the lock —
    /// callers should warm up off-lock first (`ensure_digest_warm`) or
    /// use the legacy snapshot path.
    ///
    /// A never-refreshed VALID cache (`cached` is `None`, e.g. a fresh
    /// store filled through the mutation API) is warm as long as the
    /// dirty set is small: every key in `data` was inserted through
    /// `note_dirty`, so `data.len() <= dirty.len()` and the inline full
    /// rebuild is bounded by the same inline budget.
    pub fn is_cold(&self) -> bool {
        self.invalid || self.dirty.len() > REFRESH_INLINE_MAX
    }

    /// True when the cached digest and meta table exactly describe the
    /// store's data (refreshed, nothing dirty).
    pub fn is_clean(&self) -> bool {
        !self.invalid && self.cached.is_some() && self.dirty.is_empty()
    }

    /// Number of currently dirty keys (test observability).
    pub fn dirty_len(&self) -> usize {
        self.dirty.len()
    }

    /// Whether the cache is invalid (test observability).
    pub fn is_invalid(&self) -> bool {
        self.invalid
    }

    /// Iterate the keys whose bucket is in `buckets` (requires a clean
    /// cache — the caller asserts; used to extract mismatched-bucket
    /// entries without re-hashing any key).
    pub fn keys_in_buckets<'a>(
        &'a self,
        buckets: &'a HashSet<u16>,
    ) -> impl Iterator<Item = &'a String> {
        self.key_meta
            .iter()
            .filter(move |(_, meta)| buckets.contains(&(meta.bucket as u16)))
            .map(|(key, _)| key)
    }

    /// Bring the cache up to date with `data` and return the digest.
    ///
    /// Total: correct for ANY cache state (cold states pay a full
    /// in-lock rebuild — callers avoid that via [`is_cold`](Self::is_cold)
    /// / the warm-up protocol, this is the safety net). The steady-state
    /// path (`dirty` empty) is O(1); the incremental path hashes only
    /// the dirty keys' values, then recombines the affected buckets from
    /// cached 32-byte per-key digests.
    pub fn refresh(&mut self, data: &BTreeMap<String, CrdtValue>) -> &StoreDigest {
        if self.invalid || self.cached.is_none() {
            // Full rebuild: meta and digest from one pass (INV-3
            // established under the same `&mut` borrow that clears
            // `dirty`, so no mutation can interleave).
            let mut key_meta = BTreeMap::new();
            let digest = digest_pass(data, |key, bucket, kd| {
                key_meta.insert(
                    key.to_string(),
                    KeyMeta {
                        bucket,
                        digest: *kd,
                    },
                );
            });
            self.key_meta = key_meta;
            self.dirty.clear();
            self.invalid = false;
            self.cached = Some(digest);
        } else if !self.dirty.is_empty() {
            let dirty = std::mem::take(&mut self.dirty);
            let mut dirty_buckets = [false; DIGEST_BUCKET_COUNT];
            for key in &dirty {
                match data.get(key) {
                    Some(value) => {
                        let kd = key_digest(key, value);
                        let bucket = match self.key_meta.get_mut(key) {
                            Some(meta) => {
                                meta.digest = kd;
                                meta.bucket
                            }
                            None => {
                                let bucket = bucket_of(key) as u8;
                                self.key_meta
                                    .insert(key.clone(), KeyMeta { bucket, digest: kd });
                                bucket
                            }
                        };
                        dirty_buckets[bucket as usize] = true;
                    }
                    None => {
                        // Deleted (or never-inserted: an over-dirty
                        // `delete` miss contributes nothing).
                        if let Some(meta) = self.key_meta.remove(key) {
                            dirty_buckets[meta.bucket as usize] = true;
                        }
                    }
                }
            }
            self.rebuild_dirty_buckets(&dirty_buckets);
        }

        // Debug oracle (INV-4): the cached digest must be bit-identical
        // to a from-scratch recomputation. Every debug-build test that
        // touches `Store::digest()` exercises this.
        #[cfg(debug_assertions)]
        {
            let recomputed = compute_store_digest(data);
            debug_assert_eq!(
                self.cached.as_ref().expect("cached digest just refreshed"),
                &recomputed,
                "DigestCache diverged from compute_store_digest — an INV-1 \
                 coverage hole or a broken merge_value no-op contract"
            );
        }

        self.cached.as_ref().expect("cached digest just refreshed")
    }

    /// Capture off-lock warm-up work for the current cache state and
    /// open a capture window (INV-5b) so concurrent mutations are
    /// tracked until [`adopt`](Self::adopt) consumes it.
    ///
    /// `Full` when the meta table is unusable, otherwise just the dirty
    /// keys (O(d) clone instead of O(N)). A new capture supersedes any
    /// previous window, so results from an older capture can no longer
    /// be adopted under a moved generation.
    pub fn cold_work(&mut self, data: &BTreeMap<String, CrdtValue>) -> DigestColdWork {
        self.capture_window = Some(CaptureWindow {
            at_generation: self.generation,
            dirtied: HashSet::new(),
            overflowed: false,
        });
        if self.invalid || self.cached.is_none() {
            DigestColdWork::Full(data.clone())
        } else {
            DigestColdWork::Dirty(
                self.dirty
                    .iter()
                    .map(|key| (key.clone(), data.get(key).cloned()))
                    .collect(),
            )
        }
    }

    /// Adopt off-lock results captured at `at_generation`.
    ///
    /// Accepted when either the generation is unchanged (data untouched
    /// since capture — everything is adopted and the dirty set clears),
    /// or the capture window tracked every mutation since exactly that
    /// generation — then the capture-time results are adopted and the
    /// window keys stay dirty (per-key all-or-nothing, INV-5: every
    /// non-dirty key's meta is capture-time fresh AND unmutated since,
    /// hence current).
    ///
    /// Returns `false` — adopting NOTHING — when the window overflowed
    /// or was superseded, or when the cache shape no longer matches the
    /// results. A same-generation double adoption is idempotent (same
    /// data ⟹ same results).
    pub fn adopt(&mut self, results: DigestColdResults, at_generation: u64) -> bool {
        let window = self.capture_window.take();
        let window_keys = if self.generation == at_generation {
            // Untouched since capture: nothing can be dirty afterwards.
            None
        } else {
            // Mutations landed while hashing off-lock. The window tells
            // us EXACTLY which keys they touched — required intact and
            // opened at the same generation the results were captured
            // at, otherwise the whole batch is discarded.
            match window {
                Some(w) if w.at_generation == at_generation && !w.overflowed => Some(w.dirtied),
                _ => return false,
            }
        };
        match results {
            DigestColdResults::Full { digest, key_meta } => {
                self.key_meta = key_meta;
                self.cached = Some(*digest);
                self.dirty = window_keys.unwrap_or_default();
                self.invalid = false;
            }
            DigestColdResults::Dirty(items) => {
                // Dirty results are deltas over a valid meta table; a
                // Full/Dirty mix-up (e.g. after a collapse to invalid
                // during the window) would corrupt the cache silently.
                if self.invalid || self.cached.is_none() {
                    return false;
                }
                let mut dirty_buckets = [false; DIGEST_BUCKET_COUNT];
                for (key, meta) in items {
                    match meta {
                        Some(meta) => {
                            dirty_buckets[meta.bucket as usize] = true;
                            self.key_meta.insert(key, meta);
                        }
                        None => {
                            if let Some(old) = self.key_meta.remove(&key) {
                                dirty_buckets[old.bucket as usize] = true;
                            }
                        }
                    }
                }
                // The items cover exactly the dirty set captured by
                // `cold_work`; keys mutated since then (window keys, if
                // any) remain dirty and are re-hashed by the next
                // refresh before their buckets are ever served.
                self.dirty = window_keys.unwrap_or_default();
                self.rebuild_dirty_buckets(&dirty_buckets);
            }
        }
        true
    }

    /// Recombine the buckets flagged in `dirty_buckets` from the cached
    /// per-key digests (32 bytes each — no value re-hashing), then
    /// recompute the root. One in-order `key_meta` pass preserves the
    /// scheme's lexicographic per-bucket order.
    fn rebuild_dirty_buckets(&mut self, dirty_buckets: &[bool; DIGEST_BUCKET_COUNT]) {
        let cached = self
            .cached
            .as_mut()
            .expect("rebuild_dirty_buckets requires a cached digest");

        let mut hashers: Vec<Option<Sha256>> = (0..DIGEST_BUCKET_COUNT).map(|_| None).collect();
        let mut counts = [0u32; DIGEST_BUCKET_COUNT];
        for meta in self.key_meta.values() {
            let bucket = meta.bucket as usize;
            if dirty_buckets[bucket] {
                hashers[bucket]
                    .get_or_insert_with(Sha256::new)
                    .update(meta.digest);
                counts[bucket] += 1;
            }
        }
        for (i, hasher) in hashers.into_iter().enumerate() {
            if dirty_buckets[i] {
                cached.buckets[i] = match hasher {
                    Some(h) => h.finalize().into(),
                    None => EMPTY_BUCKET_DIGEST,
                };
                cached.key_counts[i] = counts[i];
            }
        }

        let mut root_hasher = Sha256::new();
        for bucket in &cached.buckets {
            root_hasher.update(bucket);
        }
        cached.root = root_hasher.finalize().into();
        cached.total_keys = self.key_meta.len() as u64;
    }
}

/// Compare a local digest against a remote sparse bucket list and return
/// the mismatched bucket indexes (ascending).
///
/// The comparison is bidirectional: a bucket that is empty locally but
/// non-empty remotely (or vice versa) is mismatched. Absent remote
/// entries mean "empty" ([`EMPTY_BUCKET_DIGEST`]). Remote entries with an
/// out-of-range index are ignored (the caller validates the request
/// before calling; this is defence in depth).
pub fn mismatched_buckets(
    local: &StoreDigest,
    remote_buckets: &[(u16, [u8; DIGEST_LEN])],
) -> Vec<u16> {
    let mut remote = [EMPTY_BUCKET_DIGEST; DIGEST_BUCKET_COUNT];
    for (index, digest) in remote_buckets {
        if let Some(slot) = remote.get_mut(*index as usize) {
            *slot = *digest;
        }
    }
    (0..DIGEST_BUCKET_COUNT)
        .filter(|&i| local.buckets[i] != remote[i])
        .map(|i| i as u16)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::lww_register::LwwRegister;
    use crate::crdt::or_map::OrMap;
    use crate::crdt::or_set::OrSet;
    use crate::crdt::pn_counter::PnCounter;
    use crate::hlc::HlcTimestamp;
    use crate::types::NodeId;

    fn node(name: &str) -> NodeId {
        NodeId(name.into())
    }

    fn ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    /// Fixed entry set covering all four CRDT types, used by the golden
    /// test and the determinism tests.
    fn fixture_entries() -> Vec<(String, CrdtValue)> {
        let mut counter = PnCounter::new();
        counter.increment(&node("node-a"));
        counter.increment(&node("node-b"));
        counter.decrement(&node("node-a"));

        let mut set = OrSet::new();
        set.add("alice".to_string(), &node("node-a"));
        set.add("bob".to_string(), &node("node-b"));
        set.remove(&"alice".to_string());

        let mut map = OrMap::new();
        let _ = map.set(
            "name".to_string(),
            "asteroid".to_string(),
            ts(100, 0, "node-a"),
            &node("node-a"),
        );
        let _ = map.set(
            "tier".to_string(),
            "gold".to_string(),
            ts(101, 2, "node-b"),
            &node("node-b"),
        );
        map.delete(&"tier".to_string());

        let mut reg = LwwRegister::new();
        let _ = reg.set("online".to_string(), ts(200, 1, "node-c"));

        vec![
            ("counter/hits".to_string(), CrdtValue::Counter(counter)),
            ("set/users".to_string(), CrdtValue::Set(set)),
            ("map/config".to_string(), CrdtValue::Map(map)),
            ("reg/status".to_string(), CrdtValue::Register(reg)),
        ]
    }

    fn to_btree(entries: Vec<(String, CrdtValue)>) -> BTreeMap<String, CrdtValue> {
        entries.into_iter().collect()
    }

    // ---------------------------------------------------------------
    // Determinism
    // ---------------------------------------------------------------

    #[test]
    fn identical_states_produce_identical_digests() {
        let a = compute_store_digest(&to_btree(fixture_entries()));
        let b = compute_store_digest(&to_btree(fixture_entries()));
        assert_eq!(a.root, b.root);
        assert_eq!(a.buckets, b.buckets);
        assert_eq!(a.key_counts, b.key_counts);
        assert_eq!(a.total_keys, b.total_keys);
    }

    #[test]
    fn insertion_order_does_not_affect_digest() {
        let forward = to_btree(fixture_entries());
        let mut reversed_entries = fixture_entries();
        reversed_entries.reverse();
        let reversed = to_btree(reversed_entries);
        assert_eq!(
            compute_store_digest(&forward).root,
            compute_store_digest(&reversed).root
        );
    }

    /// Serde round-trips rebuild every inner `HashMap`/`HashSet` with a
    /// fresh (randomly seeded) layout — the digest must not change.
    #[test]
    fn serde_roundtrip_does_not_affect_digest() {
        let original = to_btree(fixture_entries());
        let before = compute_store_digest(&original);

        let json = serde_json::to_string(&original).unwrap();
        let restored: BTreeMap<String, CrdtValue> = serde_json::from_str(&json).unwrap();
        let after = compute_store_digest(&restored);

        assert_eq!(before.root, after.root);
        assert_eq!(before.buckets, after.buckets);
    }

    /// Merge order must not affect the digest of the converged state.
    #[test]
    fn merge_order_does_not_affect_digest() {
        let mut set_a = OrSet::new();
        set_a.add("x".to_string(), &node("node-a"));
        set_a.add("y".to_string(), &node("node-a"));
        let mut set_b = OrSet::new();
        set_b.add("z".to_string(), &node("node-b"));
        set_b.add("x".to_string(), &node("node-b"));
        set_b.remove(&"x".to_string());

        let mut ab = set_a.clone();
        ab.merge(&set_b);
        let mut ba = set_b.clone();
        ba.merge(&set_a);

        let store_ab = to_btree(vec![("k".into(), CrdtValue::Set(ab))]);
        let store_ba = to_btree(vec![("k".into(), CrdtValue::Set(ba))]);
        assert_eq!(
            compute_store_digest(&store_ab).root,
            compute_store_digest(&store_ba).root
        );
    }

    // ---------------------------------------------------------------
    // Sensitivity: a one-element difference must change the digest
    // ---------------------------------------------------------------

    #[test]
    fn changed_value_changes_only_its_bucket_and_root() {
        let base = to_btree(fixture_entries());
        let mut modified = base.clone();
        if let Some(CrdtValue::Counter(c)) = modified.get_mut("counter/hits") {
            c.increment(&node("node-a"));
        } else {
            panic!("fixture missing counter");
        }

        let d_base = compute_store_digest(&base);
        let d_mod = compute_store_digest(&modified);

        assert_ne!(d_base.root, d_mod.root);
        let changed_bucket = bucket_of("counter/hits");
        for i in 0..DIGEST_BUCKET_COUNT {
            if i == changed_bucket {
                assert_ne!(d_base.buckets[i], d_mod.buckets[i]);
            } else {
                assert_eq!(d_base.buckets[i], d_mod.buckets[i], "bucket {i} changed");
            }
        }
    }

    #[test]
    fn deferred_tombstone_difference_changes_digest() {
        // UNCOVERED tombstone difference → mismatch (a pending remove is
        // propagated by the digest path).
        let mut with_tombstone = OrSet::new();
        with_tombstone.add("x".to_string(), &node("node-a"));
        let without_tombstone = with_tombstone.clone();
        with_tombstone.add("gone".to_string(), &node("node-a"));
        with_tombstone.remove(&"gone".to_string());

        // Visible elements are identical ({"x"}), but the deferred sets
        // (and counters) differ — the digest must distinguish them so a
        // pending remove is propagated by the digest path.
        let a = to_btree(vec![("k".into(), CrdtValue::Set(with_tombstone))]);
        let b = to_btree(vec![("k".into(), CrdtValue::Set(without_tombstone))]);
        assert_ne!(compute_store_digest(&a).root, compute_store_digest(&b).root);
    }

    /// Scheme v2 canonical form: two stores that differ ONLY in a
    /// floor-COVERED own tombstone (a fresh remove below the floor,
    /// origin-retained for its gated sweep) must digest identically —
    /// AND merging either way must be a no-op on observable state, which
    /// is exactly what justifies adopting session claims on a match.
    #[test]
    fn covered_own_tombstone_is_canonical_invisible_and_merge_is_noop() {
        let n = node("node-a");
        let mut base = OrSet::new();
        base.add("keep".to_string(), &n); // (a,1)
        base.add("x".to_string(), &n); // (a,2)
        base.remove(&"x".to_string());
        // Certified sweep: floor reaches 2, tombstone (a,2) dropped.
        base.compact_deferred_certified(&base.deferred_dots(), None);

        // Replica A: fresh remove below the floor (covered own tombstone).
        let mut set_a = base.clone();
        set_a.remove(&"keep".to_string()); // tombstone (a,1), covered
        // Replica B: learned the remove (kill) but rejected the covered
        // tombstone — the post-merge normal form.
        let mut set_b = base.clone();
        set_b.merge(&set_a);
        assert_eq!(set_a.deferred_len(), 1);
        assert_eq!(set_b.deferred_len(), 0);

        let store_a = to_btree(vec![("k".into(), CrdtValue::Set(set_a.clone()))]);
        let store_b = to_btree(vec![("k".into(), CrdtValue::Set(set_b.clone()))]);
        assert_eq!(
            compute_store_digest(&store_a).root,
            compute_store_digest(&store_b).root,
            "covered own tombstone must be canonical-invisible"
        );

        // Bidirectional merge is a no-op on observable state (claims
        // adoption soundness: match ⟹ nothing to transfer either way).
        let mut a2 = set_a.clone();
        a2.merge(&set_b);
        let mut b2 = set_b.clone();
        b2.merge(&set_a);
        assert_eq!(a2.elements(), set_a.elements());
        assert_eq!(b2.elements(), set_b.elements());
        assert!(!a2.contains(&"keep".to_string()));
        assert!(!b2.contains(&"keep".to_string()));
    }

    /// Scheme v2: a floor-only difference IS a digest mismatch — the
    /// floor is semantic state (it kills and rejects dots) and must be
    /// propagated by the bucket transfer (the self-healing round trip).
    #[test]
    fn floor_only_difference_changes_digest() {
        let n = node("node-a");
        let mut unswept = OrSet::new();
        unswept.add("x".to_string(), &n);
        unswept.remove(&"x".to_string());
        let mut swept = unswept.clone();
        swept.compact_deferred_certified(&swept.deferred_dots(), None);

        let a = to_btree(vec![("k".into(), CrdtValue::Set(unswept))]);
        let b = to_btree(vec![("k".into(), CrdtValue::Set(swept))]);
        assert_ne!(
            compute_store_digest(&a).root,
            compute_store_digest(&b).root,
            "an advanced floor must mismatch so the transfer can propagate it"
        );
    }

    #[test]
    fn register_writer_identity_changes_digest() {
        let mut reg_a = LwwRegister::new();
        let _ = reg_a.set("v".to_string(), ts(100, 0, "node-a"));
        let mut reg_b = LwwRegister::new();
        let _ = reg_b.set("v".to_string(), ts(100, 0, "node-b"));

        let a = to_btree(vec![("k".into(), CrdtValue::Register(reg_a))]);
        let b = to_btree(vec![("k".into(), CrdtValue::Register(reg_b))]);
        assert_ne!(compute_store_digest(&a).root, compute_store_digest(&b).root);
    }

    #[test]
    fn counter_p_vs_n_not_confused() {
        // {p: a=1} vs {n: a=1}: same maps, different halves.
        let mut inc = PnCounter::new();
        inc.increment(&node("a"));
        let mut dec = PnCounter::new();
        dec.decrement(&node("a"));
        let a = to_btree(vec![("k".into(), CrdtValue::Counter(inc))]);
        let b = to_btree(vec![("k".into(), CrdtValue::Counter(dec))]);
        assert_ne!(compute_store_digest(&a).root, compute_store_digest(&b).root);
    }

    /// An OR-Map entry whose dot set was emptied by a merge must digest
    /// identically to a map that never had the entry (normalisation).
    #[test]
    fn empty_dot_map_entry_digests_like_absent_entry() {
        let mut map_a = OrMap::new();
        let _ = map_a.set(
            "doomed".to_string(),
            "v".to_string(),
            ts(100, 0, "node-a"),
            &node("node-a"),
        );
        // Delete on a fork, then merge back: entry disappears via retain.
        let mut fork = map_a.clone();
        fork.delete(&"doomed".to_string());
        map_a.merge(&fork);

        let map_b = fork;
        let a = to_btree(vec![("k".into(), CrdtValue::Map(map_a))]);
        let b = to_btree(vec![("k".into(), CrdtValue::Map(map_b))]);
        assert_eq!(compute_store_digest(&a).root, compute_store_digest(&b).root);
    }

    // ---------------------------------------------------------------
    // Structure invariants
    // ---------------------------------------------------------------

    #[test]
    fn key_counts_sum_to_total() {
        let digest = compute_store_digest(&to_btree(fixture_entries()));
        let sum: u64 = digest.key_counts.iter().map(|&c| c as u64).sum();
        assert_eq!(sum, digest.total_keys);
        assert_eq!(digest.total_keys, 4);
    }

    #[test]
    fn empty_store_digest_is_stable() {
        let empty = BTreeMap::new();
        let digest = compute_store_digest(&empty);
        assert_eq!(digest.total_keys, 0);
        assert!(digest.buckets.iter().all(|b| *b == EMPTY_BUCKET_DIGEST));
        // Root of 256 zero-buckets: SHA256 of 8192 zero bytes.
        let expected: [u8; 32] = Sha256::digest(vec![0u8; DIGEST_BUCKET_COUNT * DIGEST_LEN]).into();
        assert_eq!(digest.root, expected);
        assert_eq!(digest.non_empty_buckets().count(), 0);
    }

    #[test]
    fn non_empty_buckets_matches_key_counts() {
        let digest = compute_store_digest(&to_btree(fixture_entries()));
        let sparse: Vec<u16> = digest.non_empty_buckets().map(|(i, _)| i).collect();
        for (i, &count) in digest.key_counts.iter().enumerate() {
            assert_eq!(count > 0, sparse.contains(&(i as u16)), "bucket {i}");
        }
    }

    // ---------------------------------------------------------------
    // mismatched_buckets
    // ---------------------------------------------------------------

    #[test]
    fn mismatched_buckets_empty_for_identical_digests() {
        let digest = compute_store_digest(&to_btree(fixture_entries()));
        let sparse: Vec<(u16, [u8; DIGEST_LEN])> =
            digest.non_empty_buckets().map(|(i, d)| (i, *d)).collect();
        assert!(mismatched_buckets(&digest, &sparse).is_empty());
    }

    #[test]
    fn mismatched_buckets_is_bidirectional() {
        let local = compute_store_digest(&to_btree(fixture_entries()));
        // Remote is empty: every locally non-empty bucket mismatches.
        let mismatched = mismatched_buckets(&local, &[]);
        let expected: Vec<u16> = local.non_empty_buckets().map(|(i, _)| i).collect();
        assert_eq!(mismatched, expected);

        // Conversely, a remote-only bucket also mismatches.
        let empty_local = compute_store_digest(&BTreeMap::new());
        let remote = [(7u16, [0xABu8; DIGEST_LEN])];
        assert_eq!(mismatched_buckets(&empty_local, &remote), vec![7]);
    }

    #[test]
    fn mismatched_buckets_ignores_out_of_range_indexes() {
        let local = compute_store_digest(&BTreeMap::new());
        // 256 is not a valid bucket index for u16 wire values 0..=255;
        // it must be ignored, not panic or alias into range.
        let remote = [(256u16, [0xABu8; DIGEST_LEN]), (300u16, [1u8; DIGEST_LEN])];
        assert!(mismatched_buckets(&local, &remote).is_empty());
    }

    // ---------------------------------------------------------------
    // Golden digest — freezes the wire contract of scheme version 2.
    // ---------------------------------------------------------------

    /// If this test fails you have changed the canonical digest encoding.
    /// That is a WIRE CONTRACT change: bump `DIGEST_SCHEME_VERSION`, update
    /// this expected value, and note the change in docs/architecture.md.
    #[test]
    fn golden_root_digest_scheme_v2() {
        let digest = compute_store_digest(&to_btree(fixture_entries()));
        assert_eq!(
            hex::encode(digest.root),
            "c307197eecba4be1fe66034db3437ad98ee60e8123f76d50e76bc65e9dfb8372",
            "canonical digest encoding changed — see test doc comment"
        );
    }

    /// Golden digest of a floor-bearing state (freezes the v2 floor
    /// encoding and the covered-deferred exclusion, which the base
    /// fixture — floors empty — does not exercise).
    #[test]
    fn golden_root_digest_scheme_v2_with_floor() {
        let n = node("node-a");
        let mut set = OrSet::new();
        set.add("alice".to_string(), &n);
        set.add("bob".to_string(), &n);
        set.remove(&"bob".to_string());
        set.compact_deferred_certified(&set.deferred_dots(), None);
        // Fresh covered tombstone (canonical-invisible).
        set.remove(&"alice".to_string());

        let entries = to_btree(vec![("set/users".to_string(), CrdtValue::Set(set))]);
        let digest = compute_store_digest(&entries);
        assert_eq!(
            hex::encode(digest.root),
            "b0376ffa17bd9e9c8c8251f99be5ef3c726e27b8d8cf6dd021d573ce65ce8843",
            "canonical digest encoding changed — see test doc comment"
        );
    }

    #[test]
    fn bucket_of_is_stable() {
        // Freeze a few bucket assignments (part of the wire scheme).
        assert_eq!(bucket_of("counter/hits"), bucket_of("counter/hits"));
        let d = Sha256::digest("counter/hits".as_bytes());
        assert_eq!(bucket_of("counter/hits"), d[0] as usize);
    }

    // ---------------------------------------------------------------
    // digest_pass sink + incremental cache equivalence (M-7)
    // ---------------------------------------------------------------

    /// The sink must report exactly `bucket_of(key)` and the same
    /// per-key digest the wire digest was built from, in lexicographic
    /// key order, without changing the output of the pass.
    #[test]
    fn digest_pass_sink_reports_bucket_of_and_identical_digest() {
        let entries = to_btree(fixture_entries());
        let mut seen: Vec<(String, u8, [u8; DIGEST_LEN])> = Vec::new();
        let via_pass = digest_pass(&entries, |key, bucket, kd| {
            seen.push((key.to_string(), bucket, *kd));
        });

        assert_eq!(via_pass, compute_store_digest(&entries));
        assert_eq!(seen.len(), entries.len());
        let keys: Vec<&String> = entries.keys().collect();
        for (i, (key, bucket, kd)) in seen.iter().enumerate() {
            assert_eq!(key, keys[i], "sink order must be lexicographic");
            assert_eq!(*bucket as usize, bucket_of(key));
            assert_eq!(kd, &key_digest(key, &entries[key]));
        }
    }

    /// The same golden root as `golden_root_digest_scheme_v2`, but
    /// produced through the incremental `Store::digest()` cache — the
    /// cache must reproduce the frozen wire contract bit-for-bit.
    #[test]
    fn golden_root_digest_scheme_v2_via_store_cache() {
        use crate::store::kv::Store;

        let mut store = Store::new();
        for (key, value) in fixture_entries() {
            store.put(key, value);
        }
        assert_eq!(
            hex::encode(store.digest().root),
            "c307197eecba4be1fe66034db3437ad98ee60e8123f76d50e76bc65e9dfb8372",
            "the digest cache changed the wire digest — see the golden test doc"
        );

        // And incrementally: mutating one key and refreshing must equal
        // a from-scratch recompute of the mutated state.
        if let Some(CrdtValue::Counter(c)) = store.get_mut("counter/hits") {
            c.increment(&node("node-a"));
        } else {
            panic!("fixture missing counter");
        }
        let recomputed = compute_store_digest(
            &store
                .all_entries()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        assert_eq!(store.digest(), recomputed);
    }

    // ---------------------------------------------------------------
    // Capture-window adoption (INV-5b): off-lock results under
    // concurrent writes
    // ---------------------------------------------------------------

    fn register_named(text: &str) -> CrdtValue {
        let mut register = LwwRegister::new();
        let _ = register.set(text.into(), ts(9, 0, "node-w"));
        CrdtValue::Register(register)
    }

    /// Full results raced by writes: adopted, with exactly the raced
    /// keys left dirty and the next refresh bit-exact.
    #[test]
    fn adopt_full_under_concurrent_writes_keeps_raced_keys_dirty() {
        let mut data = to_btree(fixture_entries());
        let mut cache = DigestCache::default(); // invalid, e.g. post-restart
        let generation = cache.generation();
        let work = cache.cold_work(&data);
        let results = work.compute();

        // Concurrent writes while "hashing": one new key, one existing.
        data.insert("raced/new".into(), register_named("n"));
        cache.note_dirty("raced/new");
        data.insert("register/name".into(), register_named("changed"));
        cache.note_dirty("register/name");

        assert!(
            cache.adopt(results, generation),
            "an intact capture window must vouch for the batch"
        );
        assert!(!cache.is_invalid());
        assert_eq!(cache.dirty_len(), 2, "exactly the raced keys stay dirty");
        assert_eq!(cache.refresh(&data), &compute_store_digest(&data));
    }

    /// Dirty results raced by writes: capture-time metas adopted, the
    /// raced keys (including one re-raced capture key) stay dirty.
    #[test]
    fn adopt_dirty_under_concurrent_writes_keeps_raced_keys_dirty() {
        let mut data = to_btree(fixture_entries());
        let mut cache = DigestCache::default();
        cache.refresh(&data); // warm baseline

        // Two keys go dirty before the capture.
        data.insert("register/name".into(), register_named("v1"));
        cache.note_dirty("register/name");
        data.insert("dirty/other".into(), register_named("o1"));
        cache.note_dirty("dirty/other");

        let generation = cache.generation();
        let work = cache.cold_work(&data);
        assert!(matches!(work, DigestColdWork::Dirty(_)));
        let results = work.compute();

        // Race: one captured key mutates AGAIN, plus a fresh key.
        data.insert("register/name".into(), register_named("v2"));
        cache.note_dirty("register/name");
        data.insert("raced/new".into(), register_named("n"));
        cache.note_dirty("raced/new");

        assert!(cache.adopt(results, generation));
        assert_eq!(
            cache.dirty_len(),
            2,
            "the re-raced capture key and the fresh key stay dirty"
        );
        assert_eq!(cache.refresh(&data), &compute_store_digest(&data));
    }

    /// An overflowed window can no longer prove which keys mutated:
    /// the batch is discarded whole.
    #[test]
    fn adopt_rejects_results_when_window_overflowed() {
        let data = to_btree(fixture_entries());
        let mut cache = DigestCache::default();
        let generation = cache.generation();
        let results = cache.cold_work(&data).compute();

        for i in 0..=DIRTY_COLLAPSE_MAX {
            cache.note_dirty(&format!("flood-{i}"));
        }

        assert!(!cache.adopt(results, generation));
        assert!(cache.is_invalid(), "nothing may be adopted");
    }

    /// A second capture supersedes the window: results from the first
    /// capture can no longer pair with it once the generation moved.
    #[test]
    fn adopt_rejects_results_from_superseded_capture() {
        let mut data = to_btree(fixture_entries());
        let mut cache = DigestCache::default();
        let generation = cache.generation();
        let results = cache.cold_work(&data).compute();

        data.insert("raced/new".into(), register_named("n"));
        cache.note_dirty("raced/new");
        let _ = cache.cold_work(&data); // supersedes the first window

        data.insert("raced/other".into(), register_named("o"));
        cache.note_dirty("raced/other");

        assert!(
            !cache.adopt(results, generation),
            "a superseded window must reject the stale batch"
        );
        assert!(cache.is_invalid());
    }

    /// Adoption with no open window (already consumed) and a moved
    /// generation must reject — the strict pre-window contract.
    #[test]
    fn adopt_rejects_moved_generation_without_window() {
        let mut data = to_btree(fixture_entries());
        let mut cache = DigestCache::default();
        let generation = cache.generation();
        let results = cache.cold_work(&data).compute();

        data.insert("raced/new".into(), register_named("n"));
        cache.note_dirty("raced/new");
        // First adoption succeeds via the window (and consumes it)…
        assert!(cache.adopt(results, generation));

        // …a replayed batch without a window must be rejected.
        let mut stale_cache = DigestCache::default();
        let replay = stale_cache.cold_work(&data).compute();
        cache.note_dirty("raced/new");
        assert!(!cache.adopt(replay, generation));
    }
}
