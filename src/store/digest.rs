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
//! Layout (scheme version 1):
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
//! computes every bucket. Identical key sets with identical CRDT states
//! produce identical digests on every replica — the property the whole
//! protocol rests on ("digest matched" ⟺ CRDT state equality up to
//! SHA-256 collisions), and what makes adopting the sender's session
//! claims on a match as sound as after a full dump.
//!
//! The digest deliberately EXCLUDES `Store::timestamps` (per-key HLCs):
//! push-path merges re-stamp entries with a local clock tick and pruning
//! removes entries one-sidedly, so per-key HLCs never converge across
//! replicas and would cause permanent false mismatches.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::crdt::digest::write_str;
use crate::store::kv::CrdtValue;

/// Version of the digest wire scheme (bucket layout + canonical CRDT
/// streams). Bump on ANY change to the canonical encoding — peers with a
/// different version answer `scheme_ok = false` and the requester falls
/// back to the legacy full sync (rolling-upgrade safe).
pub const DIGEST_SCHEME_VERSION: u32 = 1;

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

/// Compute the two-level digest of a store snapshot.
///
/// `entries` must be the store's data map (a `BTreeMap`, i.e. already in
/// lexicographic key order). One in-order pass feeds each per-key digest
/// into its bucket's hasher; the per-bucket order is thereby the
/// lexicographic key order required by the scheme.
///
/// Cost is O(total CRDT state size) — callers on the sync path snapshot
/// the map under the store lock, release it, and run this inside
/// `spawn_blocking`.
pub fn compute_store_digest(entries: &BTreeMap<String, CrdtValue>) -> StoreDigest {
    let mut hashers: Vec<Option<Sha256>> = (0..DIGEST_BUCKET_COUNT).map(|_| None).collect();
    let mut key_counts = [0u32; DIGEST_BUCKET_COUNT];
    let mut total_keys = 0u64;

    for (key, value) in entries {
        let mut key_hasher = Sha256::new();
        write_str(&mut key_hasher, key);
        value.canonical_digest_into(&mut key_hasher);
        let key_digest = key_hasher.finalize();

        let bucket = bucket_of(key);
        hashers[bucket]
            .get_or_insert_with(Sha256::new)
            .update(key_digest);
        key_counts[bucket] += 1;
        total_keys += 1;
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
        map.set(
            "name".to_string(),
            "asteroid".to_string(),
            ts(100, 0, "node-a"),
            &node("node-a"),
        );
        map.set(
            "tier".to_string(),
            "gold".to_string(),
            ts(101, 2, "node-b"),
            &node("node-b"),
        );
        map.delete(&"tier".to_string());

        let mut reg = LwwRegister::new();
        reg.set("online".to_string(), ts(200, 1, "node-c"));

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

    #[test]
    fn register_writer_identity_changes_digest() {
        let mut reg_a = LwwRegister::new();
        reg_a.set("v".to_string(), ts(100, 0, "node-a"));
        let mut reg_b = LwwRegister::new();
        reg_b.set("v".to_string(), ts(100, 0, "node-b"));

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
        map_a.set(
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
    // Golden digest — freezes the wire contract of scheme version 1.
    // ---------------------------------------------------------------

    /// If this test fails you have changed the canonical digest encoding.
    /// That is a WIRE CONTRACT change: bump `DIGEST_SCHEME_VERSION`, update
    /// this expected value, and note the change in docs/architecture.md.
    #[test]
    fn golden_root_digest_scheme_v1() {
        let digest = compute_store_digest(&to_btree(fixture_entries()));
        assert_eq!(
            hex::encode(digest.root),
            "b6dfe8dcb7f9c9023a4649c8f0c010578991d53867def4c1e30601442ca9b67b",
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
}
