//! Off-lock warm-up protocol for the store's incremental digest cache
//! (M-7).
//!
//! A cold cache (deserialized store, or a burst of writes larger than
//! the inline refresh budget) would force `Store::digest()` to do a full
//! O(N) hashing pass UNDER the store lock. This module moves that work
//! off the lock with a per-key all-or-nothing validity check:
//!
//! 1. under the lock: capture the cold work (full snapshot, or just the
//!    dirty keys) plus the current `digest_generation` — this opens a
//!    capture window that records every key mutated from here on;
//! 2. off the lock (`spawn_blocking`): hash it;
//! 3. under the lock again: adopt the results. If the generation is
//!    unchanged everything is adopted outright; otherwise the capture
//!    window proves exactly which keys mutated meanwhile, the
//!    capture-time results are adopted and those keys simply stay dirty
//!    (re-hashed by the next refresh). Only a window that overflowed
//!    (extreme churn) or was superseded discards the batch whole.
//!
//! The window is what keeps warm-up convergent on large stores under
//! sustained writes (a restart + steady ingest would otherwise lose the
//! generation race on every attempt, forever paying discarded full
//! clones and hashes ON TOP of the legacy path). Failure is still
//! always safe: the cache stays cold and callers use the legacy
//! snapshot path (pre-M-7 behaviour, bit-identical digests).

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::api::eventual::EventualApi;
use crate::store::digest::{DigestColdResults, DigestColdWork};

/// Attempts to bring the eventual store's digest cache to the warm state
/// (at most two capture/hash/adopt rounds). Returns `true` when the
/// cache is warm on return; `false` when it is still cold (adoption
/// failed twice, or the adopted dirty backlog still exceeds the inline
/// budget — the caller falls back to the legacy snapshot digest path).
///
/// The store lock is never held across an `.await` of the hashing work.
pub async fn ensure_digest_warm(eventual: &Arc<Mutex<EventualApi>>) -> bool {
    ensure_digest_warm_with(eventual, DigestColdWork::compute).await
}

/// [`ensure_digest_warm`] with an injectable hashing step, so tests can
/// deterministically interleave writes (or corrupt the results) between
/// capture and adoption. Production always passes
/// [`DigestColdWork::compute`].
async fn ensure_digest_warm_with<F>(eventual: &Arc<Mutex<EventualApi>>, hash: F) -> bool
where
    F: Fn(DigestColdWork) -> DigestColdResults + Clone + Send + 'static,
{
    for _ in 0..2 {
        let (work, generation) = {
            let mut api = eventual.lock().await;
            if !api.store().digest_is_cold() {
                return true;
            }
            let generation = api.store().digest_generation();
            (api.store_mut().digest_cold_work(), generation)
        };

        let hash = hash.clone();
        let results = tokio::task::spawn_blocking(move || hash(work))
            .await
            .expect("spawn_blocking panicked");

        let mut api = eventual.lock().await;
        if api.store_mut().adopt_digest_work(results, generation) && !api.store().digest_is_cold() {
            return true;
        }
        // Either the capture window could not vouch for the results
        // (discarded whole) or the adopted dirty backlog is itself
        // over the inline budget: retry once — the second round
        // captures only the remaining dirty keys — then give up for
        // this cycle.
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::digest::compute_store_digest;
    use crate::types::NodeId;
    use std::collections::BTreeMap;

    fn api_with_keys(n: usize) -> Arc<Mutex<EventualApi>> {
        let mut api = EventualApi::new(NodeId("warm-node".into()));
        for i in 0..n {
            api.eventual_counter_inc(&format!("key-{i:04}")).unwrap();
        }
        Arc::new(Mutex::new(api))
    }

    /// Force a cold cache by round-tripping the store through serde
    /// (`#[serde(skip)]` yields the invalid default).
    async fn make_cold(api: &Arc<Mutex<EventualApi>>) {
        let mut guard = api.lock().await;
        let json = serde_json::to_string(guard.store()).unwrap();
        *guard.store_mut() = serde_json::from_str(&json).unwrap();
        assert!(guard.store().digest_cache_is_invalid());
        assert!(guard.store().digest_is_cold());
    }

    #[tokio::test]
    async fn warmup_on_already_warm_cache_is_a_noop_success() {
        let api = api_with_keys(3);
        assert!(ensure_digest_warm(&api).await);
        let mut guard = api.lock().await;
        assert!(!guard.store().digest_is_cold());
        let data: BTreeMap<_, _> = guard
            .store()
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(guard.store_mut().digest(), compute_store_digest(&data));
    }

    #[tokio::test]
    async fn warmup_adopts_on_stable_generation() {
        let api = api_with_keys(8);
        make_cold(&api).await;

        assert!(ensure_digest_warm(&api).await, "quiet store must warm up");

        let mut guard = api.lock().await;
        assert!(!guard.store().digest_cache_is_invalid());
        assert!(!guard.store().digest_is_cold());
        assert_eq!(guard.store().digest_cache_dirty_len(), 0);
        let data: BTreeMap<_, _> = guard
            .store()
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(
            guard.store_mut().digest(),
            compute_store_digest(&data),
            "warmed cache must reproduce the from-scratch digest"
        );
    }

    /// F1 regression (per-key all-or-nothing adoption): a write injected
    /// between capture and adoption does NOT pollute the meta table —
    /// the capture-time results are adopted, the injected key stays
    /// dirty, and the served digest is exact (includes the write).
    #[tokio::test]
    async fn warmup_adopts_under_concurrent_write_and_digest_stays_exact() {
        let api = api_with_keys(8);
        make_cold(&api).await;

        // Drive the capture/hash phase manually so a write can be
        // injected between capture and adoption.
        let (work, generation) = {
            let mut guard = api.lock().await;
            let generation = guard.store().digest_generation();
            (guard.store_mut().digest_cold_work(), generation)
        };
        let results = work.compute();

        let mut guard = api.lock().await;
        guard.store_mut().put(
            "injected-after-capture".into(),
            crate::store::kv::CrdtValue::Register(crate::crdt::lww_register::LwwRegister::new()),
        );
        assert!(
            guard.store_mut().adopt_digest_work(results, generation),
            "the capture window vouches for the batch despite the race"
        );
        assert!(!guard.store().digest_cache_is_invalid());
        assert_eq!(
            guard.store().digest_cache_dirty_len(),
            1,
            "exactly the key written during the race must stay dirty"
        );

        // The served digest must be exact — stale capture-time meta for
        // the injected key must never leak into the wire digest (the
        // refresh debug oracle would also catch it).
        let data: BTreeMap<_, _> = guard
            .store()
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert!(data.contains_key("injected-after-capture"));
        assert_eq!(guard.store_mut().digest(), compute_store_digest(&data));
        assert_eq!(guard.store().digest_cache_dirty_len(), 0);
    }

    /// Design test T20 — the give-up contract: when adoption fails on
    /// BOTH rounds, `ensure_digest_warm` returns `false`, adopts nothing
    /// on either attempt, and leaves the cache cold so callers take the
    /// legacy snapshot path (whose digest is still exact).
    ///
    /// The failure is driven through the real production trigger: the
    /// injected hashing step floods the store (off the lock) with more
    /// distinct writes than `DIRTY_COLLAPSE_MAX`, overflowing the
    /// capture window so the batch can no longer be vouched for.
    #[tokio::test]
    async fn warmup_returns_false_after_two_failed_adoptions_and_cache_stays_cold() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let api = api_with_keys(8);
        make_cold(&api).await;

        let flood_api = Arc::clone(&api);
        let flood_round = Arc::new(AtomicUsize::new(0));
        let hash = move |work: DigestColdWork| {
            let round = flood_round.fetch_add(1, Ordering::SeqCst);
            {
                // Runs inside spawn_blocking: the warm-up holds no lock.
                let mut guard = flood_api.blocking_lock();
                for i in 0..=crate::store::digest::DIRTY_COLLAPSE_MAX {
                    guard.store_mut().put(
                        format!("flood-{round}-{i}"),
                        crate::store::kv::CrdtValue::Register(
                            crate::crdt::lww_register::LwwRegister::new(),
                        ),
                    );
                }
            }
            work.compute()
        };

        assert!(
            !ensure_digest_warm_with(&api, hash).await,
            "two overflowed capture windows must give up for this cycle"
        );

        let mut guard = api.lock().await;
        assert!(
            guard.store().digest_is_cold(),
            "give-up must leave the cache cold (legacy path for callers)"
        );
        assert!(
            guard.store().digest_cache_is_invalid(),
            "nothing may have been adopted on either attempt"
        );

        // The legacy fallback stays exact: a from-scratch recompute and
        // the total `Store::digest()` safety net agree.
        let data: BTreeMap<_, _> = guard
            .store()
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(guard.store_mut().digest(), compute_store_digest(&data));
    }

    /// Restart + sustained ingest (the finding-2 workload): a cold
    /// (invalid) large-ish store whose every off-lock hashing round is
    /// raced by a write STILL warms up on the first attempt — the
    /// capture window absorbs the race instead of discarding the work.
    #[tokio::test]
    async fn warmup_converges_under_sustained_writes() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let api = api_with_keys(32);
        make_cold(&api).await;

        let writer_api = Arc::clone(&api);
        let seq = Arc::new(AtomicUsize::new(0));
        let hash = move |work: DigestColdWork| {
            let i = seq.fetch_add(1, Ordering::SeqCst);
            writer_api
                .blocking_lock()
                .eventual_counter_inc(&format!("sustained-{i}"))
                .unwrap();
            work.compute()
        };

        assert!(
            ensure_digest_warm_with(&api, hash).await,
            "a single racing write per round must not defeat the warm-up"
        );

        let mut guard = api.lock().await;
        assert!(!guard.store().digest_is_cold());
        let data: BTreeMap<_, _> = guard
            .store()
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert!(data.contains_key("sustained-0"));
        assert_eq!(guard.store_mut().digest(), compute_store_digest(&data));
    }

    /// Dirty-only warm-up: a valid cache with a large dirty burst warms
    /// up by re-hashing only the dirty keys, and matches a recompute.
    #[tokio::test]
    async fn warmup_dirty_burst_rehashes_incrementally_and_matches() {
        let api = api_with_keys(4);
        {
            // Refresh once so the cache holds a baseline digest…
            let mut guard = api.lock().await;
            let _ = guard.store_mut().digest();
            // …then dirty more keys than the inline budget.
            for i in 0..(crate::store::digest::REFRESH_INLINE_MAX + 5) {
                guard
                    .eventual_counter_inc(&format!("burst-{i:05}"))
                    .unwrap();
            }
            assert!(guard.store().digest_is_cold());
            assert!(!guard.store().digest_cache_is_invalid());
        }

        assert!(ensure_digest_warm(&api).await);

        let mut guard = api.lock().await;
        assert_eq!(guard.store().digest_cache_dirty_len(), 0);
        let data: BTreeMap<_, _> = guard
            .store()
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(guard.store_mut().digest(), compute_store_digest(&data));
    }
}
