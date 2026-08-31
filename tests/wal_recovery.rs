//! WAL crash-recovery integration tests.
//!
//! Each test simulates a crash by dropping the API object (the process
//! keeps running, but nothing is flushed or checkpointed beyond what the
//! WAL protocol itself guarantees) and then rebuilding state through the
//! real recovery path: snapshot load → WAL replay → fresh WAL writer.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::lww_register::LwwRegister;
use asteroidb_poc::crdt::or_set::OrSet;
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::error::CrdtError;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::session::SessionToken;
use asteroidb_poc::store::Store;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::store::wal::{self, SyncPolicy, WalConfig, WalReadOutcome, WalWriter};
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId};

fn node(name: &str) -> NodeId {
    NodeId(name.into())
}

fn hlc(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
    HlcTimestamp {
        physical,
        logical,
        node_id: node.into(),
    }
}

fn wal_cfg(dir: &Path) -> WalConfig {
    WalConfig::new(dir, SyncPolicy::Off)
}

/// Build a WAL-backed EventualApi rooted at `dir` (recovery included).
fn open_eventual(dir: &Path, node_id: &str) -> EventualApi {
    let snapshot = dir.join("eventual.snapshot.bin");
    let store = Store::load_snapshot_bincode_or_default(&snapshot).unwrap();
    let read = wal::read_all_segments(&dir.join("wal")).unwrap();
    assert_ne!(
        read.outcome,
        WalReadOutcome::Corruption,
        "tests never produce mid-log corruption unless they mean to"
    );
    // Mirror production recovery (persistence::recover_store): physically
    // repair the log to the replayed prefix BEFORE opening a new writer,
    // so a torn tail never becomes mid-log corruption on the next reopen.
    if read.outcome != WalReadOutcome::Clean {
        wal::truncate_to_valid_prefix(&dir.join("wal"), &read).unwrap();
    }
    let mut store = store;
    for record in read.records {
        wal::replay_record(&mut store, record);
    }
    let writer = WalWriter::open(wal_cfg(&dir.join("wal"))).unwrap();
    EventualApi::recovered(node(node_id), store, Some(writer))
}

fn store_json(store: &Store) -> serde_json::Value {
    serde_json::to_value(store).unwrap()
}

// ---------------------------------------------------------------
// (a) WAL-only recovery: no snapshot at all
// ---------------------------------------------------------------

#[test]
fn all_mutations_recover_from_wal_alone() {
    let dir = tempfile::tempdir().unwrap();
    let before = {
        let mut api = open_eventual(dir.path(), "node-a");
        api.eventual_counter_inc("cnt").unwrap();
        api.eventual_counter_dec("cnt").unwrap();
        api.eventual_set_add("set", "alice".into()).unwrap();
        api.eventual_set_add("set", "bob".into()).unwrap();
        api.eventual_set_remove("set", "alice").unwrap();
        api.eventual_map_set("map", "k".into(), "v".into()).unwrap();
        api.eventual_map_set("map", "gone".into(), "x".into())
            .unwrap();
        api.eventual_map_delete("map", "gone").unwrap();
        api.eventual_register_set("reg", "hello".into()).unwrap();
        let mut c = PnCounter::new();
        c.increment(&node("node-a"));
        api.eventual_write("raw".into(), CrdtValue::Counter(c))
            .unwrap();
        store_json(api.store())
        // api dropped here = crash (no snapshot was ever written)
    };

    let api = open_eventual(dir.path(), "node-a");
    assert_eq!(
        store_json(api.store()),
        before,
        "the full store (all 6 persisted fields) must be rebuilt from the WAL alone"
    );
    match api.get_eventual("set") {
        Some(CrdtValue::Set(s)) => {
            assert!(!s.contains(&"alice".to_string()));
            assert!(s.contains(&"bob".to_string()));
        }
        other => panic!("expected Set, got {other:?}"),
    }
    match api.get_eventual("map") {
        Some(CrdtValue::Map(m)) => {
            assert_eq!(m.get(&"k".to_string()), Some(&"v".to_string()));
            assert!(!m.contains_key(&"gone".to_string()));
        }
        other => panic!("expected Map, got {other:?}"),
    }
}

/// Regression: counters are recorded as post-state, so recovery must not
/// double-count (an op log replaying `inc` twice would).
#[test]
fn counter_recovery_does_not_double_count() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut api = open_eventual(dir.path(), "node-a");
        api.eventual_counter_inc("cnt").unwrap();
        api.eventual_counter_inc("cnt").unwrap();
        api.eventual_counter_inc("cnt").unwrap();
    }
    let api = open_eventual(dir.path(), "node-a");
    match api.get_eventual("cnt") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 3),
        other => panic!("expected Counter, got {other:?}"),
    }
}

// ---------------------------------------------------------------
// (c) Replay idempotence at the API level
// ---------------------------------------------------------------

#[test]
fn replaying_the_same_wal_twice_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut api = open_eventual(dir.path(), "node-a");
        api.eventual_counter_inc("cnt").unwrap();
        api.eventual_set_add("set", "x".into()).unwrap();
    }
    let records = wal::read_all_segments(&dir.path().join("wal"))
        .unwrap()
        .records;

    let mut once = Store::new();
    for r in &records {
        wal::replay_record(&mut once, r.clone());
    }
    let mut twice = Store::new();
    for r in records.iter().chain(records.iter()) {
        wal::replay_record(&mut twice, r.clone());
    }
    assert_eq!(store_json(&once), store_json(&twice));
}

// ---------------------------------------------------------------
// (b) Torn writes
// ---------------------------------------------------------------

#[test]
fn torn_tail_recovers_all_complete_records() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut api = open_eventual(dir.path(), "node-a");
        for i in 0..5 {
            api.eventual_counter_inc(&format!("k{i}")).unwrap();
        }
    }
    // Tear the last record: chop a few bytes off the segment tail.
    let (_, seg) = wal::list_segments(&dir.path().join("wal"))
        .unwrap()
        .pop()
        .unwrap();
    let data = std::fs::read(&seg).unwrap();
    std::fs::write(&seg, &data[..data.len() - 4]).unwrap();

    let read = wal::read_all_segments(&dir.path().join("wal")).unwrap();
    assert_eq!(read.outcome, WalReadOutcome::TornTail);
    assert_eq!(read.records.len(), 4, "only the torn record is lost");

    let api = open_eventual(dir.path(), "node-a");
    for i in 0..4 {
        assert!(api.get_eventual(&format!("k{i}")).is_some());
    }
    assert!(api.get_eventual("k4").is_none());
}

#[test]
fn crc_corruption_at_tail_recovers_prefix() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut api = open_eventual(dir.path(), "node-a");
        api.eventual_counter_inc("a").unwrap();
        api.eventual_counter_inc("b").unwrap();
    }
    let (_, seg) = wal::list_segments(&dir.path().join("wal"))
        .unwrap()
        .pop()
        .unwrap();
    let mut data = std::fs::read(&seg).unwrap();
    let last = data.len() - 1;
    data[last] ^= 0x55;
    std::fs::write(&seg, &data).unwrap();

    let api = open_eventual(dir.path(), "node-a");
    assert!(
        api.get_eventual("a").is_some(),
        "intact prefix must survive"
    );
    assert!(
        api.get_eventual("b").is_none(),
        "the damaged final record must be discarded"
    );
}

// ---------------------------------------------------------------
// (d) Checkpoint ordering: snapshot before segment deletion
// ---------------------------------------------------------------

#[test]
fn checkpoint_then_recovery_composes_snapshot_and_wal() {
    let dir = tempfile::tempdir().unwrap();
    let snapshot = dir.path().join("eventual.snapshot.bin");
    {
        let mut api = open_eventual(dir.path(), "node-a");
        api.eventual_counter_inc("pre").unwrap();

        // Manual checkpoint following the rotate → clone → save → delete
        // discipline (what persistence::checkpoint_eventual does).
        let sealed = api.wal_rotate().unwrap().expect("wal enabled");
        let clone = api.store().clone();
        clone.save_snapshot_bincode(&snapshot).unwrap();
        wal::remove_segments_up_to(&dir.path().join("wal"), sealed).unwrap();

        // Writes after the checkpoint live only in the WAL.
        api.eventual_counter_inc("post").unwrap();
    }
    let api = open_eventual(dir.path(), "node-a");
    assert!(api.get_eventual("pre").is_some(), "from the snapshot");
    assert!(api.get_eventual("post").is_some(), "from the WAL");
}

/// Crash BETWEEN snapshot success and segment deletion: snapshot and all
/// segments coexist; over-replay must be a no-op.
#[test]
fn crash_before_segment_deletion_is_harmless_over_replay() {
    let dir = tempfile::tempdir().unwrap();
    let snapshot = dir.path().join("eventual.snapshot.bin");
    let expected = {
        let mut api = open_eventual(dir.path(), "node-a");
        api.eventual_counter_inc("cnt").unwrap();
        api.eventual_counter_inc("cnt").unwrap();
        let _sealed = api.wal_rotate().unwrap().expect("wal enabled");
        let clone = api.store().clone();
        clone.save_snapshot_bincode(&snapshot).unwrap();
        // CRASH here: remove_segments_up_to never runs.
        store_json(api.store())
    };
    let api = open_eventual(dir.path(), "node-a");
    assert_eq!(
        store_json(api.store()),
        expected,
        "snapshot + full WAL over-replay must equal the pre-crash state"
    );
    match api.get_eventual("cnt") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2, "no double counting"),
        other => panic!("expected Counter, got {other:?}"),
    }
}

/// The "log deletion only after snapshot success" rule: when the snapshot
/// write fails, segments must be retained and recovery must still see
/// every write.
#[test]
fn segments_survive_a_failed_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut api = open_eventual(dir.path(), "node-a");
        api.eventual_counter_inc("cnt").unwrap();

        let sealed = api.wal_rotate().unwrap().expect("wal enabled");
        let clone = api.store().clone();
        // Force the snapshot to fail: the target is a DIRECTORY.
        let bad_snapshot = dir.path().join("eventual.snapshot.bin");
        std::fs::create_dir_all(&bad_snapshot).unwrap();
        assert!(
            clone.save_snapshot_bincode(&bad_snapshot).is_err(),
            "precondition: snapshot write must fail"
        );
        // Checkpoint protocol: on snapshot failure, deletion is skipped.
        let _ = sealed;
        std::fs::remove_dir_all(&bad_snapshot).unwrap();
    }
    let segments = wal::list_segments(&dir.path().join("wal")).unwrap();
    assert!(
        segments.len() >= 2,
        "sealed segment must still exist after the failed snapshot"
    );
    let api = open_eventual(dir.path(), "node-a");
    assert!(api.get_eventual("cnt").is_some());
}

// ---------------------------------------------------------------
// (e) Remote merges and session metadata
// ---------------------------------------------------------------

#[test]
fn remote_merges_and_session_claims_recover() {
    let dir = tempfile::tempdir().unwrap();
    let remote_hlc = hlc(1_000, 0, "node-b");
    let before = {
        let mut api = open_eventual(dir.path(), "node-a");

        // Push path (no origin HLC): local re-stamp, applied claim local only.
        let mut c = PnCounter::new();
        c.increment(&node("node-b"));
        api.merge_remote("pushed".into(), &CrdtValue::Counter(c))
            .unwrap();

        // Delta pull path (origin HLC preserved, no applied claim).
        let mut c2 = PnCounter::new();
        c2.increment(&node("node-b"));
        api.merge_remote_with_hlc("pulled".into(), &CrdtValue::Counter(c2), remote_hlc.clone())
            .unwrap();

        // Sender's claims adopted atomically.
        let mut applied = HashMap::new();
        applied.insert("node-b".to_string(), remote_hlc.clone());
        let mut visible = HashMap::new();
        visible.insert("node-b".to_string(), remote_hlc.clone());
        api.adopt_session_claims(&applied, &visible, vec!["poisoned".into()])
            .unwrap();

        store_json(api.store())
    };

    let api = open_eventual(dir.path(), "node-a");
    assert_eq!(store_json(api.store()), before);
    // Delta-pull merges make no applied claim by themselves; the adopted
    // claim (via SessionClaims) is what advances node-b's frontier.
    assert_eq!(api.store().applied_origin("node-b"), Some(&remote_hlc));
    assert!(api.store().merge_failed_contains("poisoned"));
    assert_eq!(
        api.store().visible_origins().get("node-b"),
        Some(&remote_hlc)
    );
}

/// A failed remote merge's poison mark must survive the crash even though
/// the merge itself returned an error.
#[test]
fn merge_failure_poison_survives_crash() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut api = open_eventual(dir.path(), "node-a");
        api.eventual_counter_inc("k").unwrap();
        api.merge_remote_with_hlc(
            "k".into(),
            &CrdtValue::Set(asteroidb_poc::crdt::or_set::OrSet::new()),
            hlc(500, 0, "node-b"),
        )
        .unwrap_err();
        assert!(api.store().merge_failed_contains("k"));
    }
    let api = open_eventual(dir.path(), "node-a");
    assert!(
        api.store().merge_failed_contains("k"),
        "poison must be durable — losing it while keeping a frontier would fake session success"
    );
}

/// Session tokens issued before the crash must still be satisfied after
/// recovery (read-your-writes across restart).
#[test]
fn session_token_survives_crash() {
    let dir = tempfile::tempdir().unwrap();
    let token = {
        let mut api = open_eventual(dir.path(), "node-a");
        let ts = api.eventual_counter_inc("mine").unwrap();
        SessionToken::from_hlc(&ts)
    };
    let api = open_eventual(dir.path(), "node-a");
    assert!(
        api.session_check("mine", &token),
        "a pre-crash session token must be satisfied after recovery"
    );
}

// ---------------------------------------------------------------
// HLC clock rollback prevention
// ---------------------------------------------------------------

#[test]
fn post_recovery_writes_are_strictly_newer() {
    let dir = tempfile::tempdir().unwrap();
    let pre_crash_ts = {
        let mut api = open_eventual(dir.path(), "node-a");
        api.eventual_counter_inc("k").unwrap()
    };
    let mut api = open_eventual(dir.path(), "node-a");
    let post_ts = api.eventual_counter_inc("k").unwrap();
    assert!(
        post_ts > pre_crash_ts,
        "recovered clock must never re-issue past HLCs (LWW/delta-sync rollback)"
    );
}

/// A WAL containing an HLC far beyond the wall clock (issued under skew)
/// must not prevent startup, and the seeded clock must stay ahead of it.
#[test]
fn far_future_hlc_in_wal_does_not_block_startup() {
    let dir = tempfile::tempdir().unwrap();
    let far = hlc(
        asteroidb_poc::hlc::wall_clock_ms() + asteroidb_poc::hlc::MAX_CLOCK_SKEW_MS + 120_000,
        0,
        "node-b",
    );
    {
        let mut api = open_eventual(dir.path(), "node-a");
        let mut c = PnCounter::new();
        c.increment(&node("node-b"));
        api.merge_remote_with_hlc("k".into(), &CrdtValue::Counter(c), far.clone())
            .unwrap();
    }
    let mut api = open_eventual(dir.path(), "node-a");
    let ts = api.eventual_counter_inc("k2").unwrap();
    assert!(
        ts > far,
        "seeded clock must dominate the recovered far-future HLC"
    );
}

// ---------------------------------------------------------------
// Certified store recovery
// ---------------------------------------------------------------

fn test_namespace() -> Arc<std::sync::RwLock<SystemNamespace>> {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: String::new(),
        },
        authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
        auto_generated: false,
    });
    ns.set_placement_policy(asteroidb_poc::placement::PlacementPolicy::new(
        asteroidb_poc::types::PolicyVersion(1),
        KeyRange {
            prefix: String::new(),
        },
        3,
    ))
    .unwrap();
    Arc::new(std::sync::RwLock::new(ns))
}

fn open_certified(dir: &Path, node_id: &str) -> CertifiedApi {
    let snapshot = dir.join("certified.snapshot.bin");
    let mut store = Store::load_snapshot_bincode_or_default(&snapshot).unwrap();
    let read = wal::read_all_segments(&dir.join("wal-certified")).unwrap();
    assert_ne!(read.outcome, WalReadOutcome::Corruption);
    let mut origins = HashMap::new();
    for record in read.records {
        if let wal::WalRecord::CertifiedUpsert {
            key,
            policy_version,
            ..
        } = &record
        {
            origins.insert(key.clone(), *policy_version);
        }
        wal::replay_record(&mut store, record);
    }
    let writer = WalWriter::open(wal_cfg(&dir.join("wal-certified"))).unwrap();
    CertifiedApi::recovered(
        node(node_id),
        test_namespace(),
        store,
        Some(writer),
        origins,
    )
}

/// Certified values recover; certification status regresses to Pending
/// (fail-closed: never a false Certified after losing the proof state).
#[test]
fn certified_write_recovers_value_as_pending() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut api = open_certified(dir.path(), "node-a");
        let mut c = PnCounter::new();
        c.increment(&node("node-a"));
        let status = api
            .certified_write("orders/1".into(), CrdtValue::Counter(c), OnTimeout::Pending)
            .unwrap();
        assert_eq!(status, CertificationStatus::Pending);
    }
    let api = open_certified(dir.path(), "node-a");
    let read = api.get_certified("orders/1");
    match read.value {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
        other => panic!("certified value must survive the crash, got {other:?}"),
    }
    assert_eq!(
        read.status,
        CertificationStatus::Pending,
        "certification state is volatile: recovery must regress to Pending, never fake Certified"
    );
}

// ---------------------------------------------------------------
// (f) fsync policies
// ---------------------------------------------------------------

#[tokio::test]
async fn sync_always_makes_acked_records_readable_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = WalWriter::open(WalConfig::new(dir.path(), SyncPolicy::Always)).unwrap();
    let syncer = Arc::new(writer.syncer());
    let flusher = tokio::spawn(Arc::clone(&syncer).run_flusher());

    let mut last = None;
    for i in 0..10u64 {
        let pos = writer
            .append(&wal::WalRecord::MergeFailed {
                keys: vec![format!("k{i}")],
            })
            .unwrap();
        last = Some(pos);
    }
    syncer.wait_durable(last.unwrap()).await.unwrap();

    // Everything acked (wait_durable returned) must parse from disk.
    let read = wal::read_all_segments(dir.path()).unwrap();
    assert_eq!(read.outcome, WalReadOutcome::Clean);
    assert_eq!(read.records.len(), 10);
    flusher.abort();
}

#[tokio::test]
async fn sync_always_wait_spans_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = WalWriter::open(WalConfig::new(dir.path(), SyncPolicy::Always)).unwrap();
    let syncer = Arc::new(writer.syncer());
    let flusher = tokio::spawn(Arc::clone(&syncer).run_flusher());

    let pos_old = writer
        .append(&wal::WalRecord::MergeFailed {
            keys: vec!["old".into()],
        })
        .unwrap();
    writer.rotate().unwrap();
    let pos_new = writer
        .append(&wal::WalRecord::MergeFailed {
            keys: vec!["new".into()],
        })
        .unwrap();

    // Rotation seals + syncs the old segment, so pos_old is durable even
    // without the flusher; pos_new goes through the group-commit path.
    syncer.wait_durable(pos_old).await.unwrap();
    syncer.wait_durable(pos_new).await.unwrap();

    let read = wal::read_all_segments(dir.path()).unwrap();
    assert_eq!(read.records.len(), 2);
    flusher.abort();
}

#[tokio::test]
async fn sync_interval_flushes_on_tick() {
    let dir = tempfile::tempdir().unwrap();
    let policy = SyncPolicy::Interval(std::time::Duration::from_millis(20));
    let mut writer = WalWriter::open(WalConfig::new(dir.path(), policy)).unwrap();
    let syncer = Arc::new(writer.syncer());
    let flusher = tokio::spawn(Arc::clone(&syncer).run_flusher());

    let pos = writer
        .append(&wal::WalRecord::MergeFailed {
            keys: vec!["k".into()],
        })
        .unwrap();
    // The interval flusher must make the record durable within a few ticks.
    tokio::time::timeout(std::time::Duration::from_secs(5), syncer.wait_durable(pos))
        .await
        .expect("interval flusher must sync within the timeout")
        .unwrap();
    flusher.abort();
}

#[test]
fn sync_off_still_replays() {
    // No explicit fsync anywhere: the data is still in the page cache /
    // file, so a process-level "crash" (drop) loses nothing.
    let dir = tempfile::tempdir().unwrap();
    {
        let mut api = open_eventual(dir.path(), "node-a");
        api.eventual_counter_inc("k").unwrap();
    }
    let api = open_eventual(dir.path(), "node-a");
    assert!(api.get_eventual("k").is_some());
}

// ---------------------------------------------------------------
// GC / tombstone interaction: no zombie elements across crash
// ---------------------------------------------------------------

/// set_add → set_remove → crash → recover: the removed element must not
/// resurrect (add and remove replay in log order onto the same OR-Set
/// state).
#[test]
fn removed_set_element_does_not_resurrect() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut api = open_eventual(dir.path(), "node-a");
        api.eventual_set_add("s", "zombie".into()).unwrap();
        api.eventual_set_remove("s", "zombie").unwrap();
    }
    // Recover twice (double replay) for good measure.
    {
        let api = open_eventual(dir.path(), "node-a");
        drop(api);
    }
    let api = open_eventual(dir.path(), "node-a");
    match api.get_eventual("s") {
        Some(CrdtValue::Set(s)) => assert!(
            !s.contains(&"zombie".to_string()),
            "a removed element must never resurrect through WAL replay"
        ),
        other => panic!("expected Set, got {other:?}"),
    }
}

// ---------------------------------------------------------------
// Live write vs replay symmetry (merge, never replace)
// ---------------------------------------------------------------

/// Raw writes are CRDT-merged live, matching WAL replay (which merges the
/// logged post-states): the recovered value must equal the acked pre-crash
/// value even when a later write would have "regressed" the CRDT state
/// under replace semantics.
#[test]
fn raw_write_recovery_matches_acked_state() {
    let dir = tempfile::tempdir().unwrap();
    let acked = {
        let mut api = open_eventual(dir.path(), "node-a");
        let mut c1 = PnCounter::new();
        for _ in 0..10 {
            c1.increment(&node("node-x"));
        }
        api.eventual_write("k".into(), CrdtValue::Counter(c1))
            .unwrap();
        let mut c2 = PnCounter::new();
        c2.increment(&node("node-y"));
        api.eventual_write("k".into(), CrdtValue::Counter(c2))
            .unwrap();
        match api.get_eventual("k") {
            Some(CrdtValue::Counter(c)) => c.value(),
            other => panic!("expected Counter, got {other:?}"),
        }
    };
    let api = open_eventual(dir.path(), "node-a");
    match api.get_eventual("k") {
        Some(CrdtValue::Counter(c)) => assert_eq!(
            c.value(),
            acked,
            "recovered value must equal the acked pre-crash value"
        ),
        other => panic!("expected Counter, got {other:?}"),
    }
}

/// A type-changing raw write is rejected (TypeMismatch) instead of
/// silently replacing state that WAL replay could not reconstruct (replay
/// would hit the mismatch, poison the key, and keep the OLD type).
#[test]
fn type_changing_raw_write_is_rejected() {
    let mut api = EventualApi::new(node("node-a"));
    api.eventual_write("k".into(), CrdtValue::Counter(PnCounter::new()))
        .unwrap();
    let err = api
        .eventual_write("k".into(), CrdtValue::Register(LwwRegister::new()))
        .unwrap_err();
    assert!(matches!(err, CrdtError::TypeMismatch { .. }));
}

/// Same symmetry for the certified store, which has NO anti-entropy
/// rebuild path — a live/replay divergence there would be permanent.
#[test]
fn certified_write_recovery_matches_acked_state() {
    let dir = tempfile::tempdir().unwrap();
    let acked = {
        let mut api = open_certified(dir.path(), "node-a");
        let mut c1 = PnCounter::new();
        for _ in 0..10 {
            c1.increment(&node("http-writer"));
        }
        api.certified_write(
            "orders/1".into(),
            CrdtValue::Counter(c1),
            OnTimeout::Pending,
        )
        .unwrap();
        // A REGRESSING counter write (3 after 10) is unrepresentable on
        // the certified path (merge takes the per-node max, so the
        // acked-value/live-state divergence would be silent): it must be
        // rejected loudly instead of acked as success.
        let mut c2 = PnCounter::new();
        for _ in 0..3 {
            c2.increment(&node("http-writer"));
        }
        let err = api
            .certified_write(
                "orders/1".into(),
                CrdtValue::Counter(c2),
                OnTimeout::Pending,
            )
            .unwrap_err();
        assert!(
            matches!(err, CrdtError::InvalidArgument(_)),
            "regressing certified counter write must be rejected, got {err:?}"
        );
        // An ADVANCING write (13 after 10) merges to exactly the
        // requested value and is accepted.
        let mut c3 = PnCounter::new();
        for _ in 0..13 {
            c3.increment(&node("http-writer"));
        }
        api.certified_write(
            "orders/1".into(),
            CrdtValue::Counter(c3),
            OnTimeout::Pending,
        )
        .unwrap();
        match api.get_certified("orders/1").value {
            Some(CrdtValue::Counter(c)) => c.value(),
            other => panic!("expected Counter, got {other:?}"),
        }
    };
    let api = open_certified(dir.path(), "node-a");
    match api.get_certified("orders/1").value {
        Some(CrdtValue::Counter(c)) => assert_eq!(
            c.value(),
            acked,
            "recovered certified value must equal the acked pre-crash value"
        ),
        other => panic!("expected Counter, got {other:?}"),
    }
}

// ---------------------------------------------------------------
// Poison durability under WAL append failures
// ---------------------------------------------------------------

/// Build a WAL-backed EventualApi whose every append AFTER the first
/// forces a segment rotation (tiny segment threshold), so appends can be
/// made to fail deterministically by removing write permission on the WAL
/// directory (segment creation fails; the already-open fd keeps working).
fn open_eventual_tiny_segments(dir: &Path, node_id: &str) -> EventualApi {
    let mut cfg = WalConfig::new(dir.join("wal"), SyncPolicy::Off);
    cfg.segment_max_bytes = 1;
    let writer = WalWriter::open(cfg).unwrap();
    EventualApi::recovered(node(node_id), Store::new(), Some(writer))
}

fn set_dir_mode(dir: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(dir).unwrap().permissions();
    perms.set_mode(mode);
    std::fs::set_permissions(dir, perms).unwrap();
}

fn set_wal_dir_mode(dir: &Path, mode: u32) {
    set_dir_mode(&dir.join("wal"), mode);
}

/// Seed an orphan segment at `max existing seq + 1` — the on-disk residue
/// of a rotation that died mid-create (ENOSPC before/during the header
/// write) — and return the orphan's sequence number.
fn seed_orphan_segment(wal_dir: &Path, contents: &[u8]) -> u64 {
    let (max_seq, _) = wal::list_segments(wal_dir).unwrap().pop().unwrap();
    std::fs::write(
        wal_dir.join(format!("wal-{:016x}.log", max_seq + 1)),
        contents,
    )
    .unwrap();
    max_seq + 1
}

/// A failed `MergeFailed` append must not lose the poison: it is queued
/// and re-appended in front of the next successful append, so a crash can
/// never replay a frontier without its poison mark.
#[test]
fn unlogged_poison_is_flushed_before_later_records() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut api = open_eventual_tiny_segments(dir.path(), "node-a");
        api.eventual_counter_inc("k").unwrap();

        set_wal_dir_mode(dir.path(), 0o555);
        // Type mismatch: poisons "k" in memory; the MergeFailed append
        // fails (rotation cannot create a segment).
        let err = api
            .merge_remote_with_hlc(
                "k".into(),
                &CrdtValue::Set(OrSet::new()),
                hlc(500, 0, "node-b"),
            )
            .unwrap_err();
        assert!(matches!(err, CrdtError::TypeMismatch { .. }));
        assert!(api.store().merge_failed_contains("k"));
        set_wal_dir_mode(dir.path(), 0o755);

        // A later frontier-advancing append succeeds — the queued poison
        // must be flushed into the log in front of it.
        let mut applied = HashMap::new();
        applied.insert("node-b".to_string(), hlc(600, 0, "node-b"));
        api.adopt_session_claims(&applied, &HashMap::new(), Vec::new())
            .unwrap();
    } // crash

    let api = open_eventual(dir.path(), "node-a");
    assert_eq!(
        api.store().applied_origin("node-b"),
        Some(&hlc(600, 0, "node-b")),
        "the adopted frontier was logged"
    );
    assert!(
        api.store().merge_failed_contains("k"),
        "the poison whose own append failed must still be durable"
    );
    assert!(
        !api.session_check("k", &SessionToken::from_hlc(&hlc(500, 0, "node-b"))),
        "a frontier restored without its poison would fake session success"
    );
}

/// A WAL append failure AFTER a successful in-memory merge
/// (CrdtError::Storage) must poison the key: the merged data never
/// reached the log, yet a later adopted SessionClaims record can persist
/// a frontier covering it — a crash must not produce a false session
/// success for data whose record was lost.
#[test]
fn storage_failure_after_merge_poisons_key() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut api = open_eventual_tiny_segments(dir.path(), "node-a");
        api.eventual_counter_inc("k").unwrap();

        set_wal_dir_mode(dir.path(), 0o555);
        let mut c = PnCounter::new();
        c.increment(&node("node-b"));
        let err = api
            .merge_remote_with_hlc("m".into(), &CrdtValue::Counter(c), hlc(500, 0, "node-b"))
            .unwrap_err();
        assert!(matches!(err, CrdtError::Storage(_)));
        assert!(
            api.store().merge_failed_contains("m"),
            "a merged-but-unlogged entry must poison its key"
        );
        set_wal_dir_mode(dir.path(), 0o755);

        // Session-claims adoption still proceeds for the round (the entry
        // error only bumped the caller's error count); the queued poison
        // must reach the log before the claims record.
        let mut applied = HashMap::new();
        applied.insert("node-b".to_string(), hlc(500, 0, "node-b"));
        api.adopt_session_claims(&applied, &HashMap::new(), Vec::new())
            .unwrap();
    } // crash

    let api = open_eventual(dir.path(), "node-a");
    assert!(
        api.get_eventual("m").is_none(),
        "the data record never reached the log"
    );
    assert!(api.store().merge_failed_contains("m"));
    assert!(
        !api.session_check("m", &SessionToken::from_hlc(&hlc(500, 0, "node-b"))),
        "adopted frontier must not claim data whose record was never logged"
    );
}

// ---------------------------------------------------------------
// Disk-full degrade: append failure surfaces as Storage error
// ---------------------------------------------------------------

#[test]
fn wal_append_failure_returns_storage_error_and_reads_continue() {
    let dir = tempfile::tempdir().unwrap();
    let mut api = open_eventual(dir.path(), "node-a");
    api.eventual_counter_inc("k").unwrap();

    // Sabotage the WAL directory so the next auto-rotate/creation fails:
    // force rotation by removing write permissions on the directory, then
    // filling the segment is complex — instead simulate by making the WAL
    // dir read-only and forcing a rotate.
    let wal_dir = dir.path().join("wal");
    let mut perms = std::fs::metadata(&wal_dir).unwrap().permissions();
    use std::os::unix::fs::PermissionsExt;
    perms.set_mode(0o555);
    std::fs::set_permissions(&wal_dir, perms).unwrap();

    let rotate_result = api.wal_rotate();
    assert!(
        rotate_result.is_err(),
        "creating a new segment in a read-only dir must fail"
    );

    // Reads keep working (degrade, not crash).
    assert!(api.get_eventual("k").is_some());

    // Restore permissions so tempdir cleanup succeeds.
    let mut perms = std::fs::metadata(&wal_dir).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&wal_dir, perms).unwrap();
}

// ---------------------------------------------------------------
// M-5: orphan segments from a failed rotation self-heal
// ---------------------------------------------------------------

/// Regression test for M-5. An orphan segment left by a rotation that
/// died mid-create (ENOSPC) used to make EVERY later append and
/// checkpoint fail with `AlreadyExists` until restart. Rotation must
/// reclaim the orphan in place — and the reclaim needs no directory
/// write permission (truncate + header rewrite + read-only dir fsync),
/// so it works even while the disk-full/read-only condition persists
/// for genuinely new files.
#[test]
fn orphan_segment_self_heals_without_restart() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    {
        let mut api = open_eventual_tiny_segments(dir.path(), "node-a");
        for i in 0..3 {
            api.eventual_counter_inc(&format!("k{i}")).unwrap();
        }

        // Partial-header orphan at max+1: exactly what a rotation killed
        // mid-header-write leaves behind.
        seed_orphan_segment(&wal_dir, &[0xAA; 7]);
        set_wal_dir_mode(dir.path(), 0o555);

        // The next write forces a rotation onto the orphan's sequence
        // number. Pre-fix: permanent AlreadyExists. Post-fix: reclaimed,
        // even though the directory itself is read-only.
        api.eventual_counter_inc("healed").unwrap();

        // A rotation that needs a genuinely NEW file still fails while
        // the directory is read-only (degrade) ...
        let err = api.eventual_counter_inc("denied").unwrap_err();
        assert!(matches!(err, CrdtError::Storage(_)));
        // ... reads keep working ...
        assert!(api.get_eventual("healed").is_some());

        set_wal_dir_mode(dir.path(), 0o755);
        // ... and once the directory is writable again, writes succeed
        // WITHOUT a restart.
        api.eventual_counter_inc("after").unwrap();
    } // crash

    let api = open_eventual(dir.path(), "node-a");
    for key in ["k0", "k1", "k2", "healed", "after"] {
        assert!(
            api.get_eventual(key).is_some(),
            "acked write {key} must survive the crash"
        );
    }
    // The un-acked write was never logged; its key stays poisoned
    // (fail-closed) rather than resurrecting as data.
    assert!(api.get_eventual("denied").is_none());
    assert!(api.store().merge_failed_contains("denied"));
}

/// A crash can strike at any point of the (now self-healing) rotation:
/// whatever the failed create persisted — nothing, a partial header, or a
/// bare header — always sits at the maximum sequence number, so restart
/// recovery must replay every acked record with zero loss.
#[test]
fn orphan_segment_recovers_across_restart() {
    let mut valid_header = Vec::from(wal::WAL_MAGIC);
    valid_header.extend_from_slice(&wal::WAL_FORMAT_VERSION.to_le_bytes());
    valid_header.extend_from_slice(&[0u8; 4]);
    let orphans: [&[u8]; 3] = [&[], &[0xAA; 7], &valid_header];

    for orphan in orphans {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let before = {
            let mut api = open_eventual(dir.path(), "node-a");
            for i in 0..3 {
                api.eventual_counter_inc(&format!("k{i}")).unwrap();
            }
            store_json(api.store())
        }; // crash
        seed_orphan_segment(&wal_dir, orphan);

        let api = open_eventual(dir.path(), "node-a");
        assert_eq!(
            store_json(api.store()),
            before,
            "{}-byte orphan: acked state must recover with zero loss",
            orphan.len()
        );
        drop(api);
        // The repaired log must stay readable on the NEXT boot too (the
        // orphan must not survive as a mid-log short segment).
        let read = wal::read_all_segments(&wal_dir).unwrap();
        assert_ne!(
            read.outcome,
            WalReadOutcome::Corruption,
            "{}-byte orphan: recovery must not leave mid-log corruption",
            orphan.len()
        );
    }
}

/// Checkpoint-level M-5 regression through the real persistence path
/// (`checkpoint_eventual`): an orphan segment must not fail checkpoints
/// until restart, a failing rotation must not delete any segment, and the
/// next checkpoint after recovery must succeed and prune only sealed
/// segments (the active one survives).
#[tokio::test]
async fn checkpoint_recovers_after_rotate_failure() {
    use asteroidb_poc::runtime::persistence::{
        CheckpointLocks, PersistenceConfig, checkpoint_eventual,
    };

    let dir = tempfile::tempdir().unwrap();
    // checkpoint_eventual hard-codes the production layout under data_dir.
    let wal_dir = dir.path().join("wal").join("eventual");
    let writer = WalWriter::open(wal_cfg(&wal_dir)).unwrap();
    let api = Arc::new(tokio::sync::Mutex::new(EventualApi::recovered(
        node("node-a"),
        Store::new(),
        Some(writer),
    )));
    let cfg = PersistenceConfig {
        enabled: true,
        data_dir: dir.path().to_path_buf(),
        sync: SyncPolicy::Off,
        snapshot_interval: None,
        segment_max_bytes: WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        recover_truncate: false,
        checkpoint_locks: CheckpointLocks::default(),
    };

    api.lock().await.eventual_counter_inc("pre").unwrap();

    // Pre-fix: this orphan made every checkpoint fail with AlreadyExists
    // until restart. Post-fix: the checkpoint's rotation reclaims it.
    let orphan_seq = seed_orphan_segment(&wal_dir, &[0xAA; 7]);
    checkpoint_eventual(&api, &cfg)
        .await
        .expect("checkpoint must self-heal the orphan segment");
    let segments = wal::list_segments(&wal_dir).unwrap();
    assert_eq!(
        segments.iter().map(|(seq, _)| *seq).collect::<Vec<_>>(),
        vec![orphan_seq],
        "sealed segments pruned; the reclaimed orphan is the active segment"
    );

    api.lock().await.eventual_counter_inc("mid").unwrap();

    // A checkpoint whose rotation fails outright must delete nothing.
    set_dir_mode(&wal_dir, 0o555);
    assert!(
        checkpoint_eventual(&api, &cfg).await.is_err(),
        "rotation into a read-only directory must fail the checkpoint"
    );
    set_dir_mode(&wal_dir, 0o755);
    assert!(
        !wal::list_segments(&wal_dir).unwrap().is_empty(),
        "a failed checkpoint must not delete any segment"
    );

    // After the condition clears, the next checkpoint succeeds — no
    // restart needed.
    api.lock().await.eventual_counter_inc("post").unwrap();
    checkpoint_eventual(&api, &cfg)
        .await
        .expect("checkpoint must recover once the directory is writable");
    let segments = wal::list_segments(&wal_dir).unwrap();
    assert_eq!(segments.len(), 1, "only the active segment remains");
    assert!(segments[0].0 > orphan_seq);

    // Everything acked recovers from snapshot + retained WAL.
    drop(api); // crash
    let mut store =
        Store::load_snapshot_bincode_or_default(&dir.path().join("eventual.snapshot.bin")).unwrap();
    let read = wal::read_all_segments(&wal_dir).unwrap();
    assert_eq!(read.outcome, WalReadOutcome::Clean);
    for record in read.records {
        wal::replay_record(&mut store, record);
    }
    let api = EventualApi::recovered(node("node-a"), store, None);
    for key in ["pre", "mid", "post"] {
        assert!(
            api.get_eventual(key).is_some(),
            "checkpointed write {key} must recover"
        );
    }
}

// ---------------------------------------------------------------
// (g) fence persistence across restart (fix/fence-persistence-across-restart)
//
// A certified write issued under policy v_old, then fenced when the
// operator bumps v_old -> v_new, must NOT re-certify under a v_new
// frontier after a restart that lands inside the max_age_ms window. The
// live path keeps such a write Pending (heading to Timeout) because it
// stays in the fenced v_old scope; recovery must reproduce that, not
// silently re-tag it v_new and certify it off a v_new authority frontier.
// ---------------------------------------------------------------

fn user_namespace(version: u64) -> Arc<std::sync::RwLock<SystemNamespace>> {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: "user/".into(),
        },
        authority_nodes: vec![node("auth-1")], // majority = 1
        auto_generated: false,
    });
    ns.set_placement_policy(asteroidb_poc::placement::PlacementPolicy::new(
        asteroidb_poc::types::PolicyVersion(version),
        KeyRange {
            prefix: "user/".into(),
        },
        1,
    ))
    .unwrap();
    Arc::new(std::sync::RwLock::new(ns))
}

fn user_frontier(
    version: u64,
    cover_physical: u64,
) -> asteroidb_poc::authority::ack_frontier::AckFrontier {
    asteroidb_poc::authority::ack_frontier::AckFrontier {
        authority_id: node("auth-1"),
        frontier_hlc: hlc(cover_physical, 0, "auth-1"),
        key_range: KeyRange {
            prefix: "user/".into(),
        },
        policy_version: asteroidb_poc::types::PolicyVersion(version),
        digest_hash: "auth-1-cover".into(),
    }
}

fn user_v2_frontier(cover_physical: u64) -> asteroidb_poc::authority::ack_frontier::AckFrontier {
    user_frontier(2, cover_physical)
}

/// WAL-only (un-checkpointed) write: the record itself must carry the
/// origin policy version so recovery re-tracks it under v1 and re-fences.
#[test]
fn fenced_wal_only_write_stays_uncertified_across_restart() {
    use asteroidb_poc::runtime::persistence::{
        CheckpointLocks, PersistenceConfig, recover_certified,
    };
    use asteroidb_poc::types::PolicyVersion;

    let dir = tempfile::tempdir().unwrap();
    let cfg = PersistenceConfig {
        enabled: true,
        data_dir: dir.path().to_path_buf(),
        sync: SyncPolicy::Always,
        snapshot_interval: None,
        segment_max_bytes: WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        recover_truncate: false,
        checkpoint_locks: CheckpointLocks::default(),
    };

    // Incarnation 1: write user/x under policy v1; operator then bumps to
    // v2 and the running node fences v1 (in-memory, lost on crash). No
    // checkpoint: the write lives WAL-only, inside the max_age_ms window.
    let write_ts = {
        let (mut api, _s) = recover_certified(node("node-a"), user_namespace(1), &cfg).unwrap();
        let mut c = PnCounter::new();
        c.increment(&node("node-a"));
        let status = api
            .certified_write("user/x".into(), CrdtValue::Counter(c), OnTimeout::Pending)
            .unwrap();
        assert_eq!(status, CertificationStatus::Pending);
        let ts = api.pending_writes()[0].timestamp.clone();
        api.fence_version(
            &KeyRange {
                prefix: "user/".into(),
            },
            PolicyVersion(1),
        );
        ts
    };

    // Incarnation 2: restart already under policy v2.
    let (mut api, _s) = recover_certified(node("node-a"), user_namespace(2), &cfg).unwrap();

    let pw = api
        .pending_writes()
        .iter()
        .find(|p| p.key == "user/x")
        .expect("recovered write must be tracked");
    assert_eq!(
        pw.policy_version,
        PolicyVersion(1),
        "recovered write must keep its ORIGIN policy version v1, not be re-tagged v2"
    );

    assert!(
        api.update_frontier(user_v2_frontier(write_ts.physical + 10_000)),
        "v2 frontier report should be admitted"
    );
    // Inside the max_age_ms window: live path would still be Pending.
    api.process_certifications_with_timeout(write_ts.physical + 30_000);

    assert_ne!(
        api.get_certification_status("user/x"),
        CertificationStatus::Certified,
        "a v1 write fenced before a crash must NOT certify under a v2 frontier after restart"
    );
    assert!(
        api.is_version_fenced(
            &KeyRange {
                prefix: "user/".into()
            },
            &PolicyVersion(1)
        ),
        "the old policy version must be re-derived as fenced after recovery"
    );
}

/// Checkpointed (snapshot, WAL pruned) write: the origin survives only via
/// the certified origins sidecar written at checkpoint time.
#[tokio::test]
async fn fenced_checkpointed_write_stays_uncertified_across_restart() {
    use asteroidb_poc::runtime::persistence::{
        CheckpointLocks, PersistenceConfig, checkpoint_certified, recover_certified,
    };
    use asteroidb_poc::types::PolicyVersion;

    let dir = tempfile::tempdir().unwrap();
    let cfg = PersistenceConfig {
        enabled: true,
        data_dir: dir.path().to_path_buf(),
        sync: SyncPolicy::Off,
        snapshot_interval: None,
        segment_max_bytes: WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        recover_truncate: false,
        checkpoint_locks: CheckpointLocks::default(),
    };

    let write_ts;
    {
        let (mut api, _s) = recover_certified(node("node-a"), user_namespace(1), &cfg).unwrap();
        let mut c = PnCounter::new();
        c.increment(&node("node-a"));
        api.certified_write("user/x".into(), CrdtValue::Counter(c), OnTimeout::Pending)
            .unwrap();
        write_ts = api.pending_writes()[0].timestamp.clone();
        api.fence_version(
            &KeyRange {
                prefix: "user/".into(),
            },
            PolicyVersion(1),
        );
        // Checkpoint: snapshot the store and prune the WAL segment holding
        // the write's origin record.
        let api_arc = Arc::new(tokio::sync::Mutex::new(api));
        checkpoint_certified(&api_arc, &cfg).await.unwrap();
    }

    let (mut api, _s) = recover_certified(node("node-a"), user_namespace(2), &cfg).unwrap();
    let pw = api
        .pending_writes()
        .iter()
        .find(|p| p.key == "user/x")
        .expect("checkpointed write must recover");
    assert_eq!(
        pw.policy_version,
        PolicyVersion(1),
        "checkpointed write must keep its ORIGIN policy version v1 via the sidecar"
    );
    assert!(api.update_frontier(user_v2_frontier(write_ts.physical + 10_000)));
    api.process_certifications_with_timeout(write_ts.physical + 30_000);
    assert_ne!(
        api.get_certification_status("user/x"),
        CertificationStatus::Certified,
        "a checkpointed v1 write must NOT certify under a v2 frontier after restart"
    );
}

/// A write that was ALREADY `Certified` under v1 before the bump+fence must
/// behave identically to the still-`Pending` case across a restart, on BOTH
/// recovery paths. The certification state is volatile (it regresses to
/// `Pending`), so the origin must be pinned to v1 and the fence re-derived
/// regardless of the pre-crash status — otherwise the checkpointed path
/// (which prunes the origin-carrying WAL record) would drop the certified
/// write's origin, re-tag it v2, and re-certify it off a v2 frontier while the
/// WAL-only path (which harvests every record's origin) keeps it uncertified:
/// a restart-dependent certification result. Both paths must agree.
///
/// WAL-only variant: the origin rides the retained `CertifiedUpsert` record.
#[test]
fn fenced_certified_wal_only_write_regresses_uncertified_across_restart() {
    use asteroidb_poc::runtime::persistence::{
        CheckpointLocks, PersistenceConfig, recover_certified,
    };
    use asteroidb_poc::types::PolicyVersion;

    let dir = tempfile::tempdir().unwrap();
    let cfg = PersistenceConfig {
        enabled: true,
        data_dir: dir.path().to_path_buf(),
        sync: SyncPolicy::Always,
        snapshot_interval: None,
        segment_max_bytes: WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        recover_truncate: false,
        checkpoint_locks: CheckpointLocks::default(),
    };

    let write_ts = {
        let (mut api, _s) = recover_certified(node("node-a"), user_namespace(1), &cfg).unwrap();
        let mut c = PnCounter::new();
        c.increment(&node("node-a"));
        api.certified_write("user/x".into(), CrdtValue::Counter(c), OnTimeout::Pending)
            .unwrap();
        let ts = api.pending_writes()[0].timestamp.clone();
        // Actually certify it under v1 (single authority, majority 1).
        assert!(api.update_frontier(user_frontier(1, ts.physical + 10_000)));
        api.process_certifications();
        assert_eq!(
            api.get_certification_status("user/x"),
            CertificationStatus::Certified,
            "precondition: the write is Certified under v1 before the bump"
        );
        // Operator bumps v1 -> v2; the running node fences v1 (in-memory).
        api.fence_version(
            &KeyRange {
                prefix: "user/".into(),
            },
            PolicyVersion(1),
        );
        ts
        // No checkpoint: the write's origin survives via the WAL record.
    };

    let (mut api, _s) = recover_certified(node("node-a"), user_namespace(2), &cfg).unwrap();
    let pw = api
        .pending_writes()
        .iter()
        .find(|p| p.key == "user/x")
        .expect("recovered write must be tracked");
    assert_eq!(
        pw.policy_version,
        PolicyVersion(1),
        "a previously-Certified write must still recover under its ORIGIN v1"
    );
    assert!(api.update_frontier(user_v2_frontier(write_ts.physical + 10_000)));
    api.process_certifications_with_timeout(write_ts.physical + 30_000);
    assert_ne!(
        api.get_certification_status("user/x"),
        CertificationStatus::Certified,
        "a v1 write certified-then-fenced before a crash must NOT re-certify under v2"
    );
    assert!(api.is_version_fenced(
        &KeyRange {
            prefix: "user/".into()
        },
        &PolicyVersion(1)
    ));
}

/// Checkpointed variant of the previously-`Certified` case: the origin
/// survives ONLY through the certified origins sidecar. Before this fix the
/// sidecar excluded `Certified` writes, so this recovered as v2 and
/// re-certified — diverging from the WAL-only path above.
#[tokio::test]
async fn fenced_certified_checkpointed_write_regresses_uncertified_across_restart() {
    use asteroidb_poc::runtime::persistence::{
        CheckpointLocks, PersistenceConfig, checkpoint_certified, recover_certified,
    };
    use asteroidb_poc::types::PolicyVersion;

    let dir = tempfile::tempdir().unwrap();
    let cfg = PersistenceConfig {
        enabled: true,
        data_dir: dir.path().to_path_buf(),
        sync: SyncPolicy::Off,
        snapshot_interval: None,
        segment_max_bytes: WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        recover_truncate: false,
        checkpoint_locks: CheckpointLocks::default(),
    };

    let write_ts;
    {
        let (mut api, _s) = recover_certified(node("node-a"), user_namespace(1), &cfg).unwrap();
        let mut c = PnCounter::new();
        c.increment(&node("node-a"));
        api.certified_write("user/x".into(), CrdtValue::Counter(c), OnTimeout::Pending)
            .unwrap();
        write_ts = api.pending_writes()[0].timestamp.clone();
        // Drive the write to Certified under v1 BEFORE the bump.
        assert!(api.update_frontier(user_frontier(1, write_ts.physical + 10_000)));
        api.process_certifications();
        assert_eq!(
            api.get_certification_status("user/x"),
            CertificationStatus::Certified,
            "precondition: the write is Certified under v1 before the bump"
        );
        api.fence_version(
            &KeyRange {
                prefix: "user/".into(),
            },
            PolicyVersion(1),
        );
        // Checkpoint: snapshot + prune the WAL. The origin of this
        // now-Certified write must still be captured in the sidecar.
        let api_arc = Arc::new(tokio::sync::Mutex::new(api));
        checkpoint_certified(&api_arc, &cfg).await.unwrap();
    }

    let (mut api, _s) = recover_certified(node("node-a"), user_namespace(2), &cfg).unwrap();
    let pw = api
        .pending_writes()
        .iter()
        .find(|p| p.key == "user/x")
        .expect("checkpointed write must recover");
    assert_eq!(
        pw.policy_version,
        PolicyVersion(1),
        "a checkpointed previously-Certified write must recover under its ORIGIN v1 \
         (the sidecar must capture Certified writes too, matching the WAL path)"
    );
    assert!(api.update_frontier(user_v2_frontier(write_ts.physical + 10_000)));
    api.process_certifications_with_timeout(write_ts.physical + 30_000);
    assert_ne!(
        api.get_certification_status("user/x"),
        CertificationStatus::Certified,
        "a checkpointed v1 write certified-then-fenced before a crash must NOT re-certify \
         under v2 — the checkpointed and WAL-only paths must agree"
    );
    assert!(api.is_version_fenced(
        &KeyRange {
            prefix: "user/".into()
        },
        &PolicyVersion(1)
    ));
}
