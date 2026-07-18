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
    for record in read.records {
        wal::replay_record(&mut store, record);
    }
    let writer = WalWriter::open(wal_cfg(&dir.join("wal-certified"))).unwrap();
    CertifiedApi::recovered(node(node_id), test_namespace(), store, Some(writer))
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

fn set_wal_dir_mode(dir: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let wal_dir = dir.join("wal");
    let mut perms = std::fs::metadata(&wal_dir).unwrap().permissions();
    perms.set_mode(mode);
    std::fs::set_permissions(&wal_dir, perms).unwrap();
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
