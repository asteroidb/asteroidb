//! M-16: HTTP-level WAL durability ack-path integration tests.
//!
//! Every prior test wired `eventual_wal`/`certified_wal` as `None`; this
//! suite runs the real ack gate: `AppState` carries `Some(WalSyncer)`, so
//! write handlers must wait (`wait_wal_durable` on `last_wal_pos`) for the
//! mutation's WAL record to be durable before returning 200 / a session
//! token. Recovery always goes through the production path
//! (`runtime::persistence::recover_eventual` / `recover_certified`) —
//! snapshot load → segment read → torn-tail classification → physical
//! repair → replay → fence decision — never a hand-rolled replica of it.
//!
//! INVARIANT: in `FlusherMode::Held` the test code has a MONOPOLY on every
//! means of advancing the durable watermark (no flusher task is spawned,
//! and only the test calls `wal_rotate`). The "still pending after 300 ms"
//! assertions rest entirely on that monopoly: a correct implementation
//! pends forever, a broken one acks in microseconds, so timing never
//! decides the verdict. If the implementation ever grows a THIRD path that
//! calls `advance_durable` (beyond the flusher's `sync_once` and
//! `rotate_locked`), the pending asserts in this file silently stop
//! meaning anything — any change adding an `advance_durable` call site
//! must revisit this file.
//!
//! LIMIT (fsync dimension): the crash simulation here is an in-process
//! task abort — the kernel page cache survives every simulated crash, so
//! a frame that was write(2)ten but never fdatasync'ed is still readable
//! after "restart". This suite therefore pins the ORDERING between the
//! HTTP ack and the durable watermark, but CANNOT verify that the
//! watermark itself is backed by a physical fsync: a regression that
//! drops the `sync_data` call from `sync_once` (or the `sync_all` from
//! `rotate_locked`) while still calling `advance_durable` passes every
//! test in this suite — and in the repository — GREEN. Keeping those
//! fsync calls in place is a code-review obligation; only OS-level fault
//! injection (outside an in-process test's reach) could pin it.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::network::sync::{SyncRequest, SyncResponse};
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::runtime::persistence::{
    self, CheckpointLocks, PersistenceConfig, spawn_persistence_tasks,
};
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::store::wal::{self, SyncPolicy, WalConfig, WalPos, WalReadOutcome, WalSyncer};
use asteroidb_poc::types::{KeyRange, NodeId, PolicyVersion};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;

// ---------------------------------------------------------------
// Harness
// ---------------------------------------------------------------

/// One shared HTTP client for the whole suite. A fresh
/// `reqwest::Client::new()` per request performs TLS-backend and
/// root-certificate setup that can take hundreds of milliseconds under IO
/// load — cost that would sit inside the spawned request tasks and widen
/// the spawn→append window the tests synchronize on — and each client
/// would hold its own connection pool and fds. Pool keys are per
/// host:port and every test node binds a fresh port, so connections are
/// never reused across nodes.
fn client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// How the WAL flusher tasks are (not) run for a node.
enum FlusherMode {
    /// Production wiring via `spawn_persistence_tasks` — the exact call
    /// `main.rs` makes, so a mutation that wires the syncers to a
    /// different writer is detected here. With `snapshot_interval: None`
    /// it spawns exactly the two flusher tasks and no checkpoint ticker.
    /// `spawn_persistence_tasks` returns no JoinHandles; the flusher
    /// tasks only hold syncer Arcs (no exclusive fd claim), so a crashed
    /// node's flushers linger harmlessly until the test process exits.
    Production,
    /// No flusher spawned: the test owns every durable advancement.
    Held,
}

struct TestNode {
    state: Arc<AppState>,
    addr: SocketAddr,
    server: JoinHandle<()>,
    /// Flusher tasks the TEST spawned (Held-mode late spawns); aborted on
    /// crash. Production-mode flushers are unowned (see `FlusherMode`).
    flushers: Vec<JoinHandle<()>>,
    eventual_syncer: Arc<WalSyncer>,
    certified_syncer: Arc<WalSyncer>,
}

/// Env-independent `PersistenceConfig` (same shape as the unit-test `cfg()`
/// in `runtime/persistence.rs`). `snapshot_interval: None` structurally
/// kills the checkpoint ticker, so no background task can rotate the WAL.
fn persistence_cfg(dir: &Path, sync: SyncPolicy, segment_max_bytes: u64) -> PersistenceConfig {
    PersistenceConfig {
        enabled: true,
        data_dir: dir.to_path_buf(),
        sync,
        snapshot_interval: None,
        segment_max_bytes,
        recover_truncate: false,
        checkpoint_locks: CheckpointLocks::default(),
    }
}

/// Recover both stores through the production path, build an `AppState`
/// with the syncers wired in, and serve it on an OS-assigned port.
async fn spawn_node(
    dir: &Path,
    sync: SyncPolicy,
    segment_max_bytes: u64,
    mode: FlusherMode,
) -> TestNode {
    let cfg = persistence_cfg(dir, sync, segment_max_bytes);
    let node_id = NodeId("test-node".into());

    let (eventual, eventual_syncer) =
        persistence::recover_eventual(node_id.clone(), &cfg).expect("recover_eventual");
    let eventual_syncer = eventual_syncer.expect("persistence is enabled");

    // Same namespace shape as tests/http_server.rs test_state(). The node
    // itself ("test-node") is NOT an authority: certified_write with
    // on_timeout="pending" still acks 200/Pending on a non-authority node.
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: String::new(),
        },
        authority_nodes: vec![
            NodeId("auth-1".into()),
            NodeId("auth-2".into()),
            NodeId("auth-3".into()),
        ],
        auto_generated: false,
    });
    ns.set_placement_policy(PlacementPolicy::new(
        PolicyVersion(1),
        KeyRange {
            prefix: String::new(),
        },
        3,
    ))
    .unwrap();
    let namespace = Arc::new(RwLock::new(ns));

    let (certified, certified_syncer) =
        persistence::recover_certified(node_id, Arc::clone(&namespace), &cfg)
            .expect("recover_certified");
    let certified_syncer = certified_syncer.expect("persistence is enabled");

    let consensus = Arc::new(Mutex::new(ControlPlaneConsensus::new(vec![
        NodeId("auth-1".into()),
        NodeId("auth-2".into()),
        NodeId("auth-3".into()),
    ])));

    let eventual = Arc::new(Mutex::new(eventual));
    let certified = Arc::new(Mutex::new(certified));

    let state = Arc::new(AppState {
        eventual: Arc::clone(&eventual),
        certified: Arc::clone(&certified),
        namespace,
        metrics: Arc::new(RuntimeMetrics::default()),
        peers: None,
        peer_persist_path: None,
        namespace_persist_path: None,
        consensus,
        internal_token: None,
        self_node_id: None,
        self_addr: None,
        latency_model: None,
        cluster_nodes: None,
        slo_tracker: Arc::new(asteroidb_poc::ops::slo::SloTracker::new()),
        keyset_registry: None,
        epoch_config: asteroidb_poc::authority::certificate::EpochConfig::default(),
        current_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        require_signed_frontiers: false,
        equivocation: Arc::new(
            asteroidb_poc::authority::equivocation::EquivocationDetector::new(None),
        ),
        exclude_accused_authorities: false,
        eventual_wal: Some(Arc::clone(&eventual_syncer)),
        certified_wal: Some(Arc::clone(&certified_syncer)),
    });

    match mode {
        FlusherMode::Production => {
            spawn_persistence_tasks(
                cfg.clone(),
                Arc::clone(&eventual),
                Arc::clone(&certified),
                Some(Arc::clone(&eventual_syncer)),
                Some(Arc::clone(&certified_syncer)),
            );
        }
        FlusherMode::Held => {}
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(Arc::clone(&state));
    // No startup sleep: the listener is bound BEFORE serve() is spawned,
    // so early connections queue in the accept backlog.
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    TestNode {
        state,
        addr,
        server,
        flushers: Vec::new(),
        eventual_syncer,
        certified_syncer,
    }
}

/// Tear a node down completely: abort + await the server and any
/// test-spawned flushers, then drop every `Arc<AppState>` reference so the
/// APIs (and their `WalWriter` fds) are released BEFORE the caller opens
/// the same WAL directory again.
async fn crash(node: TestNode) {
    let TestNode {
        state,
        addr: _,
        server,
        flushers,
        eventual_syncer,
        certified_syncer,
    } = node;
    server.abort();
    let _ = server.await; // the server task's AppState clone is now dropped
    for f in flushers {
        f.abort();
        let _ = f.await;
    }
    drop(eventual_syncer);
    drop(certified_syncer);
    drop(state); // last reference → EventualApi/CertifiedApi → WalWriter fd close
}

/// Fire an eventual counter_inc as a background task; resolves to
/// (status, response JSON) when the server acks.
fn spawn_counter_inc(addr: SocketAddr, key: &str) -> JoinHandle<(u16, serde_json::Value)> {
    let key = key.to_string();
    tokio::spawn(async move {
        let resp = client()
            .post(format!("http://{addr}/api/eventual/write"))
            .header("content-type", "application/json")
            .body(format!(r#"{{"type":"counter_inc","key":"{key}"}}"#))
            .send()
            .await
            .expect("eventual write request must complete");
        let status = resp.status().as_u16();
        let body: serde_json::Value = resp.json().await.expect("JSON body");
        (status, body)
    })
}

/// Wait (10s cap) until the EVENTUAL store's `last_wal_pos` reaches `n`.
///
/// The spawned request tasks expose no completion signal short of the ack
/// itself, so before asserting "pending" or pulling a release lever
/// (`wal_rotate` / a dependent follow-up request) a test MUST confirm the
/// request has reached the handler and its WAL append has landed.
/// Otherwise, under load, the release rotation can fire BEFORE the append
/// (advancing durable by nothing), the append then lands, and — the Held
/// monopoly leaving no further durable advancement — the waiter pends
/// until the 5s ack timeout turns the test RED. This wait also makes the
/// subsequent `assert_pending` meaningful ("appended but not acked", not
/// "request not yet arrived"), closing the vacuous-pass path for
/// gate-removal mutations.
async fn wait_appended_eventual(state: &Arc<AppState>, n: u64) {
    timeout(Duration::from_secs(10), async {
        loop {
            if state.eventual.lock().await.last_wal_pos() >= Some(WalPos(n)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("eventual WAL append did not reach pos {n} within 10s"));
}

/// Certified-store twin of [`wait_appended_eventual`].
async fn wait_appended_certified(state: &Arc<AppState>, n: u64) {
    timeout(Duration::from_secs(10), async {
        loop {
            if state.certified.lock().await.last_wal_pos() >= Some(WalPos(n)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("certified WAL append did not reach pos {n} within 10s"));
}

/// Assert that an in-flight ack task is still pending after 300 ms.
///
/// Callers must FIRST confirm the write's WAL append has landed
/// (`wait_appended_eventual` / `wait_appended_certified`) so this checks
/// "appended but not acked" rather than "request not yet arrived".
///
/// False-pass-safe by construction (see the file-top INVARIANT): with the
/// durable-advancement monopoly held by the test, a correct implementation
/// pends forever, while a broken one (ack without durability) completes in
/// microseconds — the timeout duration never decides the verdict.
async fn assert_pending<T>(task: &mut JoinHandle<T>, what: &str) {
    if timeout(Duration::from_millis(300), &mut *task)
        .await
        .is_ok()
    {
        panic!("{what}: HTTP ack returned although nothing advanced the durable watermark");
    }
}

/// Await an ack task with a generous safety timeout.
async fn await_ack<T>(task: JoinHandle<T>, what: &str) -> T {
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap_or_else(|_| panic!("{what}: ack did not arrive within 5s"))
        .expect("ack task must not panic")
}

/// GET an eventual key, asserting HTTP 200; returns the response JSON.
async fn get_eventual(addr: SocketAddr, key: &str) -> serde_json::Value {
    let resp = client()
        .get(format!("http://{addr}/api/eventual/{key}"))
        .send()
        .await
        .expect("eventual read request");
    assert_eq!(resp.status(), 200, "eventual read of {key}");
    resp.json().await.expect("JSON body")
}

fn assert_counter_value(body: &serde_json::Value, key: &str, expected: u64) {
    assert_eq!(body["key"], key);
    assert_eq!(body["value"]["type"], "counter", "key {key}");
    assert_eq!(body["value"]["value"], expected, "key {key}");
}

/// Restores the directory mode to 0o755 on drop (panic-safe cleanup for
/// the chmod-based fault injection in the append-failure test).
struct DirModeGuard {
    path: PathBuf,
}

impl DirModeGuard {
    fn set(path: &Path, mode: u32) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(path).unwrap().permissions();
        perms.set_mode(mode);
        std::fs::set_permissions(path, perms).unwrap();
        Self {
            path: path.to_path_buf(),
        }
    }
}

impl Drop for DirModeGuard {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(&self.path) {
            let mut perms = meta.permissions();
            perms.set_mode(0o755);
            let _ = std::fs::set_permissions(&self.path, perms);
        }
    }
}

// ---------------------------------------------------------------
// T1. Full production wiring: acked writes survive a crash
// ---------------------------------------------------------------

/// A `POST /api/eventual/write` that returned 200 + a session token must
/// still be readable after crash + restart, with the WAL frame physically
/// on disk at ack time. Runs the COMPLETE production wiring:
/// `recover_*` + `spawn_persistence_tasks` (real flushers).
#[tokio::test]
async fn http_acked_eventual_write_survives_crash() {
    let dir = tempfile::tempdir().unwrap();
    let node = spawn_node(
        dir.path(),
        SyncPolicy::Always,
        WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        FlusherMode::Production,
    )
    .await;

    for key in ["c0", "c1", "c2"] {
        let resp = client()
            .post(format!("http://{}/api/eventual/write", node.addr))
            .header("content-type", "application/json")
            .body(format!(r#"{{"type":"counter_inc","key":"{key}"}}"#))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["ok"], true);
        assert!(
            !body["session_token"].as_str().unwrap_or("").is_empty(),
            "an acked write must hand out a session token (durability receipt)"
        );
    }

    // ack ⇒ the frames are physically in the WAL, not merely reflected in
    // an in-memory watermark.
    let read = wal::read_all_segments(&dir.path().join("wal/eventual")).unwrap();
    assert_eq!(read.outcome, WalReadOutcome::Clean);
    assert_eq!(
        read.records.len(),
        3,
        "one UpsertApplied frame per acked write must be on disk at ack time"
    );

    crash(node).await;
    let node = spawn_node(
        dir.path(),
        SyncPolicy::Always,
        WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        FlusherMode::Production,
    )
    .await;

    for key in ["c0", "c1", "c2"] {
        let body = get_eventual(node.addr, key).await;
        assert_counter_value(&body, key, 1);
    }
    crash(node).await;
}

// ---------------------------------------------------------------
// T2. The ack gate itself (main detector, production release path)
// ---------------------------------------------------------------

/// Under `SyncPolicy::Always` the HTTP ack must not return before the WAL
/// record is durable, and must return exactly when the flusher's flush
/// completes (`sync_once` → `advance_durable` → watch notification).
#[tokio::test]
async fn http_ack_blocks_until_flush_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = spawn_node(
        dir.path(),
        SyncPolicy::Always,
        WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        FlusherMode::Held,
    )
    .await;

    let mut task = spawn_counter_inc(node.addr, "k");
    wait_appended_eventual(&node.state, 1).await;
    assert_pending(&mut task, "eventual write under Always with no flusher").await;

    // The append itself completed inside the handler; only durability is
    // outstanding.
    assert_eq!(
        node.state.eventual.lock().await.last_wal_pos(),
        Some(WalPos(1)),
        "the WAL append must have happened before the ack wait"
    );
    assert_eq!(
        node.eventual_syncer.durable_watermark(),
        0,
        "nothing may be durable before any flush"
    );

    // Late flusher spawn is sound: `WalWriter::append` already called
    // `wake.notify_one()`, and tokio's `Notify` stores one permit when no
    // waiter is registered — so the flusher's first `notified().await`
    // completes immediately, and `sync_once` flushes up to the FULL
    // appended watermark (one permit suffices for any number of appends).
    // If `run_flusher`'s notified→sync_once structure ever changes, this
    // test fails at the 5s timeout below instead of hanging forever.
    let flusher = tokio::spawn(Arc::clone(&node.eventual_syncer).run_flusher());
    node.flushers.push(flusher);

    let (status, body) = await_ack(task, "eventual write after the flusher ran").await;
    assert_eq!(status, 200);
    assert!(
        !body["session_token"].as_str().unwrap_or("").is_empty(),
        "the durability receipt must accompany the ack"
    );
    assert!(
        node.eventual_syncer.durable_watermark() >= 1,
        "the ack implies the durable watermark covers the record"
    );
    crash(node).await;
}

// ---------------------------------------------------------------
// T3. Certified ack gate + recovery (status regresses to Pending)
// ---------------------------------------------------------------

/// The certified write ack is durable-gated too (held → pend, released by
/// rotation), and the acked value survives a crash via `recover_certified`
/// with its certification status regressed to Pending (fail-closed).
#[tokio::test]
async fn http_certified_write_ack_gated_and_recovered() {
    let dir = tempfile::tempdir().unwrap();
    let node = spawn_node(
        dir.path(),
        SyncPolicy::Always,
        WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        FlusherMode::Held,
    )
    .await;

    let addr = node.addr;
    let mut task = tokio::spawn(async move {
        let resp = client()
            .post(format!("http://{addr}/api/certified/write"))
            .header("content-type", "application/json")
            .body(r#"{"key":"sensor","value":{"type":"counter","value":5},"on_timeout":"pending"}"#)
            .send()
            .await
            .expect("certified write request");
        let status = resp.status().as_u16();
        let body: serde_json::Value = resp.json().await.expect("JSON body");
        (status, body)
    });
    wait_appended_certified(&node.state, 1).await;
    assert_pending(&mut task, "certified write under Always with no flusher").await;
    assert_eq!(
        node.certified_syncer.durable_watermark(),
        0,
        "nothing may be durable before the release"
    );

    // Release via rotation: seals the segment (`sync_all`) and advances
    // the durable watermark through `rotate_locked` — a second
    // `advance_durable` call site, distinct from the flusher's
    // `sync_once` path exercised in the eventual-store tests.
    node.state
        .certified
        .lock()
        .await
        .wal_rotate()
        .expect("wal_rotate");

    let (status, body) = await_ack(task, "certified write after rotation").await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "Pending");

    // ack ⇒ the frame is physically in the CERTIFIED WAL directory.
    let read = wal::read_all_segments(&dir.path().join("wal/certified")).unwrap();
    assert_eq!(
        read.records.len(),
        1,
        "the certified write's frame must be on disk at ack time"
    );

    crash(node).await;
    let node = spawn_node(
        dir.path(),
        SyncPolicy::Always,
        WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        FlusherMode::Held,
    )
    .await;

    let resp = client()
        .get(format!("http://{}/api/certified/sensor", node.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["value"]["type"], "counter");
    assert_eq!(body["value"]["value"], 5);
    assert_eq!(
        body["status"], "Pending",
        "certification state is volatile: recovery must regress to Pending, never fake Certified"
    );
    crash(node).await;
}

// ---------------------------------------------------------------
// T4. Boundary asymmetry: pre-ack crash loses ONLY the un-acked write
// ---------------------------------------------------------------

/// A write whose ack never returned (torn frame at the WAL tail) may be
/// lost; recovery must succeed, the acked earlier write must be intact,
/// and the server must keep serving.
#[tokio::test]
async fn http_pre_ack_crash_loses_only_unacked_writes() {
    let dir = tempfile::tempdir().unwrap();
    let node = spawn_node(
        dir.path(),
        SyncPolicy::Always,
        WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        FlusherMode::Held,
    )
    .await;

    // A: acked — its frame is sealed into segment 1 by the release
    // rotation, hence covered by the durable watermark (see the file-top
    // LIMIT note: the physical fsync behind that watermark is not
    // verifiable in-process).
    let mut task_a = spawn_counter_inc(node.addr, "a");
    wait_appended_eventual(&node.state, 1).await;
    assert_pending(&mut task_a, "write A before any durable advancement").await;
    node.state
        .eventual
        .lock()
        .await
        .wal_rotate()
        .expect("wal_rotate");
    let (status, _) = await_ack(task_a, "write A after rotation").await;
    assert_eq!(status, 200);

    // B: appended into segment 2 but NEVER acked.
    let mut task_b = spawn_counter_inc(node.addr, "b");
    wait_appended_eventual(&node.state, 2).await;
    assert_pending(&mut task_b, "write B must stay un-acked").await;
    assert_eq!(
        node.state.eventual.lock().await.last_wal_pos(),
        Some(WalPos(2)),
        "B's frame must be appended before the surgery"
    );

    // Drop the in-flight request future BEFORE tearing the node down.
    task_b.abort();
    let _ = task_b.await;
    crash(node).await;

    // Synthesize the on-disk image of a power cut mid-`write_all`:
    // chop the tail of the final segment so B's frame is torn.
    let (_seq, seg) = wal::list_segments(&dir.path().join("wal/eventual"))
        .unwrap()
        .pop()
        .unwrap();
    let data = std::fs::read(&seg).unwrap();
    std::fs::write(&seg, &data[..data.len() - 4]).unwrap();

    // Production recovery: a torn tail is expected crash damage — warn,
    // replay the prefix, physically repair (`truncate_to_valid_prefix`).
    // Under Always no recovery fence is installed.
    let node = spawn_node(
        dir.path(),
        SyncPolicy::Always,
        WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        FlusherMode::Held,
    )
    .await;

    let body_a = get_eventual(node.addr, "a").await;
    assert_counter_value(&body_a, "a", 1);
    let body_b = get_eventual(node.addr, "b").await;
    assert!(
        body_b["value"].is_null(),
        "the un-acked, torn write B must NOT be restored (over-restoring would \
         treat an un-acked write as acked)"
    );

    // The server stays healthy: a new write still acks through the gate.
    // (The append counter restarts at 0 for the recovered writer, so C is
    // pos 1 in this process.)
    let mut task_c = spawn_counter_inc(node.addr, "c");
    wait_appended_eventual(&node.state, 1).await;
    assert_pending(&mut task_c, "write C gates on durability after recovery").await;
    node.state
        .eventual
        .lock()
        .await
        .wal_rotate()
        .expect("wal_rotate");
    let (status, _) = await_ack(task_c, "write C after rotation").await;
    assert_eq!(status, 200);
    crash(node).await;
}

// ---------------------------------------------------------------
// T5. WAL append failure ⇒ no ack, no token, no resurrection
// ---------------------------------------------------------------

/// A write whose WAL append fails must answer non-2xx WITHOUT a session
/// token, must not reappear after recovery, and the node must self-heal
/// for subsequent writes (poison record flushed before the next append).
///
/// Fault injection: `segment_max_bytes: 1` forces every append into a
/// non-empty segment to rotate first; removing write permission on the WAL
/// directory makes that rotation's segment creation fail with EACCES while
/// the already-open fd keeps working (same technique as
/// tests/wal_recovery.rs). fsync failure is NOT injected — `sync_once`
/// aborts the whole process on fsync errors by design (fsyncgate).
///
/// NOTE deviation from the design document: the design released ok-1 via
/// rotation BEFORE the chmod, but that leaves the fresh active segment
/// empty — and a first append into an empty segment never rotates
/// (`seg_records > 0` guards the rotation), so the fault would not fire.
/// ok-1 is therefore left pending across the failure phase; it is released
/// by the segment seal that the poison-flush rotation performs during
/// ok-2's append (which also exercises `rotate_locked`'s
/// `advance_durable`).
#[tokio::test]
async fn http_wal_append_failure_rejects_ack() {
    let dir = tempfile::tempdir().unwrap();
    let node = spawn_node(dir.path(), SyncPolicy::Always, 1, FlusherMode::Held).await;

    // ok-1: first append into segment 1 (no rotation), pending. The
    // append MUST have landed before the chmod below, or the fault would
    // hit ok-1 itself.
    let mut task_ok1 = spawn_counter_inc(node.addr, "ok-1");
    wait_appended_eventual(&node.state, 1).await;
    assert_pending(&mut task_ok1, "ok-1 pends until a durable boundary").await;

    let wal_dir = dir.path().join("wal/eventual");
    let guard = DirModeGuard::set(&wal_dir, 0o555);
    // Privileged processes (CAP_DAC_OVERRIDE) ignore directory modes: the
    // fault cannot fire, so skip rather than mis-assert.
    let probe = wal_dir.join("probe.tmp");
    if std::fs::File::create(&probe).is_ok() {
        let _ = std::fs::remove_file(&probe);
        drop(guard);
        eprintln!("skipping http_wal_append_failure_rejects_ack: running privileged");
        task_ok1.abort();
        let _ = task_ok1.await;
        crash(node).await;
        return;
    }

    // fail-key: the append's forced rotation cannot create segment 2 →
    // append Err → `finish_local_write` poisons the key and propagates →
    // the handler must answer non-2xx with NO session token. (The error
    // path returns before the durability wait, so no task spawn needed.)
    let resp = client()
        .post(format!("http://{}/api/eventual/write", node.addr))
        .header("content-type", "application/json")
        .body(r#"{"type":"counter_inc","key":"fail-key"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        503,
        "a write whose WAL append failed must not be acked"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error_code"], "STORAGE_UNAVAILABLE");
    assert!(
        body.get("session_token").is_none(),
        "a failed write must not hand out a durability receipt"
    );

    drop(guard); // restore 0o755: the disk "heals"

    // ok-2: its append first flushes the queued MergeFailed poison record
    // (`pending_poison` must precede any frontier-advancing record), then
    // the data record; both rotations succeed now. The first of those
    // rotations seals segment 1 and releases ok-1's waiter.
    // Wait for pos 3: poison record = pos 2, ok-2's data record = pos 3.
    let mut task_ok2 = spawn_counter_inc(node.addr, "ok-2");
    wait_appended_eventual(&node.state, 3).await;
    assert_pending(&mut task_ok2, "ok-2 pends until a durable boundary").await;
    let (status_ok1, _) = await_ack(task_ok1, "ok-1 released by the poison-flush rotation").await;
    assert_eq!(status_ok1, 200);

    node.state
        .eventual
        .lock()
        .await
        .wal_rotate()
        .expect("wal_rotate");
    let (status_ok2, _) = await_ack(task_ok2, "ok-2 after rotation").await;
    assert_eq!(status_ok2, 200);

    crash(node).await;
    let node = spawn_node(dir.path(), SyncPolicy::Always, 1, FlusherMode::Held).await;

    let body = get_eventual(node.addr, "ok-1").await;
    assert_counter_value(&body, "ok-1", 1);
    let body = get_eventual(node.addr, "ok-2").await;
    assert_counter_value(&body, "ok-2", 1);
    let body = get_eventual(node.addr, "fail-key").await;
    assert!(
        body["value"].is_null(),
        "a value that reached memory but never the WAL must not be resurrected \
         (the un-acked 503 write must stay lost)"
    );
    crash(node).await;
}

// ---------------------------------------------------------------
// T6. Group commit: one durable advancement releases all waiters
// ---------------------------------------------------------------

/// Multiple pending writes are all acked by the single durable advancement
/// that covers them: `wait_durable`'s `durable >= pos` comparison and
/// `advance_durable`'s `fetch_max` monotonicity define the ack granularity.
/// Detects a `>=`→`==` regression that every sequential test misses
/// (there, durable always lands exactly on the waited-for position; here
/// it jumps 0→2 past the pos-1 waiter).
#[tokio::test]
async fn http_group_commit_single_flush_acks_all_pending() {
    let dir = tempfile::tempdir().unwrap();
    let node = spawn_node(
        dir.path(),
        SyncPolicy::Always,
        WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        FlusherMode::Held,
    )
    .await;

    let mut task_a = spawn_counter_inc(node.addr, "g-a");
    let mut task_b = spawn_counter_inc(node.addr, "g-b");
    wait_appended_eventual(&node.state, 2).await;
    assert_pending(&mut task_a, "group write A").await;
    assert_pending(&mut task_b, "group write B").await;
    assert_eq!(
        node.state.eventual.lock().await.last_wal_pos(),
        Some(WalPos(2)),
        "both appends must be in the WAL before the single release"
    );
    assert_eq!(node.eventual_syncer.durable_watermark(), 0);

    // ONE durable advancement (0 → 2) covering both records.
    node.state
        .eventual
        .lock()
        .await
        .wal_rotate()
        .expect("wal_rotate");

    let (status_a, _) = await_ack(
        task_a,
        "waiter at pos 1 must be released by the durable jump 0→2",
    )
    .await;
    let (status_b, _) = await_ack(task_b, "waiter at pos 2").await;
    assert_eq!(status_a, 200);
    assert_eq!(status_b, 200);
    assert!(node.eventual_syncer.durable_watermark() >= 2);
    crash(node).await;
}

// ---------------------------------------------------------------
// T7. Interval policy: ack does NOT wait for a flush (pinned semantics)
// ---------------------------------------------------------------

/// Under `SyncPolicy::Interval` the ack returns immediately without any
/// flush — the `policy() == Always` branch of `wait_wal_durable` is the
/// specification being pinned here (durability under non-Always policies
/// is compensated by the recovery fence, not by the ack).
#[tokio::test]
async fn http_interval_policy_acks_without_wait() {
    let dir = tempfile::tempdir().unwrap();
    // Held + a 1-hour interval: no flusher task exists, so no tick can
    // ever fire — a "wait for flush" regression pends forever (RED via
    // the 5s timeout), while the correct immediate ack is deterministic.
    //
    // NOTE: under a non-Always policy `recover_eventual` installs a
    // recovery fence and force-writes a fence snapshot at startup. The
    // fence only adds a recovery gap and bumps the HLC — it does not
    // interfere with the write handler's 200/token issuance (fences gate
    // session-token READ evidence only), so the production recovery path
    // is used unchanged here.
    let node = spawn_node(
        dir.path(),
        SyncPolicy::Interval(Duration::from_secs(3600)),
        WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        FlusherMode::Held,
    )
    .await;

    let task = spawn_counter_inc(node.addr, "k");
    let (status, body) = await_ack(task, "Interval-policy write must ack without a flush").await;
    assert_eq!(status, 200);
    assert!(!body["session_token"].as_str().unwrap_or("").is_empty());

    // Direct proof that the ack preceded ANY durability: nothing flushed,
    // nothing rotated, yet the write was acknowledged.
    assert_eq!(
        node.eventual_syncer.durable_watermark(),
        0,
        "Interval semantics: the ack must not have waited for durability"
    );
    crash(node).await;
}

// ---------------------------------------------------------------
// T8. Internal sync ack is durable-gated too
// ---------------------------------------------------------------

/// `POST /api/internal/sync`'s ack is what lets the pushing peer advance
/// its push frontier, so under Always it must wait for the merged batch's
/// WAL records to be durable.
///
/// NOT tested: the wait-FAILURE branch (warn + ack anyway) — it is only
/// reachable when the flusher task is gone, which the fail-stop policy on
/// fsync errors makes deterministically unreachable from tests; the
/// branch is intentional (documented in the handler).
#[tokio::test]
async fn http_internal_sync_waits_for_durability() {
    let dir = tempfile::tempdir().unwrap();
    let node = spawn_node(
        dir.path(),
        SyncPolicy::Always,
        WalConfig::DEFAULT_SEGMENT_MAX_BYTES,
        FlusherMode::Held,
    )
    .await;

    let addr = node.addr;
    let mut task = tokio::spawn(async move {
        let mut counter = PnCounter::new();
        counter.increment(&NodeId("sender".into()));
        let mut entries = HashMap::new();
        entries.insert("sync-key".to_string(), CrdtValue::Counter(counter));
        let req = SyncRequest {
            sender: "sender".to_string(),
            entries,
        };
        let resp = client()
            .post(format!("http://{addr}/api/internal/sync"))
            .json(&req)
            .send()
            .await
            .expect("internal sync request");
        let status = resp.status().as_u16();
        let body: SyncResponse = resp.json().await.expect("sync response");
        (status, body)
    });
    // The merged entry lands in the EVENTUAL store's WAL as pos 1.
    wait_appended_eventual(&node.state, 1).await;
    assert_pending(
        &mut task,
        "internal sync ack (push-frontier basis) under Always with no flusher",
    )
    .await;

    node.state
        .eventual
        .lock()
        .await
        .wal_rotate()
        .expect("wal_rotate");

    let (status, body) = await_ack(task, "internal sync after rotation").await;
    assert_eq!(status, 200);
    assert_eq!(body.merged, 1, "the pushed entry must have merged");
    assert!(body.errors.is_empty());
    crash(node).await;
}
