//! Integration tests for client session guarantees (read-your-writes /
//! monotonic reads) over the eventual store.
//!
//! The invariant under test: a session-token-guarded read that answers
//! 200 (or a `session_check` that answers `true`) is NEVER a lie — the
//! replica provably contains the writes the token covers. False
//! negatives (unnecessary refusals) are acceptable; false successes are
//! not.

use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::session::SessionToken;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::NodeId;

fn node_id(s: &str) -> NodeId {
    NodeId(s.into())
}

fn zero_frontier() -> HlcTimestamp {
    HlcTimestamp {
        physical: 0,
        logical: 0,
        node_id: String::new(),
    }
}

/// Run one CLAIMED delta-sync cycle from `source` to `target`:
/// `delta_entries_since(frontier)` on the source, `merge_remote_with_hlc`
/// per entry on the target, then adoption of the source's applied_origins
/// and poisoned keys — exactly what `NodeRunner::apply_delta_response`
/// does when the completeness conditions (`claims_ok`) hold.
fn pull_delta(source: &EventualApi, target: &mut EventualApi, frontier: &HlcTimestamp) {
    pull_delta_unclaimed(source, target, frontier);
    target
        .store_mut()
        .merge_failed_extend(source.store().merge_failed_keys().iter().cloned());
    target
        .store_mut()
        .merge_applied_origins(source.store().applied_origins());
}

/// Run one UNCLAIMED delta-sync cycle: entries are merged (data
/// convergence) and the source's VISIBLE frontier is merged (response
/// tokens must cover embedded contributions), but no applied-origins
/// adoption happens — what the node runner does when the delta may be
/// incomplete (`claims_ok` false). This is a normal, recurring state in
/// production (e.g. right after the sender pruned), so session
/// invariants must hold across it too.
fn pull_delta_unclaimed(source: &EventualApi, target: &mut EventualApi, frontier: &HlcTimestamp) {
    for (key, value, hlc) in source.store().delta_entries_since(frontier) {
        target.merge_remote_with_hlc(key, &value, hlc).unwrap();
    }
    target
        .store_mut()
        .merge_visible_origins(source.store().visible_origins());
}

/// Build a response session token the way `get_eventual` does: request
/// token (empty here) + the key's own change position + the replica's
/// VISIBLE origins.
fn response_token(api: &EventualApi, key: &str) -> SessionToken {
    let mut token = SessionToken::default();
    if let Some(ts) = api.store().timestamp_for(key) {
        token.merge_hlc(ts);
    }
    token.merge_frontiers(api.store().visible_origins());
    token
}

// ===================================================================
// Read-your-writes across two nodes, all four CRDT types
// ===================================================================

#[test]
fn two_node_ryw_all_crdt_types() {
    let mut a = EventualApi::new(node_id("node-a"));
    let mut b = EventualApi::new(node_id("node-b"));

    // Four writes on A, one per CRDT type, each returning a token HLC.
    let tokens: Vec<(String, SessionToken)> = vec![
        (
            "cnt".into(),
            SessionToken::from_hlc(&a.eventual_counter_inc("cnt").unwrap()),
        ),
        (
            "set".into(),
            SessionToken::from_hlc(&a.eventual_set_add("set", "alice".into()).unwrap()),
        ),
        (
            "map".into(),
            SessionToken::from_hlc(&a.eventual_map_set("map", "k".into(), "v".into()).unwrap()),
        ),
        (
            "reg".into(),
            SessionToken::from_hlc(&a.eventual_register_set("reg", "hello".into()).unwrap()),
        ),
    ];

    // Before sync, B must refuse every token (would be a lie otherwise).
    for (key, token) in &tokens {
        assert!(
            !b.session_check(key, token),
            "unsynced replica must not satisfy token for {key}"
        );
    }

    // One real delta-sync cycle A → B.
    pull_delta(&a, &mut b, &zero_frontier());

    // Now every token is satisfied AND the value actually reflects the write.
    for (key, token) in &tokens {
        assert!(
            b.session_check(key, token),
            "synced replica must satisfy token for {key}"
        );
    }
    match b.get_eventual("cnt") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
        other => panic!("expected Counter, got {other:?}"),
    }
    match b.get_eventual("set") {
        Some(CrdtValue::Set(s)) => assert!(s.contains(&"alice".to_string())),
        other => panic!("expected Set, got {other:?}"),
    }
    match b.get_eventual("map") {
        Some(CrdtValue::Map(m)) => assert_eq!(m.get(&"k".to_string()), Some(&"v".to_string())),
        other => panic!("expected Map, got {other:?}"),
    }
    match b.get_eventual("reg") {
        Some(CrdtValue::Register(r)) => assert_eq!(r.get(), Some(&"hello".to_string())),
        other => panic!("expected Register, got {other:?}"),
    }
}

// ===================================================================
// Monotonic reads across three nodes
// ===================================================================

/// A read token minted on a synced replica must be refused by a replica
/// that has not caught up — the classic "reads go back in time" case.
#[test]
fn three_node_monotonic_reads() {
    let mut a = EventualApi::new(node_id("node-a"));
    let mut b = EventualApi::new(node_id("node-b"));
    let mut c = EventualApi::new(node_id("node-c"));

    a.eventual_counter_inc("k").unwrap();
    a.eventual_counter_inc("k").unwrap();

    // B syncs from A; C does not.
    pull_delta(&a, &mut b, &zero_frontier());

    // A read on B produces an observed-position token — the same
    // computation get_eventual performs.
    let read_token = response_token(&b, "k");
    match b.get_eventual("k") {
        Some(CrdtValue::Counter(cv)) => assert_eq!(cv.value(), 2),
        other => panic!("expected Counter, got {other:?}"),
    }

    // C would serve a rewound (missing) value: it must refuse the token.
    assert!(
        !c.session_check("k", &read_token),
        "stale replica must refuse the read token (monotonic reads)"
    );

    // After C syncs from A, the token is satisfied and the value matches.
    pull_delta(&a, &mut c, &zero_frontier());
    assert!(c.session_check("k", &read_token));
    match c.get_eventual("k") {
        Some(CrdtValue::Counter(cv)) => assert_eq!(cv.value(), 2),
        other => panic!("expected Counter, got {other:?}"),
    }
}

// ===================================================================
// Frontier adoption: why the read token must include the sender's
// applied_origins (regression for the multi-origin CRDT hole)
// ===================================================================

/// Scenario: A and B both increment key k; S merges both. R pulls from S
/// — the pulled counter VALUE embeds A's contribution, but the delta
/// entry HLC only names one origin. Without adoption, R's read token
/// would omit A, and a replica missing A's write would satisfy the token
/// while serving a lower counter (monotonic-reads lie). With adoption, R's
/// token covers A and the stale replica refuses.
#[test]
fn adoption_closes_multi_origin_token_hole() {
    let mut a = EventualApi::new(node_id("node-a"));
    let mut b = EventualApi::new(node_id("node-b"));
    let mut s = EventualApi::new(node_id("node-s"));
    let mut r = EventualApi::new(node_id("node-r"));

    let a_hlc = a.eventual_counter_inc("k").unwrap();
    b.eventual_counter_inc("k").unwrap();

    // S pulls from both A and B (complete deltas).
    pull_delta(&a, &mut s, &zero_frontier());
    pull_delta(&b, &mut s, &zero_frontier());
    match s.get_eventual("k") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
        other => panic!("expected Counter, got {other:?}"),
    }

    // R pulls a CLAIMED delta from S (entries + adoption of S's
    // applied_origins — what apply_delta_response does; S never pruned,
    // so adoption is unconditionally sound).
    pull_delta(&s, &mut r, &zero_frontier());

    // R's read now shows both contributions.
    match r.get_eventual("k") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
        other => panic!("expected Counter, got {other:?}"),
    }

    // R's response token must cover A's write...
    let read_token = response_token(&r, "k");
    assert!(
        read_token
            .entries()
            .iter()
            .any(|e| e.node_id == "node-a" && *e >= a_hlc),
        "adopted token must cover origin node-a: {read_token:?}"
    );

    // ...so a replica that only has B's write refuses it (no lie).
    let mut r2 = EventualApi::new(node_id("node-r2"));
    pull_delta(&b, &mut r2, &zero_frontier());
    assert!(
        !r2.session_check("k", &read_token),
        "replica missing node-a's write must refuse the adopted token"
    );

    // Control experiment: a token built from applied_origins alone after
    // an UNCLAIMED pull (no adoption) omits both contributing origins and
    // is satisfied on r2 even though r2's counter value (1) is lower than
    // what the reader observed (2). This is the monotonic-reads hole that
    // response tokens close by covering the VISIBLE frontier instead.
    let mut naive_r = EventualApi::new(node_id("node-naive"));
    pull_delta_unclaimed(&s, &mut naive_r, &zero_frontier());
    match naive_r.get_eventual("k") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
        other => panic!("expected Counter, got {other:?}"),
    }
    let mut naive_token = SessionToken::default();
    naive_token.merge_frontiers(naive_r.store().applied_origins());
    assert!(
        r2.session_check("k", &naive_token),
        "control: an applied-origins-only token after an unclaimed pull \
         wrongly passes on the stale replica — the hole visible-frontier \
         response tokens exist to close"
    );

    // The FIXED response token (visible frontier) is refused on r2.
    let fixed_token = response_token(&naive_r, "k");
    assert!(
        !fixed_token.entries().is_empty(),
        "visible frontier must produce a non-empty token"
    );
    assert!(
        !r2.session_check("k", &fixed_token),
        "the visible-frontier response token must be refused by the stale \
         replica (monotonic reads, no lie)"
    );
}

// ===================================================================
// Unclaimed merges (the production steady state after pushes):
// response tokens must cover what the reader can observe
// ===================================================================

/// Regression for the monotonic-reads false success through unclaimed
/// merges: a value made visible by a possibly-incomplete delta is
/// readable, so the response token must cover its origin even though no
/// applied claim was made — otherwise a stale replica satisfies the
/// token and serves an older value.
#[test]
fn unclaimed_merge_response_token_is_refused_by_stale_replica() {
    let mut x = EventualApi::new(node_id("node-x"));
    let mut y = EventualApi::new(node_id("node-y"));
    let mut c = EventualApi::new(node_id("node-c"));

    // X writes locally; Y fully syncs from X (claimed) so Y satisfies
    // any X-origin token.
    x.eventual_counter_inc("k").unwrap();
    pull_delta(&x, &mut y, &zero_frontier());

    // C writes to the same key; X receives it through an UNCLAIMED pull
    // (claims suppressed — e.g. the peer frontier was push-advanced).
    c.eventual_counter_inc("k").unwrap();
    pull_delta_unclaimed(&c, &mut x, &zero_frontier());

    // A session read on X observes both contributions (value 2).
    match x.get_eventual("k") {
        Some(CrdtValue::Counter(cv)) => assert_eq!(cv.value(), 2),
        other => panic!("expected Counter, got {other:?}"),
    }
    assert!(
        x.store().applied_origin("node-c").is_none(),
        "unclaimed merge must not claim origin node-c"
    );

    // The response token covers node-c via the visible frontier...
    let token = response_token(&x, "k");
    assert!(
        token.entries().iter().any(|e| e.node_id == "node-c"),
        "response token must cover the unclaimed contribution: {token:?}"
    );

    // ...so Y — which never saw C's write — must refuse it (412), not
    // serve its lower counter value with 200.
    assert!(
        !y.session_check("k", &token),
        "stale replica must refuse the token covering an unclaimed \
         observation (monotonic reads must not silently break)"
    );

    // Control: a token from applied_origins only (the pre-fix response
    // token) would have passed on Y while Y serves 1 < 2 — the lie.
    let mut applied_only = SessionToken::default();
    applied_only.merge_frontiers(x.store().applied_origins());
    assert!(
        y.session_check("k", &applied_only),
        "control: applied-origins-only token passes on the stale replica"
    );
}

/// Regression for the per-entry claim unsoundness (third-party origin
/// gap): B holds a LATER write of origin C but not an earlier one (gappy
/// unclaimed delta). A fresh node A doing a CLAIMED pull from B must not
/// end up claiming origin C — B itself never proved C's prefix, so A
/// must keep refusing C-origin tokens for the missing key (no false
/// success).
#[test]
fn third_party_origin_gap_is_never_claimed_transitively() {
    let mut c = EventualApi::new(node_id("node-c"));
    let mut b = EventualApi::new(node_id("node-b"));
    let mut a = EventualApi::new(node_id("node-a"));

    // C writes k1 then k2.
    let k1_hlc = c.eventual_counter_inc("k1").unwrap();
    let k2_hlc = c.eventual_counter_inc("k2").unwrap();
    assert!(k1_hlc < k2_hlc);

    // B pulls from C with a frontier that skips k1 (the push-advanced
    // frontier hazard): only k2 arrives, unclaimed.
    pull_delta_unclaimed(&c, &mut b, &k1_hlc);
    assert!(b.get_eventual("k2").is_some());
    assert!(b.get_eventual("k1").is_none(), "gap: k1 never reached B");
    assert!(b.store().applied_origin("node-c").is_none());

    // A fresh node A pulls a CLAIMED delta from B (complete relative to
    // B — but B's state has a gap in C's prefix).
    pull_delta(&b, &mut a, &zero_frontier());

    // A must NOT have claimed origin node-c: the entry HLC named C, but
    // neither A nor B holds C's full prefix up to k2.
    assert!(
        a.store().applied_origin("node-c").is_none(),
        "claimed pull must not claim a third-party origin the sender \
         never claimed"
    );

    // The client that wrote k1 on C reads k1 on A with its write token:
    // A must refuse (false negative), never serve the missing key with
    // 200 (the documented \"never a lie\" invariant).
    let k1_token = SessionToken::from_hlc(&k1_hlc);
    assert!(
        !a.session_check("k1", &k1_token),
        "read-your-writes false success: A claimed C's prefix without \
         holding k1"
    );

    // Once A pulls (claimed) from C itself, the token is satisfied and
    // the value is present.
    pull_delta(&c, &mut a, &zero_frontier());
    assert!(a.session_check("k1", &k1_token));
    assert!(a.get_eventual("k1").is_some());
}

// ===================================================================
// Full-sync (push) path: false negatives allowed, lies are not
// ===================================================================

#[test]
fn push_only_replication_is_false_negative_not_lie() {
    let mut a = EventualApi::new(node_id("node-a"));
    let mut b = EventualApi::new(node_id("node-b"));

    let write_hlc = a.eventual_counter_inc("k").unwrap();
    let token = SessionToken::from_hlc(&write_hlc);

    // Push-style replication: value only, no origin HLC (what
    // /api/internal/sync does).
    let value = a.get_eventual("k").unwrap().clone();
    b.merge_remote("k".into(), &value).unwrap();

    // B actually has the data, but cannot PROVE the origin prefix: the
    // check must fail closed (false negative, not a lie).
    assert!(
        !b.session_check("k", &token),
        "push path must not claim the remote origin"
    );

    // A full dump with applied_origins adoption (what the full-sync
    // fallback does after merging a complete KeyDumpResponse) makes the
    // token satisfiable.
    b.store_mut()
        .merge_applied_origins(a.store().applied_origins());
    assert!(b.session_check("k", &token));
}

// ===================================================================
// Compaction interplay
// ===================================================================

#[test]
fn token_survives_timestamp_pruning() {
    let mut a = EventualApi::new(node_id("node-a"));

    let write_hlc = a.eventual_counter_inc("k").unwrap();
    let token = SessionToken::from_hlc(&write_hlc);
    assert!(a.session_check("k", &token));

    // Compaction prunes the per-key timestamp at/below the checkpoint.
    let checkpoint = HlcTimestamp {
        physical: write_hlc.physical + 1_000,
        logical: 0,
        node_id: "node-a".into(),
    };
    let pruned = a.store_mut().prune_timestamps_before("", &checkpoint);
    assert_eq!(pruned, 1);
    assert!(a.store().timestamp_for("k").is_none());

    // applied_origins is prune-independent: the old token still works.
    assert!(
        a.session_check("k", &token),
        "session tokens must survive compaction pruning"
    );
    // And the pruned floor is exposed for the delta adoption guard.
    assert_eq!(a.store().pruned_floor(), Some(&checkpoint));
}

// ===================================================================
// Randomised interleavings (lightweight property test)
// ===================================================================

/// Increment-only counter on A; B applies delta pulls at random points —
/// both CLAIMED (complete, with adoption) and UNCLAIMED (possibly
/// incomplete, no adoption: the recurring production state after pushes).
/// Invariants across every interleaving:
/// - RYW: whenever B satisfies a write token, B's counter value is at
///   least the value at the time of that write (no lie).
/// - MR: a token-carrying read sequence on B never observes a decreasing
///   counter value once its token has been satisfied.
#[test]
fn property_random_interleavings_never_lie() {
    // Deterministic cheap PRNG (LCG) — no external dependency, stable runs.
    let mut seed: u64 = 0x5eed_cafe_f00d_1234;
    let mut rand = move || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) as usize
    };

    for _round in 0..50 {
        let mut a = EventualApi::new(node_id("node-a"));
        let mut b = EventualApi::new(node_id("node-b"));
        // (token, counter value on A at write time)
        let mut write_tokens: Vec<(SessionToken, i64)> = Vec::new();
        let mut a_value: i64 = 0;
        let mut last_observed: i64 = 0;
        let mut session = SessionToken::default();

        for _step in 0..40 {
            match rand() % 4 {
                // A writes.
                0 => {
                    let hlc = a.eventual_counter_inc("k").unwrap();
                    a_value += 1;
                    write_tokens.push((SessionToken::from_hlc(&hlc), a_value));
                }
                // B pulls a CLAIMED (complete) delta from A + adoption.
                1 => {
                    pull_delta(&a, &mut b, &zero_frontier());
                }
                // B pulls an UNCLAIMED delta from A (no adoption): data
                // converges but no origin claims are made.
                2 => {
                    pull_delta_unclaimed(&a, &mut b, &zero_frontier());
                }
                // B reads with the running session token (MR check).
                _ => {
                    if b.session_check("k", &session) {
                        let value = match b.get_eventual("k") {
                            Some(CrdtValue::Counter(c)) => c.value(),
                            None => 0,
                            other => panic!("expected Counter, got {other:?}"),
                        };
                        assert!(
                            value >= last_observed,
                            "monotonic reads violated: {value} < {last_observed}"
                        );
                        last_observed = value;
                        // Response token: request ∪ key position ∪ visible
                        // frontier (what get_eventual issues).
                        session = {
                            let mut t = session.clone();
                            if let Some(ts) = b.store().timestamp_for("k") {
                                t.merge_hlc(ts);
                            }
                            t.merge_frontiers(b.store().visible_origins());
                            t
                        };
                    }
                }
            }

            // RYW invariant: any satisfied write token implies the value
            // at B is at least the value at write time.
            for (token, value_at_write) in &write_tokens {
                if b.session_check("k", token) {
                    let value = match b.get_eventual("k") {
                        Some(CrdtValue::Counter(c)) => c.value(),
                        None => 0,
                        other => panic!("expected Counter, got {other:?}"),
                    };
                    assert!(
                        value >= *value_at_write,
                        "read-your-writes violated: satisfied token for \
                         value {value_at_write} but B shows {value}"
                    );
                }
            }
        }
    }
}
