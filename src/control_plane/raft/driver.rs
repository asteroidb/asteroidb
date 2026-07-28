//! Background driver for the control-plane Raft node.
//!
//! Runs as an independent `tokio::spawn` task (matching the
//! `spawn_persistence_tasks` convention — the NodeRunner `select!` is kept
//! clean), wired to the runner's shutdown `watch` channel.
//!
//! Voters run two timers:
//! - a randomized election timer (`sleep_until` a deadline; reset whenever
//!   the node hears from a live leader or grants a vote), and
//! - a fixed heartbeat/replication interval (leader only).
//!
//! Non-voters (observers) run the namespace pull loop instead (M-17):
//! Raft never replicates to non-voters, so without a pull their namespace
//! projection freezes at its join-time snapshot — an observer authority
//! would then keep signing the old policy version after a bump and be
//! silently fenced out of the certification quorum.

use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};

use crate::types::NodeId;

use super::node::RaftNode;

/// Observer pull backoff cap under consecutive failures.
const OBSERVER_PULL_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// A pull older than `interval * OBSERVER_PULL_STALE_INTERVALS` triggers a
/// staleness warning (the observer's signatures may carry an old policy
/// version and stop contributing after the next bump).
const OBSERVER_PULL_STALE_INTERVALS: u32 = 6;

fn random_timeout(min: Duration, max: Duration) -> Duration {
    if max <= min {
        return min;
    }
    let span_ms = (max - min).as_millis() as u64;
    min + Duration::from_millis(rand::thread_rng().gen_range(0..=span_ms))
}

/// `duration` with ±20% jitter (observer pulls must not synchronize
/// across a fleet of observers hitting the same voters).
fn jittered(duration: Duration) -> Duration {
    let ms = duration.as_millis() as u64;
    if ms == 0 {
        return duration;
    }
    let span = ms / 5; // 20%
    let low = ms - span;
    let high = ms + span;
    Duration::from_millis(rand::thread_rng().gen_range(low..=high))
}

/// Spawn the background driver for `node`.
///
/// Voters get the election-timer + heartbeat loop. Non-voter nodes (self
/// not in `ASTEROIDB_CONTROL_PLANE_NODES`) run as observers: no election
/// timer, but a periodic committed-namespace pull from the voters (M-17)
/// so policy bumps keep propagating to this node. The pull runs for every
/// observer regardless of whether it is currently an authority — a later
/// runtime authority reassignment (`recalculate_authorities`) must find
/// the namespace already fresh.
pub fn spawn_raft_driver(
    node: Arc<RaftNode>,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if !node.is_voter() {
            observer_loop(node, shutdown).await;
            return;
        }
        voter_loop(node, shutdown).await;
    })
}

async fn voter_loop(node: Arc<RaftNode>, mut shutdown: watch::Receiver<bool>) {
    let config = node.config().clone();
    if config.heartbeat_interval * 3 > config.election_timeout_min {
        tracing::warn!(
            heartbeat_ms = config.heartbeat_interval.as_millis() as u64,
            election_timeout_min_ms = config.election_timeout_min.as_millis() as u64,
            "heartbeat_interval * 3 exceeds election_timeout_min; spurious elections \
             are likely — widen the election timeout for this link's RTT"
        );
    }

    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut election_deadline =
        Instant::now() + random_timeout(config.election_timeout_min, config.election_timeout_max);

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(election_deadline) => {
                node.on_election_timeout();
                election_deadline = Instant::now()
                    + random_timeout(config.election_timeout_min, config.election_timeout_max);
            }
            _ = node.election_reset_notified() => {
                election_deadline = Instant::now()
                    + random_timeout(config.election_timeout_min, config.election_timeout_max);
            }
            _ = heartbeat.tick() => {
                node.on_heartbeat_tick();
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}

/// Observer mode: never elect, never vote — but keep the namespace
/// following the voters' committed control-plane state (M-17).
async fn observer_loop(node: Arc<RaftNode>, mut shutdown: watch::Receiver<bool>) {
    let interval = node.config().observer_pull_interval;
    let targets: Vec<NodeId> = node
        .voters()
        .iter()
        .filter(|v| *v != node.self_id())
        .cloned()
        .collect();

    if interval.is_zero() || targets.is_empty() {
        // interval == 0: explicit opt-out (test/repro). Empty targets can
        // only happen when the voter set is just this node — then the
        // is_voter() branch already ran, so this is purely defensive.
        tracing::warn!(
            voters = ?node.voters().iter().map(|v| v.0.as_str()).collect::<Vec<_>>(),
            "this node is NOT in the control-plane voter set: running as an inert \
             observer with NAMESPACE PULL DISABLED — its namespace will freeze at \
             the current state and any authority role on this node will silently \
             stop contributing after the next policy bump. Check \
             ASTEROIDB_CONTROL_PLANE_NODES / ASTEROIDB_OBSERVER_NS_PULL_MS."
        );
        loop {
            if shutdown.changed().await.is_err() || *shutdown.borrow() {
                return;
            }
        }
    }

    tracing::warn!(
        voters = ?node.voters().iter().map(|v| v.0.as_str()).collect::<Vec<_>>(),
        pull_interval_ms = interval.as_millis() as u64,
        "this node is NOT in the control-plane voter set: running as an observer \
         (it never starts elections and policy/authority mutations against it are \
         rejected); namespace sync active — committed control-plane state is \
         pulled from the voters every interval so authority signatures keep \
         following policy bumps"
    );

    let started_ms = crate::hlc::wall_clock_ms();
    let mut round: usize = 0;
    let mut consecutive_failures: u32 = 0;

    loop {
        // Base delay: the configured interval, doubled per consecutive
        // failure up to the cap, then jittered ±20%.
        let backoff = interval
            .saturating_mul(1u32 << consecutive_failures.min(16))
            .min(OBSERVER_PULL_BACKOFF_MAX.max(interval));
        let delay = jittered(if consecutive_failures == 0 {
            interval
        } else {
            backoff
        });

        tokio::select! {
            _ = tokio::time::sleep(delay) => {
                let target = &targets[round % targets.len()];
                round += 1;
                match node.pull_namespace_once(target).await {
                    Ok(_adopted) => {
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        // warn, not debug: the error class is the ONLY
                        // info-level signal distinguishing a partition from
                        // a local persistence failure (full/read-only disk)
                        // or a responder-identity rejection (address
                        // misconfiguration) — all of which freeze the
                        // namespace. Frequency is bounded by the pull
                        // interval and the exponential backoff.
                        tracing::warn!(
                            target = %target.0,
                            consecutive_failures,
                            error = %e,
                            "observer namespace pull failed; backing off"
                        );
                    }
                }
                // Pull-age staleness warning: while stale, a policy bump
                // silently removes this node's certification contribution
                // until the pull recovers (see ops-guide §14.8).
                let now_ms = crate::hlc::wall_clock_ms();
                let last = node.observer_last_pull_unix_ms();
                let reference = if last == 0 { started_ms } else { last };
                let stale_after =
                    interval.as_millis() as u64 * u64::from(OBSERVER_PULL_STALE_INTERVALS);
                if now_ms.saturating_sub(reference) > stale_after {
                    tracing::warn!(
                        last_success_unix_ms = last,
                        pull_interval_ms = interval.as_millis() as u64,
                        "observer namespace pull is STALE (no success for more than \
                         6 pull intervals): this node's namespace no longer follows \
                         policy bumps, and any authority role here will stop \
                         contributing to certification after the next bump — check \
                         the preceding pull-failure warnings for the cause: voter \
                         reachability (ASTEROIDB_RAFT_PEERS), the voters' \
                         /api/internal/raft/namespace endpoint (rolling upgrade?), \
                         a responder outside the voter set (address mapping), or \
                         LOCAL storage errors while persisting the adopted state"
                    );
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}
