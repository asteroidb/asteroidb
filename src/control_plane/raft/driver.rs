//! Background driver for the control-plane Raft node.
//!
//! Runs as an independent `tokio::spawn` task (matching the
//! `spawn_persistence_tasks` convention — the NodeRunner `select!` is kept
//! clean), wired to the runner's shutdown `watch` channel.
//!
//! Two timers:
//! - a randomized election timer (`sleep_until` a deadline; reset whenever
//!   the node hears from a live leader or grants a vote), and
//! - a fixed heartbeat/replication interval (leader only).

use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};

use super::node::RaftNode;

fn random_timeout(min: Duration, max: Duration) -> Duration {
    if max <= min {
        return min;
    }
    let span_ms = (max - min).as_millis() as u64;
    min + Duration::from_millis(rand::thread_rng().gen_range(0..=span_ms))
}

/// Spawn the election-timer + heartbeat driver for `node`.
///
/// Non-voter nodes (self not in `ASTEROIDB_CONTROL_PLANE_NODES`) run as
/// inert observers: no election timer at all, only a loud startup warning —
/// they can read the control plane but never propose or vote.
pub fn spawn_raft_driver(
    node: Arc<RaftNode>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if !node.is_voter() {
            tracing::warn!(
                voters = ?node.voters().iter().map(|v| v.0.as_str()).collect::<Vec<_>>(),
                "this node is NOT in the control-plane voter set: running as an inert \
                 observer (it will never start elections, and policy/authority \
                 mutations against it will be rejected). Check ASTEROIDB_CONTROL_PLANE_NODES."
            );
            loop {
                if shutdown.changed().await.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }

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

        let mut election_deadline = Instant::now()
            + random_timeout(config.election_timeout_min, config.election_timeout_max);

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
    })
}
