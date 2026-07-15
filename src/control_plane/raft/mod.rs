//! Control-plane Raft consensus (replaces the MVP majority-count check).
//!
//! Layout:
//! - [`types`] — log entries, RPC wire types, replicated state.
//! - [`core`] — pure, deterministic Raft state transitions (no IO).
//! - [`state_machine`] — applies committed entries to the replicated core
//!   and its `SystemNamespace` projection.
//! - [`storage`] — fail-stop persistence of `currentTerm`/`votedFor` and
//!   the snapshot+log file.
//! - [`node`] — the shared handle tying core + storage + transport +
//!   namespace together.
//! - [`transport`] — RPC transport trait, plus the in-process
//!   `ChannelTransport` used by multi-node tests.
//! - [`driver`] — background election-timer / heartbeat task.
//!
//! Scope decisions (see docs/architecture.md): static membership only
//! (no joint consensus), single-message snapshot transfer, no linearizable
//! reads (control-plane GETs stay local), no learner replication to
//! non-voters.

pub mod core;
pub mod driver;
pub mod node;
pub mod state_machine;
pub mod storage;
pub mod transport;
pub mod types;

pub use driver::spawn_raft_driver;
pub use node::{RaftConfig, RaftNode, RaftStatus};
