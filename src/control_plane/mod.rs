pub mod consensus;
/// Raft consensus needs tokio, the HTTP transport, and filesystem
/// persistence — all native-runtime only. `consensus` itself compiles on
/// every target (wasm builds get the detached facade).
#[cfg(feature = "native-runtime")]
pub mod raft;
pub mod system_namespace;
