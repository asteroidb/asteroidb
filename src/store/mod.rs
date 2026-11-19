pub mod backend;
pub mod digest;
pub mod kv;
#[cfg(not(target_arch = "wasm32"))]
pub mod wal;

#[cfg(not(target_arch = "wasm32"))]
pub use backend::FileBackend;
#[cfg(feature = "native-storage")]
pub use backend::RedbBackend;
pub use backend::{InMemoryKvBackend, KvBackend, MemoryBackend, StorageBackend};
pub use kv::{CrdtValue, Store};
#[cfg(all(not(target_arch = "wasm32"), feature = "native-runtime"))]
pub use wal::WalSyncer;
#[cfg(not(target_arch = "wasm32"))]
pub use wal::{SyncPolicy, WalConfig, WalPos, WalRecord, WalWriter};
