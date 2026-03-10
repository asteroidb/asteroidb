pub mod backend;
pub mod kv;
pub mod migration;

#[cfg(not(target_arch = "wasm32"))]
pub use backend::FileBackend;
#[cfg(feature = "native-storage")]
pub use backend::RedbBackend;
pub use backend::{InMemoryKvBackend, KvBackend, MemoryBackend, StorageBackend};
pub use kv::{CrdtValue, Store};
pub use migration::{Migration, MigrationRegistry};
